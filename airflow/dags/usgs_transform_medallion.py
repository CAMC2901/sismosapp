"""
DAG 2: `usgs_transform_medallion` — Transformación Spark (Bronce → Plata → Oro),
compuerta bloqueante de calidad con Pandera y publicación transaccional en PostgreSQL curated.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from alerts.callbacks import on_failure_callback, on_success_callback, record_pipeline_run
from governance.openmetadata_sync import sync_openmetadata_safe
from publish.postgres_publisher import publish_gold_from_minio_to_postgres
from quality.gate import evaluate_batch_quality_gate
from spark_jobs.bronze_to_silver import get_latest_bronze_uris, run_bronze_to_silver
from spark_jobs.silver_to_gold import run_silver_to_gold

BRONZE_ASSET = Dataset("s3://bronze/usgs")
GOLD_ASSET = Dataset("s3://gold/events")

DEFAULT_ARGS = {
    "owner": "sismowatch-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=8),
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="usgs_transform_medallion",
    description="Disparado por el Asset de Bronce: ejecuta Spark Bronce→Plata, compuerta Pandera, Spark Plata→Oro y Upsert ACID en curated.",
    schedule=[BRONZE_ASSET],
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
    tags=["sismowatch", "medallion", "spark", "quality", "gold"],
    doc_md="""
    ### DAG `usgs_transform_medallion`
    - **Disparador**: Asset/Dataset `s3://bronze/usgs`.
    - **Transformación**: Apache Spark (`local[2]`) con conector S3A hacia MinIO y `partitionOverwriteMode=dynamic`.
    - **Calidad**: Reglas por fila (descarte con rastro liviano) y compuerta bloqueante por lote con Pandera.
    - **Publicación**: Upsert transaccional (`INSERT ... ON CONFLICT DO UPDATE`) en `curated` y emisión de `s3://gold/events`.
    """,
)
def usgs_transform_medallion_dag():
    with TaskGroup(group_id="medallion_spark_and_quality_group", tooltip="Spark Bronce→Plata, Calidad y Plata→Oro"):

        @task(task_id="resolve_bronze_batch")
        def resolve_bronze_batch(**context: Any) -> Dict[str, Any]:
            dag_run = context.get("dag_run")
            run_id = dag_run.run_id if dag_run else "manual_transform"
            record_pipeline_run(
                dag_id="usgs_transform_medallion",
                run_id=run_id,
                estado="RUNNING",
                resultado="Iniciando transformación Medallion en Spark...",
            )
            info = get_latest_bronze_uris(limit=1)
            return {
                "s3_uris": info["s3_uris"],
                "latest_ts_iso": info["latest_ts_iso"],
                "run_id": run_id,
            }

        @task(task_id="spark_bronze_to_silver_and_quality_gate")
        def spark_bronze_to_silver_and_quality_gate(batch_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = batch_meta["run_id"]
            s3_uris = batch_meta["s3_uris"]
            latest_ts = datetime.fromisoformat(batch_meta["latest_ts_iso"])

            silver_res = run_bronze_to_silver(s3_uris=s3_uris, run_id=run_id)
            gate_res = evaluate_batch_quality_gate(
                run_id=run_id,
                valid_df=silver_res["batch_pdf"],
                rejections_log=silver_res["rejections"],
                latest_ingestion_ts=latest_ts,
                persist_to_db=True,
            )
            return {
                "run_id": run_id,
                "silver_uri": silver_res["silver_uri"],
                "approved_count": gate_res["approved"],
                "rejected_count": gate_res["rejected"],
                "score": gate_res["score"],
            }

        @task(task_id="spark_silver_to_gold")
        def spark_silver_to_gold(silver_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = silver_meta["run_id"]
            gold_res = run_silver_to_gold(run_id=run_id)
            return {
                "run_id": run_id,
                "events_uri": gold_res["events_uri"],
                "events_count": gold_res["events_count"],
                "hourly_count": gold_res["hourly_count"],
                "region_count": gold_res["region_count"],
                "data_version": gold_res["data_version"],
            }

    with TaskGroup(group_id="publish_curated_group", tooltip="Publicación ACID a PostgreSQL curated"):

        @task(task_id="publish_gold_to_postgres", outlets=[GOLD_ASSET])
        def publish_gold_to_postgres(gold_meta: Dict[str, Any]) -> Dict[str, Any]:
            pub_res = publish_gold_from_minio_to_postgres(
                data_version=gold_meta["data_version"],
                run_id=gold_meta["run_id"],
                dag_id="usgs_transform_medallion",
            )
            sync_openmetadata_safe()
            return pub_res

    b_meta = resolve_bronze_batch()
    s_meta = spark_bronze_to_silver_and_quality_gate(b_meta)
    g_meta = spark_silver_to_gold(s_meta)
    publish_gold_to_postgres(g_meta)


dag_instance = usgs_transform_medallion_dag()

