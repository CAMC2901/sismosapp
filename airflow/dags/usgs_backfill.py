"""
DAG 4: `usgs_backfill` — Carga histórica idempotente por tramos desde USGS (`fdsnws/event/1/query`).
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from ai.anomalies import detect_and_store_anomalies
from ai.daily_report import generate_and_store_daily_report
from ai.embeddings import index_knowledge_base_chunks
from alerts.callbacks import on_failure_callback, on_success_callback, record_pipeline_run
from ingestion.usgs_client import ingest_backfill_range
from publish.postgres_publisher import publish_gold_from_minio_to_postgres
from quality.gate import evaluate_batch_quality_gate
from spark_jobs.bronze_to_silver import run_bronze_to_silver
from spark_jobs.silver_to_gold import run_silver_to_gold

BRONZE_ASSET = Dataset("s3://bronze/usgs")
GOLD_ASSET = Dataset("s3://gold/events")

DEFAULT_ARGS = {
    "owner": "sismowatch-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="usgs_backfill",
    description="Carga histórica por tramos de N días desde fdsnws/event/1/query hacia Bronce, Plata, Oro y PostgreSQL curated.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "days": Param(30, type="integer", minimum=1, maximum=90),
        "minmagnitude": Param(2.5, type="number", minimum=-1.0, maximum=9.0),
    },
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
    tags=["sismowatch", "backfill", "historical", "spark"],
    doc_md="""
    ### DAG `usgs_backfill`
    - **Schedule**: `None` (bajo demanda por botón Admin en la plataforma o disparo automático al primer arranque).
    - **Parámetros**: `days` (por defecto 30) y `minmagnitude` (por defecto 2.5).
    - **Idempotencia**: Ejecutarlo dos veces sobre el mismo rango no duplica registros (`partitionOverwriteMode=dynamic` + dedupe en Plata y `ON CONFLICT DO UPDATE` en Oro/Postgres).
    """,
)
def usgs_backfill_dag():
    with TaskGroup(group_id="backfill_medallion_group", tooltip="Ingesta por tramos y Transformación Spark"):

        @task(task_id="ingest_historical_chunks", outlets=[BRONZE_ASSET])
        def ingest_historical_chunks(**context: Any) -> Dict[str, Any]:
            dag_run = context.get("dag_run")
            run_id = dag_run.run_id if dag_run else "manual_backfill"
            conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
            params = context.get("params") or {}
            days = int(conf.get("days", params.get("days", 30)))
            min_mag = float(conf.get("minmagnitude", params.get("minmagnitude", 2.5)))

            record_pipeline_run(
                dag_id="usgs_backfill",
                run_id=run_id,
                estado="RUNNING",
                resultado=f"Descargando backfill de {days} días (mag >= {min_mag})...",
            )
            return ingest_backfill_range(days=days, min_magnitude=min_mag, run_id=run_id)

        @task(task_id="spark_transform_and_publish_backfill", outlets=[GOLD_ASSET])
        def spark_transform_and_publish_backfill(backfill_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = backfill_meta["run_id"]
            s3_uris = backfill_meta["s3_uris"]

            silver_res = run_bronze_to_silver(s3_uris=s3_uris, run_id=run_id)
            gate_res = evaluate_batch_quality_gate(
                run_id=run_id,
                valid_df=silver_res["batch_pdf"],
                rejections_log=silver_res["rejections"],
                latest_ingestion_ts=datetime.now(timezone.utc),
                persist_to_db=True,
            )
            gold_res = run_silver_to_gold(run_id=run_id)
            pub_res = publish_gold_from_minio_to_postgres(
                data_version=gold_res["data_version"],
                run_id=run_id,
                dag_id="usgs_backfill",
            )
            return {
                "run_id": run_id,
                "approved_count": gate_res["approved"],
                "events_upserted": pub_res["events_upserted"],
                "data_version": gold_res["data_version"],
            }

        @task(task_id="bootstrap_ai_insights")
        def bootstrap_ai_insights(pub_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = pub_meta["run_id"]
            anoms = detect_and_store_anomalies(hours_window=72, run_id=run_id)
            rep = generate_and_store_daily_report(run_id=run_id)
            kb = index_knowledge_base_chunks(max_events=80, run_id=run_id)
            record_pipeline_run(
                dag_id="usgs_ai_daily",
                run_id=f"bootstrap_{run_id}",
                estado="SUCCESS",
                resultado=f"Bootstrap IA tras backfill: reporte {rep['fecha']}, {len(anoms)} anomalías, {kb['total_chunks']} chunks.",
            )
            return {
                "report_date": rep["fecha"],
                "anomalies": len(anoms),
                "kb_chunks": kb["total_chunks"],
            }

        ing = ingest_historical_chunks()
        pub = spark_transform_and_publish_backfill(ing)
        bootstrap_ai_insights(pub)


dag_instance = usgs_backfill_dag()

