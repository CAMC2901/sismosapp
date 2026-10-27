"""
DAG 1: `usgs_ingest_bronze` — Ingesta micro-batch cada 5 minutos desde USGS hacia MinIO Bronce.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from alerts.callbacks import on_failure_callback, on_success_callback, record_pipeline_run
from ingestion.usgs_client import ingest_realtime_hour_feed

BRONZE_ASSET = Dataset("s3://bronze/usgs")

DEFAULT_ARGS = {
    "owner": "sismowatch-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=4),
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="usgs_ingest_bronze",
    description="Consulta all_hour.geojson cada 5 min, valida GeoJSON, escribe en MinIO Bronce y registra en raw.ingestions.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
    tags=["sismowatch", "bronze", "ingestion", "usgs"],
    doc_md="""
    ### DAG `usgs_ingest_bronze`
    - **Cadencia**: Cada 5 minutos (`*/5 * * * *`).
    - **Fuente**: Catálogo público USGS (`all_hour.geojson`) vía HTTPS con timeouts explícitos.
    - **Idempotencia**: Escritura en ruta temporal (`_tmp/`) y renombrado determinístico en `s3://bronze/usgs/dt=YYYY-MM-DD/hh=HH/<timestamp>.json`.
    - **Encadenamiento**: Actualiza el Asset/Dataset `s3://bronze/usgs` que dispara `usgs_transform_medallion`.
    """,
)
def usgs_ingest_bronze_dag():
    with TaskGroup(group_id="bronze_ingestion_group", tooltip="Ingesta y auditoría en capa Bronce"):

        @task(task_id="fetch_validate_and_write_bronze", outlets=[BRONZE_ASSET])
        def fetch_validate_and_write_bronze(**context: Any) -> Dict[str, Any]:
            dag_run = context.get("dag_run")
            run_id = dag_run.run_id if dag_run else "manual_ingest"
            record_pipeline_run(
                dag_id="usgs_ingest_bronze",
                run_id=run_id,
                estado="RUNNING",
                resultado="Consultando feed horario USGS...",
            )
            return ingest_realtime_hour_feed(run_id=run_id)

        @task(task_id="audit_pipeline_status")
        def audit_pipeline_status(ingest_meta: Dict[str, Any]) -> Dict[str, Any]:
            record_pipeline_run(
                dag_id="usgs_ingest_bronze",
                run_id=ingest_meta["run_id"],
                estado="SUCCESS",
                resultado=f"Archivo {ingest_meta['s3_uri']} ({ingest_meta['event_count']} eventos).",
            )
            return {
                "ingestion_id": ingest_meta["ingestion_id"],
                "event_count": ingest_meta["event_count"],
                "s3_uri": ingest_meta["s3_uri"],
            }

        meta = fetch_validate_and_write_bronze()
        audit_pipeline_status(meta)


dag_instance = usgs_ingest_bronze_dag()

