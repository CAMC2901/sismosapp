"""
DAG 3: `usgs_ai_daily` — Automatización diaria de IA (cada 24 horas):
cálculo de cifras SQL, detección de anomalías (z-score + DBSCAN), reporte ejecutivo
con validación de cifras, indexación de embeddings `bge-m3` en `curated.kb_chunks` y auditoría en `ai_runs`.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from ai.anomalies import detect_and_store_anomalies
from ai.daily_report import compute_daily_metrics_sql, generate_and_store_daily_report
from ai.embeddings import index_knowledge_base_chunks
from alerts.callbacks import on_failure_callback, on_success_callback, record_pipeline_run

KB_ASSET = Dataset("postgres://postgres:5432/sismowatch.curated.kb_chunks")

DEFAULT_ARGS = {
    "owner": "sismowatch-ai-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="usgs_ai_daily",
    description="Cálculo SQL de cifras diarias, detección de anomalías, reporte narrativo validado e indexación vectorial bge-m3.",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
    tags=["sismowatch", "ai", "anomalies", "rag", "daily-report"],
    doc_md="""
    ### DAG `usgs_ai_daily`
    - **Cadencia**: Cada 24 horas (`0 6 * * *`) o disparado bajo demanda desde el panel Admin.
    - **Tolerancia a fallos**: Cascada Groq → Ollama (`qwen2.5:3b`) → Plantilla determinista Python (`fallback=True`).
      El DAG jamás falla por indisponibilidad del proveedor de IA.
    - **Indexación RAG**: Chunking natural e idempotente con `bge-m3` en `curated.kb_chunks`.
    """,
)
def usgs_ai_daily_dag():
    with TaskGroup(group_id="daily_ai_pipeline_group", tooltip="Anomalías, Reporte Diario y Base Vectorial"):

        @task(task_id="compute_daily_sql_metrics")
        def compute_daily_sql_metrics(**context: Any) -> Dict[str, Any]:
            dag_run = context.get("dag_run")
            run_id = dag_run.run_id if dag_run else "manual_ai_daily"
            record_pipeline_run(
                dag_id="usgs_ai_daily",
                run_id=run_id,
                estado="RUNNING",
                resultado="Calculando cifras SQL y ejecutando tareas de IA...",
            )
            metrics = compute_daily_metrics_sql()
            return {
                "run_id": run_id,
                "fecha_reporte": metrics["fecha_reporte"],
                "total_eventos": metrics["total_eventos"],
                "mag_maxima": metrics["mag_maxima"],
            }

        @task(task_id="detect_anomalies_zscore_dbscan")
        def detect_anomalies_zscore_dbscan(sql_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = sql_meta["run_id"]
            anomalies = detect_and_store_anomalies(hours_window=72, run_id=run_id)
            return {
                "run_id": run_id,
                "anomalies_count": len(anomalies),
            }

        @task(task_id="generate_validated_daily_report")
        def generate_validated_daily_report(anom_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = anom_meta["run_id"]
            return generate_and_store_daily_report(run_id=run_id)

        @task(task_id="index_kb_embeddings", outlets=[KB_ASSET])
        def index_kb_embeddings(report_meta: Dict[str, Any]) -> Dict[str, Any]:
            run_id = str(report_meta.get("fecha", "daily_ai"))
            kb_stats = index_knowledge_base_chunks(max_events=80, run_id=run_id)
            record_pipeline_run(
                dag_id="usgs_ai_daily",
                run_id=run_id,
                estado="SUCCESS",
                resultado=(
                    f"Reporte {report_meta['fecha']} ({report_meta['proveedor']}, fallback={report_meta['fallback']}); "
                    f"{kb_stats['total_chunks']} chunks indexados."
                ),
            )
            return kb_stats

        m = compute_daily_sql_metrics()
        a = detect_anomalies_zscore_dbscan(m)
        r = generate_validated_daily_report(a)
        index_kb_embeddings(r)


dag_instance = usgs_ai_daily_dag()
