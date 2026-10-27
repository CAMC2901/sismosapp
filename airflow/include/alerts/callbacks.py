"""
Callbacks de estado de pipeline y despacho de alertas por Webhook (Sección 8).

Cuando una tarea o DAG falla, registra el fallo en `curated.pipeline_runs` y envía
una notificación estructurada a `ALERT_WEBHOOK_URL`. De esta forma, si USGS cae,
la alerta se dispara y la plataforma sigue sirviendo el último dato con su indicador
de frescura calculado a partir de `curated.pipeline_runs`.
"""
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import requests
from common.config import get_config, get_db_connection
from common.logging_utils import get_structured_logger, log_event, redact_secrets

logger = get_structured_logger("sismowatch.alerts")


def record_pipeline_run(
    dag_id: str,
    run_id: str,
    estado: str,
    resultado: str,
    inicio: Optional[datetime] = None,
    fin: Optional[datetime] = None,
    data_version: Optional[str] = None,
) -> None:
    """Actualiza o inserta el estado de una corrida en `curated.pipeline_runs`."""
    conn = None
    try:
        conn = get_db_connection(role="transform")
        start_ts = inicio or datetime.now(timezone.utc)
        end_ts = fin or (datetime.now(timezone.utc) if estado in ("SUCCESS", "FAILED") else None)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.pipeline_runs (
                        dag_id, run_id, estado, inicio, fin, resultado, data_version, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (dag_id, run_id) DO UPDATE SET
                        estado = EXCLUDED.estado,
                        fin = COALESCE(EXCLUDED.fin, curated.pipeline_runs.fin),
                        resultado = EXCLUDED.resultado,
                        data_version = COALESCE(EXCLUDED.data_version, curated.pipeline_runs.data_version),
                        updated_at = NOW();
                    """,
                    (
                        dag_id,
                        run_id,
                        estado,
                        start_ts,
                        end_ts,
                        redact_secrets(resultado)[:500],
                        data_version,
                    ),
                )
    except Exception as exc:
        log_event(
            logger,
            "error",
            "pipeline_run_record_error",
            run_id,
            {"dag_id": dag_id, "estado": estado, "error": str(exc)},
        )
    finally:
        if conn is not None:
            conn.close()


def send_webhook_alert(
    dag_id: str,
    task_id: str,
    run_id: str,
    error_message: str,
) -> bool:
    """Envía una alerta HTTP POST al webhook configurado en `ALERT_WEBHOOK_URL`."""
    cfg = get_config()
    webhook_url = cfg.alert_webhook_url
    if not webhook_url:
        return False

    safe_err = redact_secrets(error_message)[:350]
    payload = {
        "text": f"[SismoWatch ALERTA] Fallo en DAG `{dag_id}` (tarea `{task_id}`, run `{run_id}`): {safe_err}",
        "service": "SismoWatch",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "severity": "CRITICAL",
        "error": safe_err,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = requests.post(webhook_url, json=payload, timeout=(3.0, 5.0))
        log_event(
            logger,
            "info",
            "webhook_alert_sent",
            run_id,
            {"dag_id": dag_id, "task_id": task_id, "status_code": resp.status_code},
        )
        return 200 <= resp.status_code < 300
    except requests.RequestException as exc:
        log_event(
            logger,
            "warning",
            "webhook_alert_delivery_failed",
            run_id,
            {"dag_id": dag_id, "task_id": task_id, "error": str(exc)},
        )
        return False


def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Callback estándar de Airflow invocado cuando falla una tarea o DAG.
    1. Registra estado FAILED en `curated.pipeline_runs`.
    2. Dispara el Webhook de alerta.
    """
    dag_run = context.get("dag_run")
    ti = context.get("task_instance")
    dag_id = dag_run.dag_id if dag_run else context.get("dag").dag_id
    run_id = dag_run.run_id if dag_run else str(context.get("run_id", "unknown"))
    task_id = ti.task_id if ti else "unknown_task"
    exception = context.get("exception")
    err_str = f"Fallo en tarea {task_id}: {exception}" if exception else f"Fallo en tarea {task_id}"

    record_pipeline_run(
        dag_id=dag_id,
        run_id=run_id,
        estado="FAILED",
        resultado=err_str,
    )
    send_webhook_alert(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        error_message=err_str,
    )


def on_success_callback(context: Dict[str, Any]) -> None:
    """Callback estándar de Airflow invocado al completarse exitosamente una tarea final o DAG."""
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id if dag_run else context.get("dag").dag_id
    run_id = dag_run.run_id if dag_run else str(context.get("run_id", "unknown"))
    record_pipeline_run(
        dag_id=dag_id,
        run_id=run_id,
        estado="SUCCESS",
        resultado="Ejecución completada exitosamente.",
    )
