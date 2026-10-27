"""
Disparo automático del backfill de 30 días en el primer arranque (Sección 4 y 8.4).

Verifica primero si ya existe histórico en `curated.events`. Si la tabla está vacía
(o solo tiene el lote horario inicial), espera a que Airflow cargue los DAGs en su
base de metadatos, los activa y dispara `usgs_backfill` vía API REST de Airflow.
"""
import os
import time
import requests
from common.config import get_config, get_db_connection
from common.logging_utils import get_structured_logger, log_event

logger = get_structured_logger("sismowatch.auto_backfill")

DAGS_TO_UNPAUSE = [
    "usgs_ingest_bronze",
    "usgs_transform_medallion",
    "usgs_ai_daily",
    "usgs_backfill",
]


def _get_existing_events_count() -> int:
    conn = get_db_connection(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM curated.events;")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        conn.close()


def trigger_initial_backfill_if_needed() -> None:
    cfg = get_config()
    api_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1").rstrip("/")
    api_user = os.getenv("AIRFLOW_API_USER", "sismowatch_admin")
    api_pass = os.getenv("AIRFLOW_API_PASSWORD", "sw_airflow_admin_9812")
    auth = (api_user, api_pass)

    existing_count = _get_existing_events_count()
    if existing_count > 100:
        log_event(
            logger,
            "info",
            "auto_backfill_skipped_history_exists",
            "init",
            {"existing_events": existing_count},
        )
        return

    # Esperar hasta que el scheduler haya registrado los DAGs en Airflow REST API
    for attempt in range(1, 35):
        try:
            resp = requests.get(f"{api_url}/dags/usgs_backfill", auth=auth, timeout=5)
            if resp.status_code == 200:
                break
        except requests.RequestException:
            pass
        log_event(logger, "info", "waiting_for_dags_to_load", "init", {"attempt": attempt})
        time.sleep(4)

    # Asegurar que los 4 DAGs estén despausados
    for dag_id in DAGS_TO_UNPAUSE:
        try:
            requests.patch(
                f"{api_url}/dags/{dag_id}",
                auth=auth,
                json={"is_paused": False},
                timeout=5,
            )
        except requests.RequestException as exc:
            log_event(logger, "warning", "unpause_dag_warn", "init", {"dag_id": dag_id, "error": str(exc)})

    # Verificar que no haya ya una corrida de usgs_backfill en curso
    try:
        runs_resp = requests.get(
            f"{api_url}/dags/usgs_backfill/dagRuns",
            auth=auth,
            params={"limit": 5},
            timeout=5,
        )
        if runs_resp.status_code == 200:
            dag_runs = runs_resp.json().get("dag_runs", [])
            active_or_done = [
                r for r in dag_runs if r.get("state") in ("running", "queued", "success")
            ]
            if active_or_done and existing_count > 0:
                log_event(logger, "info", "backfill_already_triggered", "init", {"runs": len(active_or_done)})
                return
    except requests.RequestException:
        pass

    # Disparar backfill de 30 días
    try:
        trigger_resp = requests.post(
            f"{api_url}/dags/usgs_backfill/dagRuns",
            auth=auth,
            json={
                "conf": {
                    "days": cfg.backfill_default_days,
                    "minmagnitude": cfg.backfill_min_mag,
                }
            },
            timeout=10,
        )
        log_event(
            logger,
            "info",
            "auto_backfill_triggered",
            "init",
            {"status_code": trigger_resp.status_code, "days": cfg.backfill_default_days},
        )
    except requests.RequestException as exc:
        log_event(logger, "error", "auto_backfill_trigger_failed", "init", {"error": str(exc)})


if __name__ == "__main__":
    trigger_initial_backfill_if_needed()
