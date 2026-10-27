"""
Cliente de ingesta USGS y persistencia atómica en MinIO Bronce + PostgreSQL raw.ingestions.

Decisiones técnicas:
1. Reintentos con backoff exponencial solo en fallos transitorios (5xx, 429, timeouts, red).
   Los errores 4xx (excepto 429) fallan inmediatamente sin reintentar.
2. Escritura en dos pasos sobre MinIO (`_tmp/<key>.tmp` -> copia al destino final -> borrado de `_tmp`)
   y nombre determinístico por ventana de ejecución para evitar archivos corruptos o duplicados en reintentos.
3. Registro en `raw.ingestions` usando `ingest_role` (mínimo privilegio).
"""
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import boto3
import requests
from common.config import get_config, get_db_connection
from common.exceptions import IngestionError
from common.logging_utils import get_structured_logger, log_event

logger = get_structured_logger("sismowatch.ingestion")


def validate_geojson_payload(payload: Any) -> Dict[str, Any]:
    """
    Valida que el documento devuelto por USGS tenga la estructura GeoJSON FeatureCollection.
    Un feed con cero eventos (`features: []`) es válido.
    """
    if not isinstance(payload, dict):
        raise IngestionError("El payload recibido no es un objeto JSON válido.", is_transient=False)
    if payload.get("type") != "FeatureCollection":
        raise IngestionError(
            f"Estructura GeoJSON inválida: se esperaba 'FeatureCollection', llegó '{payload.get('type')}'.",
            is_transient=False,
        )
    features = payload.get("features")
    if not isinstance(features, list):
        raise IngestionError("El campo 'features' no es una lista válida en el GeoJSON.", is_transient=False)
    return payload


def _http_get_geojson(url: str, params: Optional[Dict[str, Any]] = None, run_id: str = "manual") -> Dict[str, Any]:
    """
    Realiza una petición HTTPS con timeouts explícitos (conexión 5s, lectura 20s)
    y hasta 3 intentos con backoff exponencial solo para errores transitorios.
    """
    max_attempts = 3
    base_delay = 2.0

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=(5.0, 20.0),
                headers={"User-Agent": "SismoWatch-SuperApp/1.0"},
            )
            status = resp.status_code
            if status == 200:
                try:
                    data = resp.json()
                except ValueError as exc:
                    raise IngestionError(f"Respuesta no es JSON válido: {exc}", is_transient=False) from exc
                return validate_geojson_payload(data)

            # 429 o 5xx son transitorios
            if status == 429 or status >= 500:
                raise IngestionError(
                    f"HTTP {status} transitorio desde USGS ({url})",
                    is_transient=True,
                )
            # Cualquier otro 4xx NO es transitorio y no se reintenta
            raise IngestionError(
                f"HTTP {status} error cliente no recuperable desde USGS ({url})",
                is_transient=False,
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            err = IngestionError(f"Fallo de red/timeout hacia USGS: {exc}", is_transient=True)
            if attempt == max_attempts:
                log_event(logger, "error", "usgs_http_failed", run_id, {"attempt": attempt, "error": str(err)})
                raise err from exc
            sleep_sec = base_delay * (2 ** (attempt - 1))
            log_event(
                logger,
                "warning",
                "usgs_http_retry",
                run_id,
                {"attempt": attempt, "sleep_sec": sleep_sec, "reason": str(exc)},
            )
            time.sleep(sleep_sec)
        except IngestionError as exc:
            if not exc.is_transient or attempt == max_attempts:
                log_event(logger, "error", "usgs_ingestion_error", run_id, {"attempt": attempt, "error": str(exc)})
                raise
            sleep_sec = base_delay * (2 ** (attempt - 1))
            log_event(
                logger,
                "warning",
                "usgs_transient_retry",
                run_id,
                {"attempt": attempt, "sleep_sec": sleep_sec, "reason": str(exc)},
            )
            time.sleep(sleep_sec)

    raise IngestionError("Agotados los reintentos hacia USGS.", is_transient=True)


def _write_atomic_to_bronze(raw_bytes: bytes, final_key: str, run_id: str) -> str:
    """
    Escribe primero a una ruta temporal en MinIO y luego copia/renombra a la ruta determinística final.
    """
    cfg = get_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_user,
        aws_secret_access_key=cfg.minio_password,
        region_name="us-east-1",
    )
    bucket = cfg.bucket_bronze
    tmp_key = f"usgs/_tmp/{hashlib.sha256(final_key.encode()).hexdigest()}.json.tmp"

    try:
        s3.put_object(
            Bucket=bucket,
            Key=tmp_key,
            Body=raw_bytes,
            ContentType="application/geo+json",
        )
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": tmp_key},
            Key=final_key,
        )
        s3.delete_object(Bucket=bucket, Key=tmp_key)
    except Exception as exc:
        raise IngestionError(f"Error al escribir archivo en MinIO Bronce ({final_key}): {exc}", is_transient=True) from exc

    s3_uri = f"s3a://{bucket}/{final_key}"
    log_event(logger, "info", "bronze_write_complete", run_id, {"s3_uri": s3_uri, "bytes": len(raw_bytes)})
    return s3_uri


def _record_raw_ingestion(
    ingestion_id: str,
    file_path: str,
    event_count: int,
    status: str,
    run_id: str,
) -> None:
    """Registra la auditoría de ingesta en `raw.ingestions` usando `ingest_role`."""
    conn = get_db_connection(role="ingest")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.ingestions (ingestion_id, file_path, event_count, status, timestamp, run_id)
                    VALUES (%s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT (ingestion_id) DO UPDATE
                    SET file_path = EXCLUDED.file_path,
                        event_count = EXCLUDED.event_count,
                        status = EXCLUDED.status,
                        timestamp = NOW(),
                        run_id = EXCLUDED.run_id;
                    """,
                    (ingestion_id, file_path, event_count, status, run_id),
                )
    finally:
        conn.close()


def ingest_realtime_hour_feed(run_id: str, logical_dt: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Descarga el feed horario de USGS, lo valida, lo guarda de forma idempotente en
    `s3://bronze/usgs/dt=YYYY-MM-DD/hh=HH/<timestamp>.json` y lo registra en `raw.ingestions`.
    Devuelve únicamente metadatos livianos aptos para XCom.
    """
    cfg = get_config()
    now_utc = logical_dt.astimezone(timezone.utc) if logical_dt else datetime.now(timezone.utc)
    # Redondeamos al bloque de 5 minutos para que reintentos del mismo run tengan nombre determinístico
    minute_bucket = (now_utc.minute // 5) * 5
    deterministic_dt = now_utc.replace(minute=minute_bucket, second=0, microsecond=0)

    dt_str = deterministic_dt.strftime("%Y-%m-%d")
    hh_str = deterministic_dt.strftime("%H")
    ts_str = deterministic_dt.strftime("%Y%m%dT%H%M%SZ")

    final_key = f"usgs/dt={dt_str}/hh={hh_str}/{ts_str}.json"
    ingestion_id = f"realtime_{ts_str}"

    payload = _http_get_geojson(cfg.usgs_feed_url, run_id=run_id)
    features = payload.get("features", [])
    event_count = len(features)

    raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3_uri = _write_atomic_to_bronze(raw_bytes, final_key, run_id=run_id)
    _record_raw_ingestion(
        ingestion_id=ingestion_id,
        file_path=s3_uri,
        event_count=event_count,
        status="SUCCESS",
        run_id=run_id,
    )

    return {
        "ingestion_id": ingestion_id,
        "s3_uri": s3_uri,
        "event_count": event_count,
        "partition_dt": dt_str,
        "run_id": run_id,
    }


def ingest_backfill_range(
    days: int = 30,
    min_magnitude: float = 2.5,
    run_id: str = "backfill",
) -> Dict[str, Any]:
    """
    Consulta el endpoint histórico `fdsnws/event/1/query` en tramos de 5 días para
    respetar el límite de resultados por petición de USGS, persistiendo cada tramo
    de manera determinística en Bronce y registrándolo en `raw.ingestions`.
    """
    cfg = get_config()
    end_dt = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)
    start_dt = (end_dt - timedelta(days=max(1, int(days)))).replace(hour=0, minute=0, second=0, microsecond=0)

    chunk_days = 5
    cursor_dt = start_dt
    written_uris: List[str] = []
    total_events = 0

    while cursor_dt < end_dt:
        chunk_end = min(cursor_dt + timedelta(days=chunk_days), end_dt)
        start_str = cursor_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        dt_part = cursor_dt.strftime("%Y-%m-%d")
        window_slug = f"{cursor_dt.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}"

        params = {
            "format": "geojson",
            "starttime": start_str,
            "endtime": end_str,
            "minmagnitude": min_magnitude,
            "orderby": "time-asc",
        }
        payload = _http_get_geojson(cfg.usgs_query_url, params=params, run_id=run_id)
        features = payload.get("features", [])
        count = len(features)

        final_key = f"usgs/dt={dt_part}/hh=00/backfill_{window_slug}_m{min_magnitude}.json"
        ingestion_id = f"backfill_{window_slug}_m{min_magnitude}"

        raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        s3_uri = _write_atomic_to_bronze(raw_bytes, final_key, run_id=run_id)
        _record_raw_ingestion(
            ingestion_id=ingestion_id,
            file_path=s3_uri,
            event_count=count,
            status="SUCCESS",
            run_id=run_id,
        )
        written_uris.append(s3_uri)
        total_events += count
        cursor_dt = chunk_end + timedelta(seconds=1)

    return {
        "chunks_count": len(written_uris),
        "total_events": total_events,
        "s3_uris": written_uris,
        "days": days,
        "min_magnitude": min_magnitude,
        "run_id": run_id,
    }
