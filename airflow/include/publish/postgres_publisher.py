"""
Publicación transaccional desde MinIO Oro (Parquet) hacia PostgreSQL curated (Sección 8.2).

Por qué no usamos Spark JDBC para escribir en curated:
Spark JDBC no soporta `INSERT ... ON CONFLICT (id) DO UPDATE` nativo sin tablas temporales
intermedias o truncados riesgosos. En cambio, un paso dedicado en Python lee los datasets
Parquet de la capa Oro y ejecuta un `execute_values` dentro de una única transacción ACID
con `transform_role`: o se publica todo el lote o se hace rollback completo, con reintentos
con backoff exponencial únicamente ante fallos de conexión (`OperationalError`).
"""
import io
import time
from datetime import datetime, timezone
from typing import Any, Dict, List
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pyarrow.parquet as pq
from common.config import get_config, get_db_connection
from common.exceptions import PublishError
from common.logging_utils import get_structured_logger, log_event

logger = get_structured_logger("sismowatch.publish")


def _read_parquet_prefix_from_minio(bucket: str, prefix: str) -> pd.DataFrame:
    """Lee todos los archivos `.parquet` bajo un prefijo en MinIO y los concatena en un DataFrame."""
    cfg = get_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_user,
        aws_secret_access_key=cfg.minio_password,
        region_name="us-east-1",
    )
    paginator = s3.get_paginator("list_objects_v2")
    frames: List[pd.DataFrame] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                table = pq.read_table(io.BytesIO(body))
                df_part = table.to_pandas()
                # Si event_date viene en la ruta de partición (`event_date=YYYY-MM-DD`) y no en las columnas:
                if "event_date" not in df_part.columns and "event_date=" in key:
                    part_val = key.split("event_date=")[1].split("/")[0]
                    df_part["event_date"] = pd.to_datetime(part_val).date()
                frames.append(df_part)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def upsert_gold_dataframes(
    events_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    region_df: pd.DataFrame,
    data_version: str,
    run_id: str = "manual",
    dag_id: str = "usgs_transform_medallion",
) -> Dict[str, Any]:
    """
    Ejecuta el UPSERT transaccional (todo o nada) de `staging.events_silver`,
    `curated.events`, `curated.agg_hourly`, `curated.agg_region` y `curated.pipeline_runs`
    usando `transform_role`. Reintenta solo ante errores transitorios de conexión.
    """
    max_attempts = 3
    base_delay = 2.0

    for attempt in range(1, max_attempts + 1):
        conn = None
        try:
            conn = get_db_connection(role="transform")
            with conn:
                with conn.cursor() as cur:
                    events_upserted = 0
                    hourly_upserted = 0
                    region_upserted = 0

                    if not events_df.empty:
                        # Deduplicar por id en memoria por seguridad antes del batch insert
                        dedup_ev = events_df.sort_values("updated", ascending=False).drop_duplicates(subset=["id"])
                        silver_tuples = []
                        curated_tuples = []
                        for _, r in dedup_ev.iterrows():
                            t_val = pd.to_datetime(r["time"], utc=True).to_pydatetime()
                            u_val = pd.to_datetime(r["updated"], utc=True).to_pydatetime()
                            ev_date = t_val.date()
                            silver_tuples.append(
                                (
                                    str(r["id"]),
                                    float(r["mag"]),
                                    str(r.get("mag_type") or "unknown"),
                                    str(r["place"]),
                                    t_val,
                                    u_val,
                                    float(r["lat"]),
                                    float(r["lon"]),
                                    float(r["depth_km"]),
                                    int(r.get("tsunami") or 0),
                                    int(r.get("sig") or 0),
                                    str(r.get("status") or "automatic"),
                                    str(r.get("type") or "earthquake"),
                                    str(r.get("url") or ""),
                                    str(r["region"]),
                                    ev_date,
                                )
                            )
                            curated_tuples.append(
                                (
                                    str(r["id"]),
                                    float(r["mag"]),
                                    str(r.get("mag_type") or "unknown"),
                                    str(r["place"]),
                                    t_val,
                                    u_val,
                                    float(r["lat"]),
                                    float(r["lon"]),
                                    float(r["depth_km"]),
                                    int(r.get("tsunami") or 0),
                                    int(r.get("sig") or 0),
                                    str(r.get("status") or "automatic"),
                                    str(r.get("type") or "earthquake"),
                                    str(r.get("url") or ""),
                                    str(r["region"]),
                                )
                            )

                        execute_values(
                            cur,
                            """
                            INSERT INTO staging.events_silver (
                                id, mag, mag_type, place, time, updated,
                                lat, lon, depth_km, tsunami, sig, status, type, url, region, event_date
                            ) VALUES %s
                            ON CONFLICT (id) DO UPDATE SET
                                mag = EXCLUDED.mag,
                                mag_type = EXCLUDED.mag_type,
                                place = EXCLUDED.place,
                                time = EXCLUDED.time,
                                updated = EXCLUDED.updated,
                                lat = EXCLUDED.lat,
                                lon = EXCLUDED.lon,
                                depth_km = EXCLUDED.depth_km,
                                tsunami = EXCLUDED.tsunami,
                                sig = EXCLUDED.sig,
                                status = EXCLUDED.status,
                                type = EXCLUDED.type,
                                url = EXCLUDED.url,
                                region = EXCLUDED.region,
                                event_date = EXCLUDED.event_date,
                                ingested_at = NOW()
                            WHERE EXCLUDED.updated >= staging.events_silver.updated;
                            """,
                            silver_tuples,
                            page_size=500,
                        )

                        execute_values(
                            cur,
                            """
                            INSERT INTO curated.events (
                                id, mag, mag_type, place, time, updated,
                                lat, lon, depth_km, tsunami, sig, status, type, url, region
                            ) VALUES %s
                            ON CONFLICT (id) DO UPDATE SET
                                mag = EXCLUDED.mag,
                                mag_type = EXCLUDED.mag_type,
                                place = EXCLUDED.place,
                                time = EXCLUDED.time,
                                updated = EXCLUDED.updated,
                                lat = EXCLUDED.lat,
                                lon = EXCLUDED.lon,
                                depth_km = EXCLUDED.depth_km,
                                tsunami = EXCLUDED.tsunami,
                                sig = EXCLUDED.sig,
                                status = EXCLUDED.status,
                                type = EXCLUDED.type,
                                url = EXCLUDED.url,
                                region = EXCLUDED.region,
                                ingested_at = NOW()
                            WHERE EXCLUDED.updated >= curated.events.updated;
                            """,
                            curated_tuples,
                            page_size=500,
                        )
                        events_upserted = len(curated_tuples)

                    if not hourly_df.empty:
                        dedup_hr = hourly_df.drop_duplicates(subset=["hora"])
                        hourly_tuples = [
                            (
                                pd.to_datetime(r["hora"], utc=True).to_pydatetime(),
                                int(r["conteo"]),
                                float(r["mag_max"]),
                                float(r["mag_promedio"]),
                                int(r["tsunami"]),
                            )
                            for _, r in dedup_hr.iterrows()
                        ]
                        execute_values(
                            cur,
                            """
                            INSERT INTO curated.agg_hourly (hora, conteo, mag_max, mag_promedio, tsunami, updated_at)
                            VALUES %s
                            ON CONFLICT (hora) DO UPDATE SET
                                conteo = EXCLUDED.conteo,
                                mag_max = EXCLUDED.mag_max,
                                mag_promedio = EXCLUDED.mag_promedio,
                                tsunami = EXCLUDED.tsunami,
                                updated_at = NOW();
                            """,
                            hourly_tuples,
                            template="(%s, %s, %s, %s, %s, NOW())",
                            page_size=500,
                        )
                        hourly_upserted = len(hourly_tuples)

                    if not region_df.empty:
                        dedup_rg = region_df.drop_duplicates(subset=["region", "fecha"])
                        region_tuples = [
                            (
                                str(r["region"]),
                                pd.to_datetime(r["fecha"]).date(),
                                int(r["conteo"]),
                                float(r["mag_max"]),
                                float(r["mag_promedio"]),
                            )
                            for _, r in dedup_rg.iterrows()
                        ]
                        execute_values(
                            cur,
                            """
                            INSERT INTO curated.agg_region (region, fecha, conteo, mag_max, mag_promedio, updated_at)
                            VALUES %s
                            ON CONFLICT (region, fecha) DO UPDATE SET
                                conteo = EXCLUDED.conteo,
                                mag_max = EXCLUDED.mag_max,
                                mag_promedio = EXCLUDED.mag_promedio,
                                updated_at = NOW();
                            """,
                            region_tuples,
                            template="(%s, %s, %s, %s, %s, NOW())",
                            page_size=500,
                        )
                        region_upserted = len(region_tuples)

                    # Actualizar data_version en pipeline_runs para invalidar caché IA y notificar SSE
                    cur.execute(
                        """
                        INSERT INTO curated.pipeline_runs (dag_id, run_id, estado, inicio, fin, resultado, data_version, updated_at)
                        VALUES (%s, %s, 'SUCCESS', NOW(), NOW(), %s, %s, NOW())
                        ON CONFLICT (dag_id, run_id) DO UPDATE SET
                            estado = 'SUCCESS',
                            fin = NOW(),
                            resultado = EXCLUDED.resultado,
                            data_version = EXCLUDED.data_version,
                            updated_at = NOW();
                        """,
                        (
                            dag_id,
                            run_id,
                            f"Publicados {events_upserted} eventos, {hourly_upserted} horas, {region_upserted} regiones.",
                            data_version,
                        ),
                    )

            result = {
                "events_upserted": events_upserted,
                "hourly_upserted": hourly_upserted,
                "region_upserted": region_upserted,
                "data_version": data_version,
                "run_id": run_id,
            }
            log_event(logger, "info", "gold_published_to_postgres", run_id, result)
            return result

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            if attempt == max_attempts:
                raise PublishError(
                    f"Error de conexión a PostgreSQL tras {max_attempts} intentos: {exc}",
                    is_transient=True,
                ) from exc
            sleep_sec = base_delay * (2 ** (attempt - 1))
            log_event(
                logger,
                "warning",
                "postgres_publish_retry",
                run_id,
                {"attempt": attempt, "sleep_sec": sleep_sec, "error": str(exc)},
            )
            time.sleep(sleep_sec)
        except Exception as exc:
            raise PublishError(f"Error no recuperable al publicar en curated: {exc}", is_transient=False) from exc
        finally:
            if conn is not None:
                conn.close()

    raise PublishError("Agotados los reintentos al publicar en PostgreSQL curated.", is_transient=True)


def publish_gold_from_minio_to_postgres(
    data_version: str,
    run_id: str = "manual",
    dag_id: str = "usgs_transform_medallion",
) -> Dict[str, Any]:
    """
    Lee los archivos Parquet de `s3://gold/` (`events`, `agg_hourly`, `agg_region`)
    y ejecuta el upsert transaccional en PostgreSQL `curated`.
    """
    cfg = get_config()
    bucket = cfg.bucket_gold
    events_df = _read_parquet_prefix_from_minio(bucket, "events/")
    hourly_df = _read_parquet_prefix_from_minio(bucket, "agg_hourly/")
    region_df = _read_parquet_prefix_from_minio(bucket, "agg_region/")

    return upsert_gold_dataframes(
        events_df=events_df,
        hourly_df=hourly_df,
        region_df=region_df,
        data_version=data_version,
        run_id=run_id,
        dag_id=dag_id,
    )
