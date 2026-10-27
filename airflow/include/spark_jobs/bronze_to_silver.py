"""
Transformación PySpark: Bronce → Plata (Sección 9).

Decisiones de ingeniería:
1. Todas las transformaciones de la capa Medallion se ejecutan en Apache Spark (`local[2]`).
2. Para garantizar que una fila corrupta jamás tumbe el lote (Sección 10), el parseo y
   validación por fila se ejecuta encapsulado en `try/except` produciendo estructuras tipadas
   con estado `is_valid` y registro liviano de rechazo `{id, regla, motivo}`.
3. La deduplicación por `id` conservando el mayor `updated` se realiza en Spark mediante
   `Window.partitionBy("id").orderBy(F.col("updated").desc())`.
4. Para que las ingestas micro-batch cada 5 minutos no borren horas previas del mismo día al
   sobrescribir la partición `event_date`, se hace `unionByName` con las particiones afectadas
   ya existentes en `s3a://silver/events` antes de deduplicar y escribir con
   `spark.sql.sources.partitionOverwriteMode=dynamic`. Así, ejecutar el job N veces sobre
   el mismo intervalo es 100% idempotente y sin pérdida de datos.
"""
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import boto3
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from common.config import get_config
from common.logging_utils import get_structured_logger, log_event
from quality.rules import transform_geojson_feature_to_silver_row
from spark_jobs.session import create_spark_session

logger = get_structured_logger("sismowatch.spark.bronze_to_silver")

PARSED_ROW_SCHEMA = T.StructType(
    [
        T.StructField("is_valid", T.BooleanType(), nullable=False),
        T.StructField("id", T.StringType(), nullable=True),
        T.StructField("mag", T.DoubleType(), nullable=True),
        T.StructField("mag_type", T.StringType(), nullable=True),
        T.StructField("place", T.StringType(), nullable=True),
        T.StructField("time", T.TimestampType(), nullable=True),
        T.StructField("updated", T.TimestampType(), nullable=True),
        T.StructField("lat", T.DoubleType(), nullable=True),
        T.StructField("lon", T.DoubleType(), nullable=True),
        T.StructField("depth_km", T.DoubleType(), nullable=True),
        T.StructField("tsunami", T.IntegerType(), nullable=True),
        T.StructField("sig", T.IntegerType(), nullable=True),
        T.StructField("status", T.StringType(), nullable=True),
        T.StructField("type", T.StringType(), nullable=True),
        T.StructField("url", T.StringType(), nullable=True),
        T.StructField("region", T.StringType(), nullable=True),
        T.StructField("event_date", T.DateType(), nullable=True),
        T.StructField("reject_id", T.StringType(), nullable=True),
        T.StructField("reject_rule", T.StringType(), nullable=True),
        T.StructField("reject_reason", T.StringType(), nullable=True),
    ]
)

SILVER_COLS = [
    "id",
    "mag",
    "mag_type",
    "place",
    "time",
    "updated",
    "lat",
    "lon",
    "depth_km",
    "tsunami",
    "sig",
    "status",
    "type",
    "url",
    "region",
    "event_date",
]


def _feature_to_spark_tuple(feature_json_str: str) -> tuple:
    """Convierte un feature JSON serializado en una tupla compatible con PARSED_ROW_SCHEMA."""
    try:
        feature = json.loads(feature_json_str)
    except Exception as exc:
        return (
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "CORRUPT_JSON",
            "corrupt_row_exception",
            f"JSON inválido en feature: {exc}",
        )

    silver_row, rejection = transform_geojson_feature_to_silver_row(feature)
    if silver_row is not None:
        return (
            True,
            silver_row["id"],
            silver_row["mag"],
            silver_row["mag_type"],
            silver_row["place"],
            silver_row["time"],
            silver_row["updated"],
            silver_row["lat"],
            silver_row["lon"],
            silver_row["depth_km"],
            silver_row["tsunami"],
            silver_row["sig"],
            silver_row["status"],
            silver_row["type"],
            silver_row["url"],
            silver_row["region"],
            silver_row["event_date"],
            None,
            None,
            None,
        )
    rej = rejection or {"id": "UNKNOWN", "regla": "unknown", "motivo": "Rechazo desconocido"}
    return (
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        str(rej.get("id", "UNKNOWN")),
        str(rej.get("regla", "unknown")),
        str(rej.get("motivo", "")),
    )


def transform_features_with_spark(
    spark: SparkSession,
    features: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Transforma una lista de features GeoJSON usando un DataFrame de PySpark,
    aplicando reglas por fila y deduplicación por `id` conservando el mayor `updated`.
    """
    serialized = [(json.dumps(f),) for f in features]
    if not serialized:
        empty_df = spark.createDataFrame([], PARSED_ROW_SCHEMA).select(*SILVER_COLS)
        return {"valid_df": empty_df, "rejections": []}

    raw_df = spark.createDataFrame(serialized, ["feature_json"])
    parse_udf = F.udf(_feature_to_spark_tuple, PARSED_ROW_SCHEMA)
    parsed_df = raw_df.withColumn("parsed", parse_udf(F.col("feature_json"))).select("parsed.*")

    # 1. Separar filas rechazadas por reglas de dominio
    invalid_rows = (
        parsed_df.filter(~F.col("is_valid"))
        .select("reject_id", "reject_rule", "reject_reason")
        .collect()
    )
    rejections: List[Dict[str, str]] = [
        {
            "id": r["reject_id"] or "UNKNOWN",
            "regla": r["reject_rule"] or "unknown",
            "motivo": r["reject_reason"] or "",
        }
        for r in invalid_rows
    ]

    # 2. Deduplicar filas válidas por `id` conservando el mayor `updated` con Window de Spark
    valid_candidates = parsed_df.filter(F.col("is_valid")).select(*SILVER_COLS)
    window_spec = Window.partitionBy("id").orderBy(F.col("updated").desc())
    ranked_df = valid_candidates.withColumn("_rn", F.row_number().over(window_spec))

    dup_rows = ranked_df.filter(F.col("_rn") > 1).select("id", "updated").collect()
    for dup in dup_rows:
        rejections.append(
            {
                "id": str(dup["id"]),
                "regla": "duplicate_id",
                "motivo": f"ID duplicado en lote descartado en favor de registro con mayor updated ({dup['updated']}).",
            }
        )

    deduped_df = ranked_df.filter(F.col("_rn") == 1).drop("_rn")
    return {"valid_df": deduped_df, "rejections": rejections}


def get_latest_bronze_uris(limit: int = 3) -> Dict[str, Any]:
    """
    Consulta en `raw.ingestions` (usando `transform_role`) las rutas S3A más recientes
    pendientes o recién ingresadas y su timestamp de ingesta para evaluar frescura.
    """
    from common.config import get_db_connection

    conn = get_db_connection(role="transform")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT file_path, timestamp
                FROM raw.ingestions
                WHERE status = 'SUCCESS'
                ORDER BY timestamp DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
            if not rows:
                return {"s3_uris": [], "latest_ts_iso": datetime.now(timezone.utc).isoformat()}
            s3_uris = list(dict.fromkeys(r[0] for r in rows))
            latest_ts = max(r[1] for r in rows)
            return {
                "s3_uris": s3_uris,
                "latest_ts_iso": latest_ts.astimezone(timezone.utc).isoformat(),
            }
    finally:
        conn.close()


def _extract_features_from_bronze_s3(s3_uris: List[str]) -> List[Dict[str, Any]]:
    """Lee los archivos GeoJSON en Bronce desde MinIO y extrae su arreglo `features`."""
    cfg = get_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_user,
        aws_secret_access_key=cfg.minio_password,
        region_name="us-east-1",
    )
    all_features: List[Dict[str, Any]] = []
    for uri in s3_uris:
        clean = uri.replace("s3a://", "").replace("s3://", "")
        bucket, key = clean.split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        feats = payload.get("features", [])
        if isinstance(feats, list):
            all_features.extend(feats)
    return all_features


def run_bronze_to_silver(
    s3_uris: List[str],
    run_id: str = "manual",
) -> Dict[str, Any]:
    """
    Ejecuta el job Spark Bronce → Plata sobre los archivos `s3_uris` indicados,
    escribiendo Parquet particionado por `event_date` en `s3a://silver/events`
    con sobrescritura dinámica de partición e idempotencia completa.
    """
    cfg = get_config()
    spark = create_spark_session("SismoWatch-BronzeToSilver")
    silver_uri = f"s3a://{cfg.bucket_silver}/events"

    features = _extract_features_from_bronze_s3(s3_uris)
    result = transform_features_with_spark(spark, features)
    batch_valid_df: DataFrame = result["valid_df"]
    rejections: List[Dict[str, str]] = result["rejections"]

    # Materializar en caché para conteo y validación sin recalcular
    batch_valid_df.cache()
    approved_count = batch_valid_df.count()

    if approved_count > 0:
        affected_dates = [r["event_date"] for r in batch_valid_df.select("event_date").distinct().collect()]
        merged_df = batch_valid_df
        try:
            existing_silver_df = spark.read.parquet(silver_uri).filter(F.col("event_date").isin(affected_dates))
            merged_df = existing_silver_df.select(*SILVER_COLS).unionByName(batch_valid_df.select(*SILVER_COLS))
            w_merge = Window.partitionBy("id").orderBy(F.col("updated").desc())
            merged_df = (
                merged_df.withColumn("_rn", F.row_number().over(w_merge))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
        except Exception:
            # Primera escritura en s3a://silver/events cuando aún no existen particiones previas
            merged_df = batch_valid_df

        (
            merged_df.write.mode("overwrite")
            .partitionBy("event_date")
            .parquet(silver_uri)
        )

    batch_pdf = batch_valid_df.toPandas()
    batch_valid_df.unpersist()

    log_event(
        logger,
        "info",
        "bronze_to_silver_completed",
        run_id,
        {
            "silver_uri": silver_uri,
            "approved_count": approved_count,
            "rejected_count": len(rejections),
        },
    )

    return {
        "silver_uri": silver_uri,
        "approved_count": approved_count,
        "rejected_count": len(rejections),
        "rejections": rejections,
        "batch_pdf": batch_pdf,
        "completed_at": datetime.now(timezone.utc),
    }
