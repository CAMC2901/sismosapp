"""
Transformación PySpark: Plata → Oro (Sección 9).

Lee los eventos tipados y deduplicados desde `s3a://silver/events` y produce en `s3a://gold/`:
1. `s3a://gold/events`: eventos válidos listos para análisis y publicación.
2. `s3a://gold/agg_hourly`: serie horaria (`hora`, `conteo`, `mag_max`, `mag_promedio`, `tsunami`).
3. `s3a://gold/agg_region`: agregación diaria por región (`region`, `fecha`, `conteo`, `mag_max`, `mag_promedio`).
"""
from datetime import datetime, timezone
from typing import Any, Dict
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from common.config import get_config
from common.logging_utils import get_structured_logger, log_event
from spark_jobs.session import create_spark_session

logger = get_structured_logger("sismowatch.spark.silver_to_gold")


def build_gold_aggregations(silver_df: DataFrame) -> Dict[str, DataFrame]:
    """
    Funciones puras de transformación Spark que reciben un DataFrame de Plata
    y devuelven los 3 DataFrames de Oro (`events`, `agg_hourly`, `agg_region`).
    """
    w_dedup = Window.partitionBy("id").orderBy(F.col("updated").desc())
    events_gold = (
        silver_df.withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    agg_hourly = (
        events_gold.withColumn("hora", F.date_trunc("hour", F.col("time")))
        .groupBy("hora")
        .agg(
            F.count("*").cast("int").alias("conteo"),
            F.round(F.max("mag"), 2).alias("mag_max"),
            F.round(F.avg("mag"), 2).alias("mag_promedio"),
            F.sum(F.col("tsunami")).cast("int").alias("tsunami"),
        )
        .orderBy(F.col("hora").desc())
    )

    agg_region = (
        events_gold.withColumn("fecha", F.to_date(F.col("time")))
        .groupBy("region", "fecha")
        .agg(
            F.count("*").cast("int").alias("conteo"),
            F.round(F.max("mag"), 2).alias("mag_max"),
            F.round(F.avg("mag"), 2).alias("mag_promedio"),
        )
        .orderBy(F.col("fecha").desc(), F.col("conteo").desc())
    )

    return {
        "events": events_gold,
        "agg_hourly": agg_hourly,
        "agg_region": agg_region,
    }


def run_silver_to_gold(run_id: str = "manual") -> Dict[str, Any]:
    """
    Ejecuta el job Spark Plata → Oro y escribe los 3 datasets Parquet en `s3a://gold/`.
    """
    cfg = get_config()
    spark = create_spark_session("SismoWatch-SilverToGold")
    silver_uri = f"s3a://{cfg.bucket_silver}/events"
    gold_base = f"s3a://{cfg.bucket_gold}"

    events_uri = f"{gold_base}/events"
    hourly_uri = f"{gold_base}/agg_hourly"
    region_uri = f"{gold_base}/agg_region"

    try:
        silver_df = spark.read.parquet(silver_uri)
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "silver_empty_on_gold_run",
            run_id,
            {"silver_uri": silver_uri, "reason": str(exc)},
        )
        return {
            "events_uri": events_uri,
            "hourly_uri": hourly_uri,
            "region_uri": region_uri,
            "events_count": 0,
            "hourly_count": 0,
            "region_count": 0,
            "data_version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        }

    gold_dfs = build_gold_aggregations(silver_df)

    events_df = gold_dfs["events"]
    hourly_df = gold_dfs["agg_hourly"]
    region_df = gold_dfs["agg_region"]

    events_df.write.mode("overwrite").partitionBy("event_date").parquet(events_uri)
    hourly_df.write.mode("overwrite").parquet(hourly_uri)
    region_df.write.mode("overwrite").parquet(region_uri)

    events_count = events_df.count()
    hourly_count = hourly_df.count()
    region_count = region_df.count()
    data_version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    summary = {
        "events_uri": events_uri,
        "hourly_uri": hourly_uri,
        "region_uri": region_uri,
        "events_count": events_count,
        "hourly_count": hourly_count,
        "region_count": region_count,
        "data_version": data_version,
    }
    log_event(logger, "info", "silver_to_gold_completed", run_id, summary)
    return summary

