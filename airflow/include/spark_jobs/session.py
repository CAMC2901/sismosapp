"""
Constructor de SparkSession para la arquitectura Medallion de SismoWatch.

Decisión de diseño:
- Ejecuta en modo `local[2]` dentro del contenedor de Airflow para mantener la huella
  de memoria bajo el presupuesto de 9-10 GB en un host de 16 GB RAM sin GPU.
- El mismo código PySpark escala horizontalmente a un clúster Spark Standalone, YARN
  o Kubernetes simplemente cambiando `SPARK_MASTER` (por ejemplo `spark://spark-master:7077`)
  sin modificar una sola línea de lógica de transformación.
- Usa `spark.sql.sources.partitionOverwriteMode=dynamic` para garantizar idempotencia
  al reprocesar fechas individuales sin afectar el resto de particiones históricas.
"""
import os
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "SismoWatch-Medallion") -> SparkSession:
    """Crea y configura una sesión PySpark con conector S3A hacia MinIO."""
    spark_master = os.getenv("SPARK_MASTER", "local[2]")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "sismowatch_minio")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "sw_minio_local_9812")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.driver.memory", "768m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # Configuración S3A hacia MinIO sin SSL local y con path-style access
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.timeout", "15000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    )
    return builder.getOrCreate()

