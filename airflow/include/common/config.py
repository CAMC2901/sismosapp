"""
Configuración centralizada y fábrica de conexiones por rol de mínimo privilegio.
"""
import os
from dataclasses import dataclass
import psycopg2
from psycopg2.extensions import connection as PgConnection


@dataclass(frozen=True)
class AppConfig:
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "sismowatch")

    db_ingest_user: str = os.getenv("DB_INGEST_USER", "ingest_role")
    db_ingest_password: str = os.getenv("DB_INGEST_PASSWORD", "sw_ingest_local_9812")

    db_transform_user: str = os.getenv("DB_TRANSFORM_USER", "transform_role")
    db_transform_password: str = os.getenv("DB_TRANSFORM_PASSWORD", "sw_transform_local_9812")

    db_ai_user: str = os.getenv("DB_AI_USER", "ai_role")
    db_ai_password: str = os.getenv("DB_AI_PASSWORD", "sw_ai_local_9812")

    db_reader_user: str = os.getenv("DB_READER_USER", "reader_role")
    db_reader_password: str = os.getenv("DB_READER_PASSWORD", "sw_reader_local_9812")

    db_api_rw_user: str = os.getenv("DB_API_RW_USER", "api_rw_role")
    db_api_rw_password: str = os.getenv("DB_API_RW_PASSWORD", "sw_apirw_local_9812")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user: str = os.getenv("MINIO_ROOT_USER", "sismowatch_minio")
    minio_password: str = os.getenv("MINIO_ROOT_PASSWORD", "sw_minio_local_9812")
    bucket_bronze: str = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    bucket_silver: str = os.getenv("MINIO_BUCKET_SILVER", "silver")
    bucket_gold: str = os.getenv("MINIO_BUCKET_GOLD", "gold")

    usgs_feed_url: str = os.getenv(
        "USGS_FEED_URL",
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    )
    usgs_query_url: str = os.getenv(
        "USGS_QUERY_URL",
        "https://earthquake.usgs.gov/fdsnws/event/1/query",
    )
    backfill_default_days: int = int(os.getenv("BACKFILL_DEFAULT_DAYS", "30"))
    backfill_min_mag: float = float(os.getenv("BACKFILL_MIN_MAGNITUDE", "2.5"))

    freshness_max_minutes: float = float(os.getenv("FRESHNESS_MAX_MINUTES", "180"))
    volume_min: int = int(os.getenv("VOLUME_MIN", "0"))
    volume_max: int = int(os.getenv("VOLUME_MAX", "25000"))
    invalid_ratio_max: float = float(os.getenv("INVALID_RATIO_MAX", "0.20"))

    llm_provider: str = os.getenv("LLM_PROVIDER", "groq")
    groq_api_key: str = os.getenv("GROQ_API_KEY", "")
    groq_model: str = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    ollama_url: str = os.getenv("OLLAMA_URL", "http://ollama:11434")
    ollama_fallback_model: str = os.getenv("OLLAMA_FALLBACK_MODEL", "qwen2.5:3b")
    ollama_embed_model: str = os.getenv("OLLAMA_EMBED_MODEL", "bge-m3")
    alert_webhook_url: str = os.getenv(
        "ALERT_WEBHOOK_URL", "http://api:8000/api/internal/webhook-sink"
    )


def get_config() -> AppConfig:
    return AppConfig()


def get_db_connection(role: str = "reader", connect_timeout: int = 10) -> PgConnection:
    """
    Devuelve una conexión PostgreSQL autenticada con el rol de mínimo privilegio solicitado.
    Roles soportados: 'ingest', 'transform', 'ai', 'reader', 'api_rw'.
    """
    cfg = get_config()
    role_map = {
        "ingest": (cfg.db_ingest_user, cfg.db_ingest_password),
        "transform": (cfg.db_transform_user, cfg.db_transform_password),
        "ai": (cfg.db_ai_user, cfg.db_ai_password),
        "reader": (cfg.db_reader_user, cfg.db_reader_password),
        "api_rw": (cfg.db_api_rw_user, cfg.db_api_rw_password),
    }
    if role not in role_map:
        raise ValueError(f"Rol de BD desconocido: {role}")

    user, password = role_map[role]
    return psycopg2.connect(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        dbname=cfg.postgres_db,
        user=user,
        password=password,
        connect_timeout=connect_timeout,
    )

