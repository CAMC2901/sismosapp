"""
Configuración central de la API FastAPI y conexiones de BD con mínimo privilegio.

Regla de seguridad obligatoria (Sección 2, 7 y 12):
- Para todas las consultas de negocio y del agente conversacional, la API se conecta
  exclusivamente con `reader_role` (solo SELECT en `curated`; acceso revocado a `raw` y `staging`).
- Para persistir caché semántica (`curated.ai_cache`), telemetría (`curated.ai_runs`) y sesiones
  de chat (`curated.chat_sessions`), usa `api_rw_role`.
- Para regenerar el reporte diario cuando lo solicita un `admin`, usa `ai_role`.
"""
import os
from dataclasses import dataclass, field
from typing import List
import psycopg2
from psycopg2.extensions import connection as PgConnection


def _parse_cors_origins() -> List[str]:
    raw = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
    return [o.strip() for o in raw.split(",") if o.strip()]


@dataclass(frozen=True)
class ApiSettings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "sismowatch")

    db_reader_user: str = os.getenv("DB_READER_USER", "reader_role")
    db_reader_password: str = os.getenv("DB_READER_PASSWORD", "sw_reader_local_9812")

    db_api_rw_user: str = os.getenv("DB_API_RW_USER", "api_rw_role")
    db_api_rw_password: str = os.getenv("DB_API_RW_PASSWORD", "sw_apirw_local_9812")

    db_ai_user: str = os.getenv("DB_AI_USER", "ai_role")
    db_ai_password: str = os.getenv("DB_AI_PASSWORD", "sw_ai_local_9812")

    jwt_secret: str = os.getenv(
        "JWT_SECRET", "sw_jwt_secret_key_superapp_sismowatch_2026_local_only"
    )
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "120"))

    admin_username: str = os.getenv("ADMIN_USERNAME", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "AdminSismo2026!")
    viewer_username: str = os.getenv("VIEWER_USERNAME", "viewer")
    viewer_password: str = os.getenv("VIEWER_PASSWORD", "ViewerSismo2026!")

    cors_origins: List[str] = field(default_factory=_parse_cors_origins)

    airflow_api_url: str = os.getenv(
        "AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1"
    ).rstrip("/")
    airflow_api_user: str = os.getenv("AIRFLOW_API_USER", "sismowatch_admin")
    airflow_api_password: str = os.getenv("AIRFLOW_API_PASSWORD", "sw_airflow_admin_9812")

    llm_provider: str = os.getenv("LLM_PROVIDER", "groq")
    groq_api_key: str = os.getenv("GROQ_API_KEY", "")
    groq_model: str = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    ollama_url: str = os.getenv("OLLAMA_URL", "http://ollama:11434").rstrip("/")
    ollama_fallback_model: str = os.getenv("OLLAMA_FALLBACK_MODEL", "qwen2.5:3b")
    ollama_embed_model: str = os.getenv("OLLAMA_EMBED_MODEL", "bge-m3")
    ai_cache_ttl: int = int(os.getenv("AI_CACHE_TTL", "3600"))
    freshness_max_minutes: float = float(os.getenv("FRESHNESS_MAX_MINUTES", "180"))


def get_settings() -> ApiSettings:
    return ApiSettings()


def get_db_conn(role: str = "reader", timeout: int = 5) -> PgConnection:
    """
    Crea una conexión PostgreSQL con el rol de mínimo privilegio indicado ('reader', 'api_rw', 'ai').
    Lanza `DatabaseUnavailableError` si PostgreSQL no responde para que el manejador global devuelva 503.
    """
    from app.core.exceptions import DatabaseUnavailableError

    s = get_settings()
    roles = {
        "reader": (s.db_reader_user, s.db_reader_password),
        "api_rw": (s.db_api_rw_user, s.db_api_rw_password),
        "ai": (s.db_ai_user, s.db_ai_password),
    }
    if role not in roles:
        raise ValueError(f"Rol de BD inválido en API: {role}")

    user, pwd = roles[role]
    try:
        return psycopg2.connect(
            host=s.postgres_host,
            port=s.postgres_port,
            dbname=s.postgres_db,
            user=user,
            password=pwd,
            connect_timeout=timeout,
        )
    except psycopg2.OperationalError as exc:
        raise DatabaseUnavailableError(
            "El servicio de base de datos PostgreSQL no se encuentra disponible temporalmente."
        ) from exc
