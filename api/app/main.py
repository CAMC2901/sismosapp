"""
Punto de entrada principal del servicio FastAPI de SismoWatch (Sección 12).
- Documentación OpenAPI disponible en `/docs`.
- Healthcheck en `/health`.
- CORS restringido a `CORS_ORIGINS`.
- Manejador global de excepciones uniforme para 401, 403, 404, 422, 429 y 503.
"""
from datetime import datetime, timezone
from typing import Any, Dict
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import get_db_conn, get_settings
from app.core.exceptions import register_exception_handlers
from app.routers import admin_router, agent_router, auth_router, data_router

settings = get_settings()

app = FastAPI(
    title="SismoWatch API — SuperApp",
    description=(
        "API de servicio sobre la capa Oro (`curated`) del producto de monitoreo sísmico global "
        "casi en tiempo real SismoWatch. Implementa JWT + RBAC (`viewer` / `admin`), Server-Sent Events (SSE) "
        "y Agente Conversacional en cascada de 4 niveles."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Last-Event-ID"],
)

register_exception_handlers(app)

app.include_router(auth_router.router)
app.include_router(data_router.router)
app.include_router(admin_router.router)
app.include_router(agent_router.router)


@app.get("/health", tags=["Healthcheck"])
async def healthcheck() -> Dict[str, Any]:
    """Verifica la disponibilidad de la API y la conexión de solo lectura a PostgreSQL curated."""
    conn = get_db_conn(role="reader", timeout=3)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
    finally:
        conn.close()
    return {
        "status": "healthy",
        "service": "sismowatch-api",
        "db_role": "reader_role",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/internal/webhook-sink", tags=["Alertas Internas"])
async def internal_webhook_sink(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Receptor HTTP por defecto para alertas de `on_failure_callback` cuando no se usa Slack/Telegram externo."""
    return {
        "received": True,
        "service": payload.get("service", "SismoWatch"),
        "dag_id": payload.get("dag_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
