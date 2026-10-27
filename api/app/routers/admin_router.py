"""
Router exclusivo para rol `admin` (Sección 12).
Un usuario con rol `viewer` recibe HTTP 403 Forbidden con mensaje claro en cualquiera de estas rutas.
"""
from typing import Optional
from fastapi import APIRouter, Body, Depends, Query
from app.agent.orchestrator import regenerate_daily_report_via_api
from app.auth.security import require_admin
from app.models.schemas import (
    AdminActionResponse,
    BackfillTriggerRequest,
    QuarantineResponse,
    UserInfo,
)
from app.services.data_service import (
    fetch_quarantine_rejections,
    trigger_airflow_backfill,
)

router = APIRouter(prefix="/api", tags=["Administración (Solo Admin)"])


@router.get("/quality/quarantine", response_model=QuarantineResponse)
async def get_quality_quarantine(
    limit: int = Query(default=25, ge=1, le=100),
    _admin: UserInfo = Depends(require_admin),
) -> QuarantineResponse:
    """Devuelve el log liviano de rechazos por fila (`{id, regla, motivo}`) desde `curated.quality_results`."""
    return fetch_quarantine_rejections(limit=limit)


@router.post("/admin/backfill", response_model=AdminActionResponse)
async def post_admin_backfill(
    payload: Optional[BackfillTriggerRequest] = Body(default=None),
    _admin: UserInfo = Depends(require_admin),
) -> AdminActionResponse:
    """Dispara el DAG `usgs_backfill` a través de la API REST de Airflow."""
    days = payload.days if payload else 30
    minmag = payload.minmagnitude if payload else 2.5
    return trigger_airflow_backfill(days=days, minmagnitude=minmag)


@router.post("/admin/reports/regenerate", response_model=AdminActionResponse)
async def post_admin_regenerate_report(
    _admin: UserInfo = Depends(require_admin),
) -> AdminActionResponse:
    """Regenera el reporte ejecutivo diario de IA con verificación de cifras SQL."""
    return regenerate_daily_report_via_api()

