"""
Router de endpoints de datos y tiempo real SSE sobre `curated` (`viewer` y `admin`).
"""
import asyncio
from datetime import date, datetime, timezone
import json
from typing import Optional
from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import Response, StreamingResponse
from app.auth.security import require_viewer
from app.models.schemas import (
    KpisResponse,
    PaginatedEventsResponse,
    PaginatedReportsResponse,
    PipelineStatusResponse,
    TimeseriesStatsResponse,
    UserInfo,
)
from app.services.data_service import (
    export_reports_content,
    fetch_events,
    fetch_kpis,
    fetch_pipeline_status,
    fetch_reports,
    fetch_timeseries_stats,
)

router = APIRouter(prefix="/api", tags=["Datos Sísmicos y Reportes"])


@router.get("/kpis", response_model=KpisResponse)
async def get_kpis(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    min_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    region: Optional[str] = Query(default=None, max_length=120),
    _user: UserInfo = Depends(require_viewer),
) -> KpisResponse:
    """Devuelve los 6 KPIs ejecutivos desde `curated`, sincronizados con los filtros activos."""
    return fetch_kpis(
        start_date=start_date,
        end_date=end_date,
        min_mag=min_mag,
        region=region,
    )


@router.get("/events", response_model=PaginatedEventsResponse)
async def get_events(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    min_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    region: Optional[str] = Query(default=None, max_length=120),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=300),
    _user: UserInfo = Depends(require_viewer),
) -> PaginatedEventsResponse:
    """Listado paginado de eventos sísmicos validados con filtros dinámicos."""
    return fetch_events(
        start_date=start_date,
        end_date=end_date,
        min_mag=min_mag,
        region=region,
        page=page,
        page_size=page_size,
    )


@router.get("/stats/timeseries", response_model=TimeseriesStatsResponse)
async def get_timeseries(
    hours: int = Query(default=72, ge=6, le=720),
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    min_mag: Optional[float] = Query(default=None, ge=-2.0, le=10.0),
    region: Optional[str] = Query(default=None, max_length=120),
    _user: UserInfo = Depends(require_viewer),
) -> TimeseriesStatsResponse:
    """Devuelve la serie horaria, distribución por magnitud y top regiones."""
    return fetch_timeseries_stats(
        hours=hours,
        start_date=start_date,
        end_date=end_date,
        min_mag=min_mag,
        region=region,
    )


@router.get("/reports", response_model=PaginatedReportsResponse)
async def get_reports(
    search: Optional[str] = Query(default=None, max_length=120),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc|ASC|DESC)$"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=50),
    _user: UserInfo = Depends(require_viewer),
) -> PaginatedReportsResponse:
    """Historial paginado de reportes narrativos diarios de IA con búsqueda y orden."""
    return fetch_reports(search=search, sort_dir=sort_dir, page=page, page_size=page_size)


@router.get("/reports/export")
async def export_reports(
    format: str = Query(default="csv", pattern="^(csv|html|pdf|CSV|HTML|PDF)$"),
    _user: UserInfo = Depends(require_viewer),
) -> Response:
    """Exporta reportes diarios y eventos sísmicos en formato CSV o HTML/PDF descargable."""
    content_str, media_type, filename = export_reports_content(fmt=format)
    return Response(
        content=content_str,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/pipeline/status", response_model=PipelineStatusResponse)
async def get_pipeline_status(user: UserInfo = Depends(require_viewer)) -> PipelineStatusResponse:
    """
    Estado de los 4 DAGs, calidad y frescura.
    - `viewer`: ve el resumen.
    - `admin`: ve además `admin_detail` completo.
    """
    return fetch_pipeline_status(is_admin=(user.role == "admin"))


@router.get("/stream")
async def stream_seismic_events(
    request: Request,
    last_event_id: Optional[str] = Header(default=None, alias="Last-Event-ID"),
    max_ticks: Optional[int] = Query(default=None, ge=1, le=10),
    _user: UserInfo = Depends(require_viewer),
) -> StreamingResponse:
    """
    Stream Server-Sent Events (SSE) en tiempo real de consumo (Sección 2 y 12).
    - Emite un evento inicial de sincronización y compara periódicamente `data_version`
      y los IDs más recientes en `curated.events` para empujar nuevos sismos sin recargar.
    - Incluye `heartbeat` periódico y soporte de `Last-Event-ID`.
    """

    async def event_generator():
        seen_version = last_event_id or ""
        seen_top_id = ""
        ticks = 0

        while True:
            if await request.is_disconnected():
                break

            ticks += 1
            try:
                kpis = fetch_kpis()
                latest_page = fetch_events(page=1, page_size=5)
                latest_items = [ev.model_dump(mode="json") for ev in latest_page.items]
                top_id = latest_items[0]["id"] if latest_items else "none"
                current_event_id = f"{kpis.data_version}:{top_id}"

                if current_event_id != seen_version or top_id != seen_top_id or ticks == 1:
                    seen_version = current_event_id
                    seen_top_id = top_id
                    payload = {
                        "type": "seismic_update",
                        "data_version": kpis.data_version,
                        "kpis": kpis.model_dump(mode="json"),
                        "latest_events": latest_items,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    yield (
                        f"id: {current_event_id}\n"
                        f"event: seismic_update\n"
                        f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    )
                else:
                    hb = {
                        "type": "heartbeat",
                        "data_version": kpis.data_version,
                        "freshness_minutes": kpis.freshness_minutes,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    yield (
                        f"id: {current_event_id}\n"
                        f"event: heartbeat\n"
                        f"data: {json.dumps(hb, ensure_ascii=False)}\n\n"
                    )
            except Exception:
                yield f"event: heartbeat\ndata: {json.dumps({'type': 'heartbeat', 'status': 'degraded'})}\n\n"

            if max_ticks is not None and ticks >= max_ticks:
                break
            await asyncio.sleep(5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

