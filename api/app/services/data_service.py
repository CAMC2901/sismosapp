"""
Servicios de consulta sobre el schema `curated` usando `reader_role` (Sección 7 y 12).
"""
import csv
from datetime import date, datetime, timezone
import io
import json
import math
from typing import Any, Dict, List, Optional, Tuple
import requests
from app.core.config import get_db_conn, get_settings
from app.models.schemas import (
    AdminActionResponse,
    DailyReportItem,
    HourlyPoint,
    KpisResponse,
    MagnitudeDistributionPoint,
    PaginatedEventsResponse,
    PaginatedReportsResponse,
    PipelineStatusResponse,
    QuarantineBatchItem,
    QuarantineResponse,
    RegionActivityPoint,
    SeismicEventItem,
    TimeseriesStatsResponse,
)


def get_current_data_version(conn: Any = None) -> str:
    """Obtiene la marca `data_version` más reciente publicada en `curated.pipeline_runs`."""
    own_conn = False
    if conn is None:
        conn = get_db_conn(role="reader")
        own_conn = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_version, COALESCE(fin, inicio)
                FROM curated.pipeline_runs
                WHERE data_version IS NOT NULL AND estado = 'SUCCESS'
                ORDER BY COALESCE(fin, inicio) DESC
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
            cur.execute("SELECT COUNT(*), MAX(updated) FROM curated.events;")
            ev_row = cur.fetchone()
            if ev_row and ev_row[0]:
                ts = ev_row[1].strftime("%Y%m%dT%H%M%SZ") if ev_row[1] else "0"
                return f"v_{ev_row[0]}_{ts}"
            return "v_initial"
    finally:
        if own_conn:
            conn.close()


def fetch_kpis() -> KpisResponse:
    """
    Calcula los 6 KPIs obligatorios desde `curated`:
    1. Eventos en 24h (si las últimas 24h tienen 0 y existe histórico, informa las 24h reales).
    2. Magnitud máxima (en la ventana de 24h, o global si 24h es 0).
    3. Magnitud promedio.
    4. Eventos con alerta de tsunami.
    5. Puntaje de calidad más reciente (`curated.quality_results`).
    6. Hora de última actualización y frescura (`curated.pipeline_runs`).
    """
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MAX(mag)::numeric, 1), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(SUM(tsunami), 0)::int
                FROM curated.events
                WHERE time >= NOW() - INTERVAL '24 hours';
                """
            )
            ev_24h, mag_max, mag_avg, tsu_count = cur.fetchone()

            if int(ev_24h) == 0:
                # Si aún no hay sismos en las últimas 24h exactas pero sí en el lote cargado
                cur.execute(
                    """
                    SELECT
                        COUNT(*)::int,
                        COALESCE(ROUND(MAX(mag)::numeric, 1), 0.0)::float,
                        COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                        COALESCE(SUM(tsunami), 0)::int
                    FROM curated.events;
                    """
                )
                ev_24h, mag_max, mag_avg, tsu_count = cur.fetchone()

            cur.execute(
                """
                SELECT puntaje, timestamp
                FROM curated.quality_results
                ORDER BY timestamp DESC
                LIMIT 1;
                """
            )
            q_row = cur.fetchone()
            quality_score = round(float(q_row[0]) * 100.0, 1) if q_row else 100.0

            cur.execute(
                """
                SELECT COALESCE(fin, inicio), data_version
                FROM curated.pipeline_runs
                WHERE estado = 'SUCCESS'
                ORDER BY COALESCE(fin, inicio) DESC
                LIMIT 1;
                """
            )
            p_row = cur.fetchone()
            last_updated = p_row[0] if p_row else (q_row[1] if q_row else None)

            if last_updated is None:
                cur.execute("SELECT MAX(ingested_at) FROM curated.events;")
                r_ing = cur.fetchone()
                last_updated = r_ing[0] if r_ing else None

            freshness_min = 0.0
            if last_updated is not None:
                lu_utc = (
                    last_updated.astimezone(timezone.utc)
                    if last_updated.tzinfo
                    else last_updated.replace(tzinfo=timezone.utc)
                )
                freshness_min = round(
                    max(0.0, (datetime.now(timezone.utc) - lu_utc).total_seconds() / 60.0),
                    1,
                )

            dv = get_current_data_version(conn)

            return KpisResponse(
                events_24h=int(ev_24h),
                max_magnitude=float(mag_max),
                avg_magnitude=float(mag_avg),
                tsunami_alerts=int(tsu_count),
                quality_score=quality_score,
                last_updated=last_updated,
                freshness_minutes=freshness_min,
                data_version=dv,
            )
    finally:
        conn.close()


def _build_event_params(
    start_date: Optional[date],
    end_date: Optional[date],
    min_mag: Optional[float],
    region: Optional[str],
) -> List[Any]:
    start_val = start_date.isoformat() if start_date is not None else None
    end_val = end_date.isoformat() if end_date is not None else None
    mag_val = float(min_mag) if min_mag is not None else None
    reg_val = (
        f"%{region.strip()}%"
        if (region and region.strip() and region.strip().lower() != "all")
        else None
    )
    return [start_val, start_val, end_val, end_val, mag_val, mag_val, reg_val, reg_val]


def fetch_events(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_mag: Optional[float] = None,
    region: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> PaginatedEventsResponse:
    """Consulta paginada de `curated.events` con filtros por fecha, magnitud mínima y región."""
    conn = get_db_conn(role="reader")
    params = _build_event_params(start_date, end_date, min_mag, region)
    offset = max(0, (page - 1) * page_size)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int
                FROM curated.events
                WHERE (%s::date IS NULL OR time >= %s::date)
                  AND (%s::date IS NULL OR time < (%s::date + INTERVAL '1 day'))
                  AND (%s::float IS NULL OR mag >= %s::float)
                  AND (%s::text IS NULL OR region ILIKE %s::text);
                """,
                params,
            )
            total = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT
                    id, ROUND(mag::numeric, 2)::float, mag_type, place, time, updated,
                    ROUND(lat::numeric, 4)::float, ROUND(lon::numeric, 4)::float,
                    ROUND(depth_km::numeric, 2)::float, tsunami, sig, status, type, url, region
                FROM curated.events
                WHERE (%s::date IS NULL OR time >= %s::date)
                  AND (%s::date IS NULL OR time < (%s::date + INTERVAL '1 day'))
                  AND (%s::float IS NULL OR mag >= %s::float)
                  AND (%s::text IS NULL OR region ILIKE %s::text)
                ORDER BY time DESC, mag DESC
                LIMIT %s OFFSET %s;
                """,
                [*params, page_size, offset],
            )
            items = [
                SeismicEventItem(
                    id=str(r[0]),
                    mag=float(r[1]),
                    mag_type=r[2],
                    place=str(r[3]),
                    time=r[4],
                    updated=r[5],
                    lat=float(r[6]),
                    lon=float(r[7]),
                    depth_km=float(r[8]),
                    tsunami=int(r[9]),
                    sig=int(r[10]),
                    status=r[11],
                    type=r[12],
                    url=r[13],
                    region=str(r[14]),
                )
                for r in cur.fetchall()
            ]
        total_pages = max(1, math.ceil(total / page_size))
        return PaginatedEventsResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )
    finally:
        conn.close()


def fetch_timeseries_stats(
    hours: int = 72,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_mag: Optional[float] = None,
    region: Optional[str] = None,
) -> TimeseriesStatsResponse:
    """
    Devuelve la serie temporal por hora, la distribución por magnitud y las regiones más activas.
    Aplica los mismos filtros dinámicos de fecha/magnitud/región cuando el usuario filtra el dashboard.
    """
    conn = get_db_conn(role="reader")
    params = _build_event_params(start_date, end_date, min_mag, region)

    try:
        with conn.cursor() as cur:
            # 1. Serie horaria (si no hay filtros específicos, podemos usar agg_hourly o calcular sobre el filtro)
            cur.execute(
                """
                SELECT
                    date_trunc('hour', time) AS hora,
                    COUNT(*)::int AS conteo,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float AS mag_max,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float AS mag_promedio,
                    COALESCE(SUM(tsunami), 0)::int AS tsunami
                FROM curated.events
                WHERE (%s::date IS NULL OR time >= %s::date)
                  AND (%s::date IS NULL OR time < (%s::date + INTERVAL '1 day'))
                  AND (%s::float IS NULL OR mag >= %s::float)
                  AND (%s::text IS NULL OR region ILIKE %s::text)
                GROUP BY date_trunc('hour', time)
                ORDER BY hora DESC
                LIMIT %s;
                """,
                [*params, max(12, min(hours, 720))],
            )
            hourly_rows = cur.fetchall()
            hourly = [
                HourlyPoint(
                    hora=r[0],
                    conteo=int(r[1]),
                    mag_max=float(r[2]),
                    mag_promedio=float(r[3]),
                    tsunami=int(r[4]),
                )
                for r in reversed(hourly_rows)
            ]

            # 2. Distribución por rangos de magnitud
            cur.execute(
                """
                SELECT
                    CASE
                        WHEN mag < 3.0 THEN '< 3.0 (Micro)'
                        WHEN mag < 4.0 THEN '3.0 - 3.9 (Menor)'
                        WHEN mag < 5.0 THEN '4.0 - 4.9 (Ligero)'
                        WHEN mag < 6.0 THEN '5.0 - 5.9 (Moderado)'
                        WHEN mag < 7.0 THEN '6.0 - 6.9 (Fuerte)'
                        ELSE '7.0+ (Mayor/Gran)'
                    END AS rango,
                    COUNT(*)::int AS conteo
                FROM curated.events
                WHERE (%s::date IS NULL OR time >= %s::date)
                  AND (%s::date IS NULL OR time < (%s::date + INTERVAL '1 day'))
                  AND (%s::float IS NULL OR mag >= %s::float)
                  AND (%s::text IS NULL OR region ILIKE %s::text)
                GROUP BY 1
                ORDER BY MIN(mag) ASC;
                """,
                params,
            )
            mag_dist = [
                MagnitudeDistributionPoint(rango=str(r[0]), conteo=int(r[1]))
                for r in cur.fetchall()
            ]

            # 3. Top 10 regiones con más actividad
            cur.execute(
                """
                SELECT
                    region,
                    COUNT(*)::int AS conteo,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float AS mag_max,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float AS mag_promedio
                FROM curated.events
                WHERE (%s::date IS NULL OR time >= %s::date)
                  AND (%s::date IS NULL OR time < (%s::date + INTERVAL '1 day'))
                  AND (%s::float IS NULL OR mag >= %s::float)
                  AND (%s::text IS NULL OR region ILIKE %s::text)
                GROUP BY region
                ORDER BY conteo DESC, mag_max DESC
                LIMIT 10;
                """,
                params,
            )
            top_regions = [
                RegionActivityPoint(
                    region=str(r[0]),
                    conteo=int(r[1]),
                    mag_max=float(r[2]),
                    mag_promedio=float(r[3]),
                )
                for r in cur.fetchall()
            ]

            return TimeseriesStatsResponse(
                hourly=hourly,
                magnitude_distribution=mag_dist,
                top_regions=top_regions,
            )
    finally:
        conn.close()


def fetch_reports(
    search: Optional[str] = None,
    sort_dir: str = "desc",
    page: int = 1,
    page_size: int = 10,
) -> PaginatedReportsResponse:
    """Consulta paginada, con búsqueda y orden de `curated.daily_reports`."""
    conn = get_db_conn(role="reader")
    offset = max(0, (page - 1) * page_size)
    q = f"%{search.strip()}%" if (search and search.strip()) else None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int
                FROM curated.daily_reports
                WHERE (%s::text IS NULL OR texto ILIKE %s::text OR proveedor ILIKE %s::text OR fecha::text ILIKE %s::text);
                """,
                (q, q, q, q),
            )
            total = int(cur.fetchone()[0])

            if sort_dir.lower() == "asc":
                cur.execute(
                    """
                    SELECT fecha, json_validado, texto, proveedor, fallback, version_prompt, updated_at
                    FROM curated.daily_reports
                    WHERE (%s::text IS NULL OR texto ILIKE %s::text OR proveedor ILIKE %s::text OR fecha::text ILIKE %s::text)
                    ORDER BY fecha ASC
                    LIMIT %s OFFSET %s;
                    """,
                    (q, q, q, q, page_size, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT fecha, json_validado, texto, proveedor, fallback, version_prompt, updated_at
                    FROM curated.daily_reports
                    WHERE (%s::text IS NULL OR texto ILIKE %s::text OR proveedor ILIKE %s::text OR fecha::text ILIKE %s::text)
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (q, q, q, q, page_size, offset),
                )
            items: List[DailyReportItem] = []
            for r in cur.fetchall():
                j_val = r[1] if isinstance(r[1], dict) else json.loads(r[1])
                items.append(
                    DailyReportItem(
                        fecha=r[0],
                        titulo=str(j_val.get("titulo", f"Reporte Sísmico Diario — {r[0]}")),
                        resumen_ejecutivo=str(j_val.get("resumen_ejecutivo", "")),
                        texto=str(r[2]),
                        proveedor=str(r[3]),
                        fallback=bool(r[4]),
                        version_prompt=str(r[5]),
                        json_validado=j_val,
                        updated_at=r[6],
                    )
                )
        return PaginatedReportsResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
        )
    finally:
        conn.close()


def export_reports_content(fmt: str = "csv") -> Tuple[str, str, str]:
    """
    Genera el contenido descargable de reportes y métricas sísmicas en formato CSV o HTML/PDF.
    Retorna `(content_str, media_type, filename)`.
    """
    reports_page = fetch_reports(page=1, page_size=50)
    events_page = fetch_events(page=1, page_size=100)
    now_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    if fmt.lower() == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "tipo_registro",
                "fecha_hora_utc",
                "id_o_proveedor",
                "magnitud_o_version",
                "region_o_fallback",
                "profundidad_km",
                "descripcion_o_resumen",
            ]
        )
        for rep in reports_page.items:
            writer.writerow(
                [
                    "REPORTE_DIARIO_IA",
                    rep.fecha.isoformat(),
                    rep.proveedor,
                    rep.version_prompt,
                    f"fallback={rep.fallback}",
                    "-",
                    rep.resumen_ejecutivo,
                ]
            )
        for ev in events_page.items:
            writer.writerow(
                [
                    "EVENTO_SISMICO",
                    ev.time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    ev.id,
                    ev.mag,
                    ev.region,
                    ev.depth_km,
                    ev.place,
                ]
            )
        return buf.getvalue(), "text/csv; charset=utf-8", f"sismowatch_export_{now_slug}.csv"

    # HTML / Imprimible a PDF
    rows_rep_html = "".join(
        f"""
        <div class="report-card">
            <h3>{r.titulo} <span class="badge">{r.proveedor} (Prompt: {r.version_prompt})</span></h3>
            <p><strong>Fecha:</strong> {r.fecha.isoformat()} | <strong>Modo Respaldo:</strong> {'Sí' if r.fallback else 'No'}</p>
            <p>{r.resumen_ejecutivo}</p>
        </div>
        """
        for r in reports_page.items
    ) or "<p>No hay reportes diarios generados aún.</p>"

    rows_ev_html = "".join(
        f"""
        <tr>
            <td>{e.time.strftime('%Y-%m-%d %H:%M UTC')}</td>
            <td><strong>M {e.mag:.1f}</strong></td>
            <td>{e.depth_km:.1f} km</td>
            <td>{e.region}</td>
            <td>{e.place}</td>
            <td>{'Alerta Activa' if e.tsunami == 1 else 'Sin alerta'}</td>
        </tr>
        """
        for e in events_page.items[:50]
    )

    html_doc = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte Ejecutivo SismoWatch — SuperApp ({now_slug})</title>
    <style>
        body {{ font-family: system-ui, -apple-system, sans-serif; color: #0f172a; margin: 32px; line-height: 1.5; }}
        h1 {{ color: #b91c1c; border-bottom: 2px solid #ef4444; padding-bottom: 8px; }}
        h2 {{ color: #1e293b; margin-top: 28px; }}
        .report-card {{ border: 1px solid #cbd5e1; border-radius: 8px; padding: 16px; margin-bottom: 16px; background: #f8fafc; }}
        .badge {{ background: #fee2e2; color: #991b1b; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }}
        th, td {{ border: 1px solid #cbd5e1; padding: 8px 10px; text-align: left; }}
        th {{ background: #1e293b; color: #ffffff; }}
        tr:nth-child(even) {{ background: #f1f5f9; }}
        @media print {{ .no-print {{ display: none; }} }}
    </style>
</head>
<body>
    <div class="no-print" style="margin-bottom: 16px;">
        <button onclick="window.print()" style="padding:8px 16px;background:#dc2626;color:#fff;border:none;border-radius:6px;cursor:pointer;">
            Imprimir / Guardar como PDF
        </button>
    </div>
    <h1>SismoWatch — Informe Ejecutivo de Monitoreo Sísmico Global (SuperApp)</h1>
    <p>Generado el {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} | Fuente: USGS Earthquake Hazards Program (Schema Curated)</p>
    <h2>1. Historial de Reportes Ejecutivos Diarios (IA)</h2>
    {rows_rep_html}
    <h2>2. Últimos Eventos Sísmicos Validados (Capa Oro)</h2>
    <table>
        <thead>
            <tr>
                <th>Fecha/Hora (UTC)</th>
                <th>Magnitud</th>
                <th>Profundidad</th>
                <th>Región</th>
                <th>Ubicación Sanitizada</th>
                <th>Tsunami</th>
            </tr>
        </thead>
        <tbody>
            {rows_ev_html}
        </tbody>
    </table>
</body>
</html>"""
    return html_doc, "text/html; charset=utf-8", f"sismowatch_report_{now_slug}.html"


def fetch_pipeline_status(is_admin: bool = False) -> PipelineStatusResponse:
    """
    Obtiene el estado del pipeline (Sección 12):
    - `viewer`: ve el resumen por DAG, frescura y puntaje de calidad.
    - `admin`: recibe además `admin_detail` con el historial de corridas, desglose de reglas de calidad
      y auditoría de ejecuciones de IA (`curated.ai_runs`).
    """
    s = get_settings()
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            # Última ejecución de cada uno de los 4 DAGs
            cur.execute(
                """
                SELECT DISTINCT ON (dag_id)
                    dag_id, run_id, estado, inicio, fin, resultado, data_version
                FROM curated.pipeline_runs
                ORDER BY dag_id, COALESCE(fin, inicio) DESC;
                """
            )
            latest_by_dag = []
            latest_ts: Optional[datetime] = None
            for r in cur.fetchall():
                ts_val = r[4] or r[3]
                if ts_val and (latest_ts is None or ts_val > latest_ts):
                    latest_ts = ts_val
                latest_by_dag.append(
                    {
                        "dag_id": str(r[0]),
                        "run_id": str(r[1]),
                        "estado": str(r[2]),
                        "inicio": r[3].isoformat() if r[3] else None,
                        "fin": r[4].isoformat() if r[4] else None,
                        "resultado": str(r[5] or ""),
                        "data_version": r[6],
                    }
                )

            cur.execute(
                """
                SELECT run_id, reglas_evaluadas, aprobadas, rechazadas, puntaje,
                       conteo_por_regla, frescura_ok, frescura_minutos, volumen_ok, volumen_total, timestamp
                FROM curated.quality_results
                ORDER BY timestamp DESC
                LIMIT 1;
                """
            )
            q_row = cur.fetchone()
            quality_score = round(float(q_row[4]) * 100.0, 1) if q_row else 100.0

            freshness_min = 0.0
            if latest_ts is not None:
                lt_utc = (
                    latest_ts.astimezone(timezone.utc)
                    if latest_ts.tzinfo
                    else latest_ts.replace(tzinfo=timezone.utc)
                )
                freshness_min = round(
                    max(0.0, (datetime.now(timezone.utc) - lt_utc).total_seconds() / 60.0),
                    1,
                )

            freshness_ok = freshness_min <= s.freshness_max_minutes
            failed_dags = [d for d in latest_by_dag if d["estado"] == "FAILED"]
            overall = "DEGRADED" if (failed_dags or not freshness_ok) else "HEALTHY"
            dv = get_current_data_version(conn)

            admin_detail: Optional[Dict[str, Any]] = None
            if is_admin:
                cur.execute(
                    """
                    SELECT dag_id, run_id, estado, inicio, fin, resultado, data_version
                    FROM curated.pipeline_runs
                    ORDER BY COALESCE(fin, inicio) DESC
                    LIMIT 25;
                    """
                )
                recent_runs = [
                    {
                        "dag_id": str(r[0]),
                        "run_id": str(r[1]),
                        "estado": str(r[2]),
                        "inicio": r[3].isoformat() if r[3] else None,
                        "fin": r[4].isoformat() if r[4] else None,
                        "resultado": str(r[5] or ""),
                        "data_version": r[6],
                    }
                    for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    SELECT tarea, modelo, proveedor, tokens_entrada, tokens_salida,
                           latencia_ms, costo_estimado, exito, fallback, created_at
                    FROM curated.ai_runs
                    ORDER BY created_at DESC
                    LIMIT 20;
                    """
                )
                ai_runs = [
                    {
                        "tarea": str(r[0]),
                        "modelo": str(r[1]),
                        "proveedor": str(r[2]),
                        "tokens_entrada": int(r[3]),
                        "tokens_salida": int(r[4]),
                        "latencia_ms": float(r[5]),
                        "costo_estimado": float(r[6]),
                        "exito": bool(r[7]),
                        "fallback": bool(r[8]),
                        "created_at": r[9].isoformat() if r[9] else None,
                    }
                    for r in cur.fetchall()
                ]

                admin_detail = {
                    "recent_pipeline_runs": recent_runs,
                    "recent_ai_runs": ai_runs,
                    "latest_quality_gate": {
                        "run_id": str(q_row[0]) if q_row else "none",
                        "evaluated": int(q_row[1]) if q_row else 0,
                        "approved": int(q_row[2]) if q_row else 0,
                        "rejected": int(q_row[3]) if q_row else 0,
                        "rule_counts": q_row[5] if q_row else {},
                        "freshness_ok": bool(q_row[6]) if q_row else True,
                        "volume_ok": bool(q_row[8]) if q_row else True,
                        "volume_total": int(q_row[9]) if q_row else 0,
                    },
                }

            return PipelineStatusResponse(
                overall_status=overall,
                freshness_minutes=freshness_min,
                freshness_ok=freshness_ok,
                quality_score=quality_score,
                data_version=dv,
                dags_summary=latest_by_dag,
                admin_detail=admin_detail,
            )
    finally:
        conn.close()


def fetch_quarantine_rejections(limit: int = 25) -> QuarantineResponse:
    """
    Devuelve el registro de auditoría liviano de rechazos por calidad desde `curated.quality_results`.
    No existe tabla de cuarentena duplicando el dato sucio: el JSON crudo sigue en Bronce y aquí
    solo se expone `{id, regla, motivo}` para el rol `admin`.
    """
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    run_id, timestamp, reglas_evaluadas, aprobadas, rechazadas,
                    puntaje, conteo_por_regla, rechazos_log, frescura_ok,
                    frescura_minutos, volumen_ok, volumen_total
                FROM curated.quality_results
                ORDER BY timestamp DESC
                LIMIT %s;
                """,
                (limit,),
            )
            batches: List[QuarantineBatchItem] = []
            total_rejected = 0
            for r in cur.fetchall():
                rej_count = int(r[4])
                total_rejected += rej_count
                rule_c = r[6] if isinstance(r[6], dict) else json.loads(r[6] or "{}")
                rej_log = r[7] if isinstance(r[7], list) else json.loads(r[7] or "[]")
                batches.append(
                    QuarantineBatchItem(
                        run_id=str(r[0]),
                        timestamp=r[1],
                        reglas_evaluadas=int(r[2]),
                        aprobadas=int(r[3]),
                        rechazadas=rej_count,
                        puntaje=round(float(r[5]) * 100.0, 2),
                        conteo_por_regla=rule_c,
                        rechazos_log=rej_log,
                        frescura_ok=bool(r[8]),
                        frescura_minutos=float(r[9] or 0.0),
                        volumen_ok=bool(r[10]),
                        volumen_total=int(r[11] or 0),
                    )
                )
            return QuarantineResponse(batches=batches, total_rejected_rows=total_rejected)
    finally:
        conn.close()


def trigger_airflow_backfill(days: int = 30, minmagnitude: float = 2.5) -> AdminActionResponse:
    """Invoca la API REST de Airflow para disparar el DAG `usgs_backfill`."""
    s = get_settings()
    url = f"{s.airflow_api_url}/dags/usgs_backfill/dagRuns"
    auth = (s.airflow_api_user, s.airflow_api_password)
    try:
        # Asegurar que el DAG esté despausado
        requests.patch(
            f"{s.airflow_api_url}/dags/usgs_backfill",
            auth=auth,
            json={"is_paused": False},
            timeout=5,
        )
        resp = requests.post(
            url,
            auth=auth,
            json={"conf": {"days": days, "minmagnitude": minmagnitude}},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            data = resp.json()
            return AdminActionResponse(
                status="TRIGGERED",
                message=f"DAG 'usgs_backfill' disparado exitosamente para {days} días (mag >= {minmagnitude}).",
                details={
                    "dag_run_id": data.get("dag_run_id"),
                    "state": data.get("state"),
                    "days": days,
                    "minmagnitude": minmagnitude,
                },
            )
        return AdminActionResponse(
            status="QUEUED_FALLBACK",
            message=f"Solicitud enviada a Airflow REST API (HTTP {resp.status_code}).",
            details={"http_status": resp.status_code},
        )
    except requests.RequestException as exc:
        return AdminActionResponse(
            status="AIRFLOW_UNREACHABLE",
            message=f"No se pudo contactar la API REST de Airflow en este instante: {exc}",
            details={"days": days, "minmagnitude": minmagnitude},
        )
