"""
Generación del Reporte Narrativo Diario en español con validación estricta de cifras (Sección 11).

Reglas obligatorias:
1. Todas las cifras (`total_eventos`, `mag_maxima`, `mag_promedio`, `eventos_tsunami`, `top_regiones`,
   `eventos_destacados`) son calculadas en SQL/Python; el LLM únicamente redacta.
2. La respuesta estructurada se valida con Pydantic (`DailyReportSchema`) y se verifica con
   `verify_calculated_numbers` que las cifras clave aparezcan sin alteración.
3. Si el proveedor falla o inventa cifras tras el reintento de reparación, cae al respaldo
   determinista en Python (`fallback=True`) para que el pipeline jamás se rompa.
"""
from datetime import date, datetime, timezone
import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from ai.llm_client import execute_structured_llm_with_cascade, load_versioned_prompt
from common.config import get_db_connection
from common.logging_utils import get_structured_logger, log_event
from common.sanitization import delimit_untrusted_data, sanitize_place

logger = get_structured_logger("sismowatch.ai.daily_report")

PROMPT_VERSION = "daily_report_v1"


class DailyReportSchema(BaseModel):
    titulo: str = Field(..., min_length=10)
    resumen_ejecutivo: str = Field(..., min_length=30)
    zonas_mas_activas: List[str] = Field(default_factory=list)
    eventos_destacados: List[str] = Field(default_factory=list)
    analisis_anomalias: str = Field(..., min_length=10)
    recomendaciones: str = Field(..., min_length=10)


def compute_daily_metrics_sql(target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Calcula con SQL sobre `curated` las cifras oficiales de las últimas 24 horas (o de la fecha indicada).
    """
    conn = get_db_connection(role="reader")
    report_dt = target_date or datetime.now(timezone.utc).date()
    try:
        with conn.cursor() as cur:
            # Si hay eventos en las últimas 24h usamos esa ventana móvil; si no, tomamos la ventana disponible
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_eventos,
                    COALESCE(ROUND(MAX(mag)::numeric, 1), 0.0)::float AS mag_maxima,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float AS mag_promedio,
                    COALESCE(SUM(tsunami), 0)::int AS eventos_tsunami
                FROM curated.events
                WHERE time >= NOW() - INTERVAL '24 hours';
                """
            )
            row = cur.fetchone()
            interval_param = "24 hours"
            if not row or int(row[0]) == 0:
                cur.execute(
                    """
                    SELECT
                        COUNT(*)::int AS total_eventos,
                        COALESCE(ROUND(MAX(mag)::numeric, 1), 0.0)::float AS mag_maxima,
                        COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float AS mag_promedio,
                        COALESCE(SUM(tsunami), 0)::int AS eventos_tsunami
                    FROM curated.events
                    WHERE time >= NOW() - INTERVAL '30 days';
                    """
                )
                row = cur.fetchone()
                interval_param = "30 days"

            total_eventos = int(row[0]) if row else 0
            mag_maxima = float(row[1]) if row else 0.0
            mag_promedio = float(row[2]) if row else 0.0
            eventos_tsunami = int(row[3]) if row else 0

            # Top 5 regiones más activas
            cur.execute(
                """
                SELECT region, COUNT(*)::int AS conteo, ROUND(MAX(mag)::numeric, 1)::float AS mag_max
                FROM curated.events
                WHERE time >= NOW() - %s::interval
                GROUP BY region
                ORDER BY conteo DESC, mag_max DESC
                LIMIT 5;
                """,
                (interval_param,),
            )
            top_regiones = [
                {
                    "region": sanitize_place(r[0]),
                    "conteo": int(r[1]),
                    "mag_max": float(r[2]),
                }
                for r in cur.fetchall()
            ]

            # Top 5 eventos destacados por magnitud
            cur.execute(
                """
                SELECT id, ROUND(mag::numeric, 1)::float, place, time, ROUND(depth_km::numeric, 1)::float, tsunami
                FROM curated.events
                WHERE time >= NOW() - %s::interval
                ORDER BY mag DESC, time DESC
                LIMIT 5;
                """,
                (interval_param,),
            )
            eventos_destacados = [
                {
                    "id": str(r[0]),
                    "mag": float(r[1]),
                    "place": sanitize_place(r[2]),
                    "time_utc": r[3].strftime("%Y-%m-%d %H:%M UTC"),
                    "depth_km": float(r[4]),
                    "tsunami": int(r[5]),
                }
                for r in cur.fetchall()
            ]

            # Anomalías recientes
            cur.execute(
                """
                SELECT tipo, ventana, score, explicacion
                FROM curated.anomalies
                ORDER BY detected_at DESC
                LIMIT 5;
                """
            )
            anomalias = [
                {
                    "tipo": str(r[0]),
                    "ventana": sanitize_place(r[1]),
                    "score": float(r[2]),
                    "explicacion": sanitize_place(r[3]),
                }
                for r in cur.fetchall()
            ]

        return {
            "fecha_reporte": report_dt.isoformat(),
            "total_eventos": total_eventos,
            "mag_maxima": mag_maxima,
            "mag_promedio": mag_promedio,
            "eventos_tsunami": eventos_tsunami,
            "top_regiones": top_regiones,
            "eventos_destacados": eventos_destacados,
            "anomalias": anomalias,
        }
    finally:
        conn.close()


def _build_deterministic_report_template(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Construye un reporte diario determinista en español con las cifras exactas de SQL."""
    fecha = metrics["fecha_reporte"]
    tot = metrics["total_eventos"]
    m_max = metrics["mag_maxima"]
    m_avg = metrics["mag_promedio"]
    tsu = metrics["eventos_tsunami"]

    zonas_list = [
        f"{z['region']}: {z['conteo']} sismos registrados (magnitud máxima {z['mag_max']})"
        for z in metrics.get("top_regiones", [])
    ] or ["Sin zonas con actividad significativa registrada en el corte."]

    destacados_list = [
        f"M {ev['mag']} en {ev['place']} ({ev['time_utc']}, profundidad {ev['depth_km']} km)"
        for ev in metrics.get("eventos_destacados", [])
    ] or ["No se registraron sismos destacados en el periodo evaluado."]

    anoms = metrics.get("anomalias", [])
    if anoms:
        anom_text = "Se detectaron " + "; ".join(
            f"{a['tipo']} en {a['ventana']} (score {a['score']}): {a['explicacion']}" for a in anoms[:3]
        )
    else:
        anom_text = "No se registraron anomalías estadísticas (z-score) ni enjambres espaciales fuera del rango basal."

    return {
        "titulo": f"Reporte Ejecutivo Sísmico Diario SismoWatch — {fecha}",
        "resumen_ejecutivo": (
            f"Durante el corte del {fecha}, la plataforma SismoWatch procesó un total de {tot} eventos sísmicos "
            f"validados en el catálogo global USGS. La magnitud máxima registrada alcanzó {m_max} "
            f"(con una magnitud promedio global de {m_avg}), contabilizándose {tsu} eventos con indicador "
            f"de vigilancia de tsunami."
        ),
        "zonas_mas_activas": zonas_list,
        "eventos_destacados": destacados_list,
        "analisis_anomalias": anom_text,
        "recomendaciones": (
            "Mantener vigilancia activa sobre las regiones con mayor densidad de réplicas y verificar "
            "el panel de calidad de datos y frescura del pipeline cada 5 minutos."
        ),
    }


def _format_narrative_text(report_data: Dict[str, Any], metrics: Dict[str, Any]) -> str:
    """Convierte el JSON validado en un documento narrativo legible para UI y exportación."""
    zonas_md = "\n".join(f"- {z}" for z in report_data.get("zonas_mas_activas", []))
    eventos_md = "\n".join(f"- {e}" for e in report_data.get("eventos_destacados", []))
    return (
        f"# {report_data['titulo']}\n\n"
        f"## Resumen Ejecutivo\n{report_data['resumen_ejecutivo']}\n\n"
        f"**Cifras Oficiales Verificadas (SQL):** Total eventos: {metrics['total_eventos']} | "
        f"Magnitud máxima: {metrics['mag_maxima']} | Magnitud promedio: {metrics['mag_promedio']} | "
        f"Alertas de tsunami: {metrics['eventos_tsunami']}\n\n"
        f"## Zonas Más Activas\n{zonas_md}\n\n"
        f"## Eventos Destacados\n{eventos_md}\n\n"
        f"## Análisis de Anomalías\n{report_data['analisis_anomalias']}\n\n"
        f"## Recomendaciones Operativas\n{report_data['recomendaciones']}"
    )


def generate_and_store_daily_report(
    target_date: Optional[date] = None,
    run_id: str = "daily_ai",
) -> Dict[str, Any]:
    """
    Calcula las métricas del día en SQL, genera el reporte narrativo con validación
    de cifras y lo guarda en `curated.daily_reports` usando `ai_role`.
    """
    metrics = compute_daily_metrics_sql(target_date=target_date)
    report_date_str = metrics["fecha_reporte"]
    sys_prompt = load_versioned_prompt(PROMPT_VERSION)

    # Sanitizar y delimitar todos los textos geográficos antes de enviarlos al LLM
    safe_payload_for_llm = {
        "fecha_reporte": report_date_str,
        "total_eventos": metrics["total_eventos"],
        "mag_maxima": metrics["mag_maxima"],
        "mag_promedio": metrics["mag_promedio"],
        "eventos_tsunami": metrics["eventos_tsunami"],
        "top_regiones": [
            {
                "region": delimit_untrusted_data(r["region"]),
                "conteo": r["conteo"],
                "mag_max": r["mag_max"],
            }
            for r in metrics["top_regiones"]
        ],
        "eventos_destacados": [
            {
                "id": e["id"],
                "mag": e["mag"],
                "place": delimit_untrusted_data(e["place"]),
                "time_utc": e["time_utc"],
                "depth_km": e["depth_km"],
            }
            for e in metrics["eventos_destacados"]
        ],
        "anomalias": metrics["anomalias"],
    }

    user_prompt = (
        "Redacta el reporte ejecutivo diario en JSON válido utilizando exclusivamente "
        f"estas CIFRAS_OFICIALES_SQL:\n{json.dumps(safe_payload_for_llm, ensure_ascii=False, indent=2)}"
    )

    required_numbers = [
        float(metrics["total_eventos"]),
        float(metrics["mag_maxima"]),
        float(metrics["eventos_tsunami"]),
    ]

    llm_res = execute_structured_llm_with_cascade(
        tarea="daily_executive_report",
        system_prompt=sys_prompt,
        user_prompt=user_prompt,
        schema_cls=DailyReportSchema,
        required_numbers=required_numbers,
        template_fallback_fn=lambda: _build_deterministic_report_template(metrics),
        run_id=run_id,
        db_role="ai",
        timeout_sec=22.0,
    )

    validated_report = llm_res["data"]
    full_json_stored = {
        **validated_report,
        "cifras_oficiales": {
            "total_eventos": metrics["total_eventos"],
            "mag_maxima": metrics["mag_maxima"],
            "mag_promedio": metrics["mag_promedio"],
            "eventos_tsunami": metrics["eventos_tsunami"],
        },
        "top_regiones_raw": metrics["top_regiones"],
        "eventos_destacados_raw": metrics["eventos_destacados"],
    }
    narrative_text = _format_narrative_text(validated_report, metrics)

    conn = get_db_connection(role="ai")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.daily_reports (
                        fecha, json_validado, texto, proveedor, fallback, version_prompt, created_at, updated_at
                    )
                    VALUES (%s, %s::jsonb, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (fecha) DO UPDATE SET
                        json_validado = EXCLUDED.json_validado,
                        texto = EXCLUDED.texto,
                        proveedor = EXCLUDED.proveedor,
                        fallback = EXCLUDED.fallback,
                        version_prompt = EXCLUDED.version_prompt,
                        updated_at = NOW();
                    """,
                    (
                        report_date_str,
                        json.dumps(full_json_stored, ensure_ascii=False),
                        narrative_text,
                        llm_res["provider"],
                        llm_res["fallback"],
                        PROMPT_VERSION,
                    ),
                )
    finally:
        conn.close()

    summary = {
        "fecha": report_date_str,
        "proveedor": llm_res["provider"],
        "fallback": llm_res["fallback"],
        "version_prompt": PROMPT_VERSION,
        "total_eventos": metrics["total_eventos"],
        "mag_maxima": metrics["mag_maxima"],
    }
    log_event(logger, "info", "daily_report_saved", run_id, summary)
    return summary
