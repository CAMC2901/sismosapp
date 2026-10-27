"""
Catálogo de Herramientas (Skills) y Motor de Consultas a Base de Datos para el Agente SismoWatch:
Permite consultar y analizar completamente la base de datos PostgreSQL (`curated`) con filtros
dinámicos de ventana temporal (15m, 1h, 6h, 12h, 24h/hoy, 48h/ayer, 7d, 15d, 30d), región/país,
magnitud y profundidad, comparativas temporales, auditoría de calidad/pipelines (Meta-Consultas)
y un generador Text-to-SQL seguro de solo lectura (`reader_role`) para cualquier consulta.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
import requests
from app.agent.sql_validator import validate_safe_sql
from app.core.config import get_db_conn, get_settings
from app.core.exceptions import UnsafeQueryError

# Vocabulario de dominio sísmico y operativo para validar consultas en RAG
DOMAIN_KEYWORDS = {
    "sismo",
    "sismos",
    "terremoto",
    "terremotos",
    "temblor",
    "temblores",
    "temblado",
    "temblo",
    "sacudon",
    "remezon",
    "meneo",
    "magnitud",
    "grados",
    "richter",
    "profundidad",
    "hipocentro",
    "epicentro",
    "tsunami",
    "maremoto",
    "alerta",
    "alertas",
    "region",
    "regiones",
    "zona",
    "zonas",
    "pais",
    "paises",
    "placa",
    "placas",
    "falla",
    "replica",
    "replicas",
    "enjambre",
    "enjambres",
    "anomalia",
    "anomalias",
    "dbscan",
    "zscore",
    "z-score",
    "inusual",
    "patron",
    "estadistica",
    "estadisticas",
    "metrica",
    "metricas",
    "kpi",
    "kpis",
    "hora",
    "horaria",
    "minuto",
    "minutos",
    "diario",
    "diaria",
    "reporte",
    "reportes",
    "boletin",
    "usgs",
    "sismowatch",
    "superapp",
    "mapa",
    "dashboard",
    "calidad",
    "puntaje",
    "rechazado",
    "rechazados",
    "descartado",
    "descartados",
    "invalido",
    "invalidos",
    "coordenadas",
    "regla",
    "pandera",
    "bronce",
    "plata",
    "oro",
    "medallion",
    "minio",
    "postgres",
    "postgresql",
    "spark",
    "airflow",
    "dag",
    "dags",
    "corrida",
    "fallo",
    "pipeline",
    "backfill",
    "admin",
    "viewer",
    "rol",
    "roles",
    "exportar",
    "csv",
    "pdf",
    "frescura",
    "actualizo",
    "actualizado",
    "latencia",
    "significancia",
    "sig",
    "fuerte",
    "fuertes",
    "mayor",
    "maxima",
    "minima",
    "promedio",
    "media",
    "pico",
    "reciente",
    "recientes",
    "ultimo",
    "ultimos",
    "hoy",
    "ayer",
    "comparado",
    "semana",
    "mes",
    "madrugada",
    "superficial",
    "superficiales",
    "profundo",
    "profundos",
    "intermedio",
    "base de datos",
    "catalogo",
    "sistema",
    "tabla",
    "cuantos",
    "donde",
    "cual",
}

# Mapeo de países, regiones y macro-regiones en español e inglés hacia el catálogo USGS
REGION_ALIASES: Dict[str, str] = {
    "alaska": "Alaska",
    "california": "California",
    "chile": "Chile",
    "japon": "Japan",
    "japan": "Japan",
    "panama": "Panama",
    "mexico": "Mexico",
    "peru": "Peru",
    "colombia": "Colombia",
    "indonesia": "Indonesia",
    "hawai": "Hawaii",
    "hawaii": "Hawaii",
    "puerto rico": "Puerto Rico",
    "filipinas": "Philippines",
    "philippines": "Philippines",
    "taiwan": "Taiwan",
    "rusia": "Russia",
    "russia": "Russia",
    "argentina": "Argentina",
    "ecuador": "Ecuador",
    "costa rica": "Costa Rica",
    "guatemala": "Guatemala",
    "el salvador": "El Salvador",
    "nicaragua": "Nicaragua",
    "honduras": "Honduras",
    "tonga": "Tonga",
    "fiji": "Fiji",
    "nevada": "Nevada",
    "texas": "Texas",
    "oklahoma": "Oklahoma",
    "oregon": "Oregon",
    "washington": "Washington",
    "montana": "Montana",
    "utah": "Utah",
    "wyoming": "Wyoming",
    "nueva zelanda": "New Zealand",
    "new zealand": "New Zealand",
    "papua": "Papua New Guinea",
    "grecia": "Greece",
    "greece": "Greece",
    "turquia": "Turkey",
    "turkey": "Turkey",
    "italia": "Italy",
    "italy": "Italy",
    "china": "China",
    "india": "India",
    "iran": "Iran",
    "bolivia": "Bolivia",
    "venezuela": "Venezuela",
    "canada": "Canada",
    "south america": "South America",
    "sudamerica": "South America",
    "suramerica": "South America",
    "america del sur": "South America",
    "central america": "Central America",
    "centroamerica": "Central America",
    "north america": "North America",
    "norteamerica": "North America",
}

# Sub-regiones para agregaciones continentales/macro-regionales
MACRO_REGION_COUNTRIES: Dict[str, List[str]] = {
    "South America": [
        "Chile",
        "Peru",
        "Argentina",
        "Ecuador",
        "Colombia",
        "Bolivia",
        "Venezuela",
        "Brazil",
        "South America",
    ],
    "Central America": [
        "Panama",
        "Costa Rica",
        "Nicaragua",
        "Honduras",
        "El Salvador",
        "Guatemala",
        "Belize",
        "Central America",
    ],
    "North America": [
        "Alaska",
        "California",
        "Mexico",
        "Nevada",
        "Texas",
        "Oklahoma",
        "Oregon",
        "Washington",
        "Montana",
        "Utah",
        "Wyoming",
        "Canada",
    ],
}


def _deterministic_hash_vector(text: str, dim: int = 1024) -> List[float]:
    cleaned = (text or "empty").strip().lower()
    raw: List[float] = []
    counter = 0
    while len(raw) < dim:
        digest = hashlib.sha512(f"{cleaned}::{counter}".encode("utf-8")).digest()
        for i in range(0, len(digest), 4):
            if len(raw) >= dim:
                break
            val = int.from_bytes(digest[i : i + 4], byteorder="big", signed=True) / float(2**31)
            raw.append(val)
        counter += 1
    norm = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [round(x / norm, 6) for x in raw]


def compute_query_embedding(text: str) -> List[float]:
    """Obtiene el vector de 1024 dimensiones con Ollama bge-m3 o respaldo determinista."""
    s = get_settings()
    try:
        resp = requests.post(
            f"{s.ollama_url}/api/embeddings",
            json={"model": s.ollama_embed_model, "prompt": text},
            timeout=(1.5, 4.0),
        )
        if resp.status_code == 200:
            vec = resp.json().get("embedding")
            if isinstance(vec, list) and len(vec) == 1024:
                return [float(x) for x in vec]
    except requests.RequestException:
        pass
    return _deterministic_hash_vector(text, 1024)


def _vec_literal(vec: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def parse_natural_query_filters(raw_question: str, norm_q: str) -> Dict[str, Any]:
    """
    Extrae automáticamente de la pregunta del usuario:
    - `minutes`: ventana en minutos si se pregunta por minutos específicos (ej. "últimos 15 minutos")
    - `hours`: ventana de tiempo en horas (None = catálogo completo de 30 días, 24 = últimas 24h / hoy, etc.)
    - `window_label`: etiqueta legible de la ventana temporal
    - `region`: país, estado o macro-región geográfica si se menciona
    - `min_mag` / `max_mag`: filtros de magnitud si se mencionan
    - `limit`: cantidad de filas solicitadas
    """
    minutes: Optional[int] = None
    hours: Optional[int] = None
    window_label = "Catálogo Completo (30 días)"

    # 1. Detectar ventana de tiempo explícita (minutos, horas, días)
    m_mins = re.search(r"(?:ultimos?|hace)\s+(\d+)\s*min(?:utos?)?\b", norm_q)
    m_hours = re.search(r"ultimas?\s+(\d+)\s*h(?:oras?)?\b", norm_q)
    m_days = re.search(r"ultimos?\s+(\d+)\s*d(?:ias?)?\b", norm_q)

    if m_mins:
        minutes = max(1, min(int(m_mins.group(1)), 1440))
        window_label = f"Últimos {minutes} minutos"
    elif m_hours:
        hours = max(1, min(int(m_hours.group(1)), 720))
        window_label = f"Últimas {hours} horas"
    elif m_days:
        d = max(1, min(int(m_days.group(1)), 30))
        hours = d * 24
        window_label = f"Últimos {d} días"
    elif any(
        w in norm_q
        for w in (
            "ultima hora",
            "en la ultima hora",
            "hace una hora",
            "1 hora",
            "1h",
        )
    ):
        hours = 1
        window_label = "Última 1 hora"
    elif any(w in norm_q for w in ("ultimas 6 horas", "6 horas", "6h")):
        hours = 6
        window_label = "Últimas 6 horas"
    elif any(w in norm_q for w in ("ultimas 12 horas", "12 horas", "12h", "medio dia")):
        hours = 12
        window_label = "Últimas 12 horas"
    elif any(
        w in norm_q
        for w in (
            "ultimas 24 horas",
            "24 horas",
            "24h",
            "ultimas 24",
            "en el ultimo dia",
            "ultimo dia",
            "hoy",
            "dia de hoy",
            "en este momento",
        )
    ):
        hours = 24
        window_label = "Hoy (Últimas 24 horas)"
    elif any(w in norm_q for w in ("ultimas 48 horas", "48 horas", "48h", "ayer y hoy")):
        hours = 48
        window_label = "Últimas 48 horas"
    elif any(w in norm_q for w in ("ultimas 72 horas", "72 horas", "72h", "tres dias")):
        hours = 72
        window_label = "Últimas 72 horas"
    elif any(w in norm_q for w in ("ultima semana", "esta semana", "7 dias", "semanal")):
        hours = 168
        window_label = "Esta Semana (Últimos 7 días)"
    elif any(w in norm_q for w in ("quincena", "15 dias")):
        hours = 360
        window_label = "Últimos 15 días"
    elif any(w in norm_q for w in ("ultimo mes", "este mes", "30 dias", "mensual")):
        hours = 720
        window_label = "Último Mes (30 días)"

    # 2. Detectar región, país o macro-región (ordenando por longitud descendente para frases compuestas como "south america")
    region: Optional[str] = None
    for alias in sorted(REGION_ALIASES.keys(), key=len, reverse=True):
        if re.search(rf"\b{re.escape(alias)}\b", norm_q):
            region = REGION_ALIASES[alias]
            break

    if not region:
        reg_match = re.search(
            r"\b(?:cerca de|en la region de|en el pais de|en la zona de|en)\s+([a-z]{3,20})\b",
            norm_q,
        )
        if reg_match:
            candidate = reg_match.group(1).strip()
            ignore_words = {
                "las",
                "los",
                "el",
                "la",
                "que",
                "cual",
                "cuales",
                "donde",
                "como",
                "cuanto",
                "cuantos",
                "vivo",
                "tiempo",
                "total",
                "general",
                "mundo",
                "tierra",
                "superficie",
                "catalogo",
                "base",
                "datos",
                "sismowatch",
                "sistema",
                "hora",
                "horas",
                "minuto",
                "minutos",
                "dia",
                "dias",
                "semana",
                "mes",
                "vista",
                "este",
                "esta",
                "ultima",
                "ultimo",
                "ultimas",
                "ultimos",
            }
            if candidate not in ignore_words:
                region = REGION_ALIASES.get(candidate, candidate.title())

    # 3. Detectar magnitud mínima / máxima (usando raw_question en minúsculas para conservar puntos decimales como 5.5 o 6.0)
    raw_low = unicodedata.normalize("NFKD", (raw_question or "").lower())
    raw_low = "".join(c for c in raw_low if not unicodedata.combining(c))
    min_mag: Optional[float] = None
    max_mag: Optional[float] = None

    m_between = re.search(r"entre\s+(\d+(?:\.\d+)?)\s+y\s+(\d+(?:\.\d+)?)", raw_low)
    if m_between:
        min_mag = float(m_between.group(1))
        max_mag = float(m_between.group(2))
    else:
        m_min = re.search(
            r"(?:mayor(?:es)?\s+(?:a|de|que)|mas\s+de|superior(?:es)?\s+a|superaron(?:\s+los)?|superen(?:\s+los)?|sobre(?:\s+los)?|desde|minimo|magnitud\s+mayor\s+a|m\s*>=?)\s*(\d+(?:\.\d+)?)",
            raw_low,
        )
        m_max = re.search(
            r"(?:menor(?:es)?\s+(?:a|de|que)(?:\s+magnitud)?|menos\s+de|inferior(?:es)?\s+a|bajo(?:\s+los)?|hasta|maximo|m\s*<=?)\s*(\d+(?:\.\d+)?)",
            raw_low,
        )
        if m_min:
            min_mag = float(m_min.group(1))
        elif "sismos fuertes" in norm_q or "terremotos fuertes" in norm_q:
            min_mag = 5.0
        if m_max:
            max_mag = float(m_max.group(1))
        elif "microsismo" in norm_q or "microsismos" in norm_q:
            max_mag = 3.0

    # 4. Detectar cantidad límite (ej. "últimos 5 sismos", "top 5", "las 3 regiones", "último sismo")
    limit = 8
    if re.search(r"\b(?:el\s+ultimo\s+(?:sismo|temblor|terremoto|evento)|sismo\s+mas\s+reciente|temblor\s+mas\s+reciente)\b", norm_q):
        limit = 1
    else:
        m_lim = re.search(
            r"\b(?:top|ultimos?|ultimas?|primeros?|mostrar|muestrame|dame|lista\s+de|las|los)\s+(\d{1,2})\s*(?:sismos?|temblores?|terremotos?|eventos?|regiones?|zonas?|paises?)?\b",
            norm_q,
        )
        if m_lim:
            val = int(m_lim.group(1))
            if val not in (12, 15, 24, 30, 48, 72) or any(
                w in norm_q for w in (f"ultimos {val} sismos", f"top {val}", f"las {val} regiones", f"los {val} sismos")
            ):
                limit = max(1, min(val, 25))

    return {
        "minutes": minutes,
        "hours": hours,
        "window_label": window_label,
        "region": region,
        "min_mag": min_mag,
        "max_mag": max_mag,
        "limit": limit,
    }


def _build_sql_where(
    hours: Optional[int] = None,
    region: Optional[str] = None,
    min_mag: Optional[float] = None,
    max_mag: Optional[float] = None,
    only_tsunami: bool = False,
    minutes: Optional[int] = None,
) -> Tuple[str, List[Any]]:
    """
    Construye una cláusula WHERE parametrizada sobre `curated.events` anclada al timestamp
    más reciente del catálogo para que las ventanas de minutos/horas/días sean exactas.
    Soporta además macro-regiones como 'South America'.
    """
    clauses: List[str] = ["1=1"]
    params: List[Any] = []

    if minutes is not None:
        clauses.append(
            "time >= (SELECT COALESCE(MAX(time), NOW()) FROM curated.events) - (%s || ' minutes')::interval"
        )
        params.append(int(minutes))
    elif hours is not None:
        clauses.append(
            "time >= (SELECT COALESCE(MAX(time), NOW()) FROM curated.events) - (%s || ' hours')::interval"
        )
        params.append(int(hours))

    if region:
        macro_list = MACRO_REGION_COUNTRIES.get(region)
        if macro_list:
            or_parts = []
            for sub_reg in macro_list:
                or_parts.append("region ILIKE %s OR place ILIKE %s")
                params.extend([f"%{sub_reg}%", f"%{sub_reg}%"])
            clauses.append("(" + " OR ".join(or_parts) + ")")
        else:
            clauses.append("(region ILIKE %s OR place ILIKE %s)")
            params.extend([f"%{region}%", f"%{region}%"])

    if min_mag is not None:
        clauses.append("mag >= %s")
        params.append(float(min_mag))

    if max_mag is not None:
        clauses.append("mag <= %s")
        params.append(float(max_mag))

    if only_tsunami:
        clauses.append("tsunami = 1")

    return " AND ".join(clauses), params


def ultimo_sismo_detallado() -> Dict[str, Any]:
    """
    Obtiene el último sismo registrado en el sistema (`ORDER BY time DESC LIMIT 1`),
    calculando exactamente hace cuántos minutos ocurrió respecto a `NOW()` y a la última ingesta,
    junto con los 5 eventos más recientes de referencia.
    """
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    ROUND(mag::numeric, 2)::float,
                    place,
                    region,
                    time,
                    ROUND(depth_km::numeric, 1)::float,
                    ROUND(lat::numeric, 3)::float,
                    ROUND(lon::numeric, 3)::float,
                    tsunami,
                    sig,
                    GREATEST(0, ROUND((EXTRACT(EPOCH FROM (NOW() - time)) / 60.0)::numeric, 1))::float AS minutos_atras
                FROM curated.events
                ORDER BY time DESC, mag DESC
                LIMIT 5;
                """
            )
            rows = cur.fetchall()
            if not rows:
                return {"skill": "ultimo_sismo_detallado", "encontrado": False}

            r0 = rows[0]
            recientes = [
                {
                    "id": str(r[0]),
                    "magnitud": float(r[1]),
                    "lugar": str(r[2]),
                    "region": str(r[3]),
                    "fecha_utc": r[4].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "hora_exacta_utc": r[4].strftime("%H:%M:%S UTC"),
                    "profundidad_km": float(r[5]),
                    "lat": float(r[6]),
                    "lon": float(r[7]),
                    "tsunami": int(r[8]),
                    "sig": int(r[9]),
                    "hace_minutos": float(r[10]),
                }
                for r in rows
            ]

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE time >= NOW() - INTERVAL '15 minutes')::int,
                    COUNT(*) FILTER (WHERE time >= (SELECT MAX(time) FROM curated.events) - INTERVAL '15 minutes')::int,
                    COUNT(*) FILTER (WHERE time >= (SELECT MAX(time) FROM curated.events) - INTERVAL '1 hour')::int
                FROM curated.events;
                """
            )
            c_15m_real, c_15m_cat, c_1h_cat = cur.fetchone()

        return {
            "skill": "ultimo_sismo_detallado",
            "encontrado": True,
            "ultimo_evento": recientes[0],
            "hace_minutos": recientes[0]["hace_minutos"],
            "sismos_ultimos_15_min": int(max(c_15m_real, c_15m_cat)),
            "sismos_ultima_hora": int(c_1h_cat),
            "ultimos_5_eventos": recientes,
        }
    except Exception:
        return {"skill": "ultimo_sismo_detallado", "error": "Error al consultar el último sismo registrado."}
    finally:
        conn.close()


def ultimos_sismos(
    hours: Optional[int] = None,
    region: Optional[str] = None,
    min_mag: Optional[float] = None,
    max_mag: Optional[float] = None,
    limit: int = 10,
    window_label: str = "Recientes",
    minutes: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Consulta los últimos sismos registrados en la base de datos en orden cronológico inverso (`ORDER BY time DESC`),
    junto con el resumen estadístico de los eventos en esa ventana/región.
    """
    lim = max(1, min(int(limit), 25))
    where_sql, params = _build_sql_where(
        hours=hours,
        region=region,
        min_mag=min_mag,
        max_mag=max_mag,
        minutes=minutes,
    )
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(depth_km)::numeric, 1), 0.0)::float,
                    COALESCE(SUM(tsunami), 0)::int
                FROM curated.events
                WHERE {where_sql};
                """,
                params,
            )
            tot, m_max, m_avg, d_avg, tsu = cur.fetchone()

            cur.execute(
                f"""
                SELECT
                    id,
                    ROUND(mag::numeric, 2)::float,
                    place,
                    region,
                    time,
                    ROUND(depth_km::numeric, 1)::float,
                    tsunami,
                    sig,
                    GREATEST(0, ROUND((EXTRACT(EPOCH FROM (NOW() - time)) / 60.0)::numeric, 1))::float AS hace_min
                FROM curated.events
                WHERE {where_sql}
                ORDER BY time DESC, mag DESC
                LIMIT %s;
                """,
                [*params, lim],
            )
            eventos = [
                {
                    "id": str(r[0]),
                    "magnitud": float(r[1]),
                    "lugar": str(r[2]),
                    "region": str(r[3]),
                    "fecha_utc": r[4].strftime("%Y-%m-%d %H:%M UTC"),
                    "profundidad_km": float(r[5]),
                    "tsunami": int(r[6]),
                    "sig": int(r[7]),
                    "hace_minutos": float(r[8]),
                }
                for r in cur.fetchall()
            ]

        return {
            "skill": "ultimos_sismos",
            "ventana": window_label,
            "region_filtro": region or "Global",
            "total_en_ventana": int(tot),
            "mostrados": len(eventos),
            "magnitud_maxima": float(m_max),
            "magnitud_promedio": float(m_avg),
            "profundidad_promedio_km": float(d_avg),
            "alertas_tsunami": int(tsu),
            "eventos": eventos,
        }
    except Exception:
        return {"skill": "ultimos_sismos", "error": "Error al consultar los últimos sismos."}
    finally:
        conn.close()


def kpis(
    rango: str = "30d",
    hours: Optional[int] = None,
    region: Optional[str] = None,
    window_label: str = "Catálogo Activo",
    min_mag: Optional[float] = None,
    max_mag: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Skill 1: Calcula las métricas y KPIs sísmicos sobre PostgreSQL (`curated.events`),
    soportando tanto el catálogo global (30d + 24h) como filtros por ventana temporal, magnitud o región.
    """
    conn = get_db_conn(role="reader")
    where_sql, params = _build_sql_where(hours=hours, region=region, min_mag=min_mag, max_mag=max_mag)
    where_24h, params_24h = _build_sql_where(hours=24, region=region, min_mag=min_mag, max_mag=max_mag)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(MIN(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(depth_km)::numeric, 1), 0.0)::float,
                    COALESCE(ROUND(MAX(depth_km)::numeric, 1), 0.0)::float,
                    COALESCE(SUM(CASE WHEN mag >= 5.0 THEN 1 ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN mag < 3.0 THEN 1 ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN depth_km < 70.0 THEN 1 ELSE 0 END), 0)::int,
                    COALESCE(SUM(tsunami), 0)::int
                FROM curated.events
                WHERE {where_sql};
                """,
                params,
            )
            (
                tot_cat,
                m_max_cat,
                m_avg_cat,
                m_min_cat,
                d_avg_cat,
                d_max_cat,
                strong_cat,
                micro_cat,
                shallow_cat,
                tsu_cat,
            ) = cur.fetchone()

            cur.execute(
                f"""
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(SUM(CASE WHEN mag < 3.0 THEN 1 ELSE 0 END), 0)::int,
                    COALESCE(SUM(tsunami), 0)::int
                FROM curated.events
                WHERE {where_24h};
                """,
                params_24h,
            )
            ev_24h, m_max_24h, m_avg_24h, micro_24h, tsu_24h = cur.fetchone()

            cur.execute(
                "SELECT COALESCE(ROUND((puntaje * 100)::numeric, 1), 100.0)::float FROM curated.quality_results ORDER BY timestamp DESC LIMIT 1;"
            )
            q_row = cur.fetchone()
            q_score = float(q_row[0]) if q_row else 100.0

        return {
            "skill": "kpis",
            "rango": window_label if hours else rango,
            "region_filtro": region or "Global",
            "min_mag_filtro": min_mag,
            "max_mag_filtro": max_mag,
            "total_eventos": int(tot_cat),
            "total_catalogo_30d": int(tot_cat),
            "eventos_24h": int(ev_24h),
            "eventos_ultimas_24h": int(ev_24h),
            "magnitud_maxima": float(m_max_cat),
            "magnitud_promedio": float(m_avg_cat),
            "magnitud_minima": float(m_min_cat),
            "profundidad_promedio_km": float(d_avg_cat),
            "profundidad_maxima_km": float(d_max_cat),
            "sismos_fuertes_m5": int(strong_cat),
            "sismos_menores_m3": int(micro_cat),
            "sismos_menores_m3_24h": int(micro_24h),
            "sismos_superficiales": int(shallow_cat),
            "alertas_tsunami": int(tsu_cat),
            "magnitud_maxima_24h": float(m_max_24h),
            "magnitud_promedio_24h": float(m_avg_24h),
            "alertas_tsunami_24h": int(tsu_24h),
            "puntaje_calidad_pct": q_score,
        }
    except Exception:
        return {"skill": "kpis", "error": "No fue posible consultar las métricas en este momento."}
    finally:
        conn.close()


def comparar_hoy_vs_ayer(region: Optional[str] = None) -> Dict[str, Any]:
    """
    Compara la actividad sísmica de las últimas 24 horas ('Hoy') frente a las 24 horas previas ('Ayer', 24h-48h),
    calculando diferencia absoluta, variación porcentual y magnitudes máximas.
    """
    conn = get_db_conn(role="reader")
    where_reg, params_reg = _build_sql_where(region=region)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH max_ts AS (
                    SELECT COALESCE(MAX(time), NOW()) AS ref_time FROM curated.events
                )
                SELECT
                    COUNT(*) FILTER (WHERE time >= (SELECT ref_time FROM max_ts) - INTERVAL '24 hours')::int AS hoy_cnt,
                    COALESCE(ROUND((MAX(mag) FILTER (WHERE time >= (SELECT ref_time FROM max_ts) - INTERVAL '24 hours'))::numeric, 2), 0.0)::float AS hoy_max,
                    COALESCE(ROUND((AVG(mag) FILTER (WHERE time >= (SELECT ref_time FROM max_ts) - INTERVAL '24 hours'))::numeric, 2), 0.0)::float AS hoy_avg,
                    COUNT(*) FILTER (
                        WHERE time < (SELECT ref_time FROM max_ts) - INTERVAL '24 hours'
                          AND time >= (SELECT ref_time FROM max_ts) - INTERVAL '48 hours'
                    )::int AS ayer_cnt,
                    COALESCE(ROUND((MAX(mag) FILTER (
                        WHERE time < (SELECT ref_time FROM max_ts) - INTERVAL '24 hours'
                          AND time >= (SELECT ref_time FROM max_ts) - INTERVAL '48 hours'
                    ))::numeric, 2), 0.0)::float AS ayer_max,
                    COALESCE(ROUND((AVG(mag) FILTER (
                        WHERE time < (SELECT ref_time FROM max_ts) - INTERVAL '24 hours'
                          AND time >= (SELECT ref_time FROM max_ts) - INTERVAL '48 hours'
                    ))::numeric, 2), 0.0)::float AS ayer_avg
                FROM curated.events
                WHERE {where_reg};
                """,
                params_reg,
            )
            hoy_cnt, hoy_max, hoy_avg, ayer_cnt, ayer_max, ayer_avg = cur.fetchone()
            diff = int(hoy_cnt) - int(ayer_cnt)
            pct = round((diff * 100.0) / (int(ayer_cnt) or 1), 1)
            tendencia = "Más actividad hoy" if diff > 0 else ("Menos actividad hoy" if diff < 0 else "Actividad idéntica")

        return {
            "skill": "comparar_hoy_vs_ayer",
            "region_filtro": region or "Global",
            "hoy_sismos": int(hoy_cnt),
            "hoy_mag_max": float(hoy_max),
            "hoy_mag_promedio": float(hoy_avg),
            "ayer_sismos": int(ayer_cnt),
            "ayer_mag_max": float(ayer_max),
            "ayer_mag_promedio": float(ayer_avg),
            "diferencia_sismos": diff,
            "variacion_pct": pct,
            "conclusion": tendencia,
        }
    except Exception:
        return {"skill": "comparar_hoy_vs_ayer", "error": "Error al comparar la actividad de hoy con ayer."}
    finally:
        conn.close()


def top_regiones(
    n: int = 8,
    rango: str = "30d",
    hours: Optional[int] = None,
    min_mag: Optional[float] = None,
    window_label: str = "Catálogo Completo (30 días)",
) -> Dict[str, Any]:
    """
    Skill 2: Obtiene las N regiones con mayor actividad sísmica en la ventana/magnitud solicitada,
    e identifica además cuál de esas N regiones registró el sismo más fuerte del ranking.
    """
    limit_n = max(1, min(int(n), 15))
    where_sql, params = _build_sql_where(hours=hours, min_mag=min_mag)
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*)::int FROM curated.events WHERE {where_sql};", params)
            total_global = int(cur.fetchone()[0]) or 1

            cur.execute(
                f"""
                SELECT
                    region,
                    COUNT(*)::int AS conteo,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float AS mag_max,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float AS mag_promedio,
                    COALESCE(ROUND(AVG(depth_km)::numeric, 1), 0.0)::float AS prof_promedio
                FROM curated.events
                WHERE {where_sql}
                GROUP BY region
                ORDER BY conteo DESC, mag_max DESC
                LIMIT %s;
                """,
                [*params, limit_n],
            )
            rows = cur.fetchall()
            regiones = [
                {
                    "region": str(r[0]),
                    "conteo": int(r[1]),
                    "porcentaje": round((int(r[1]) * 100.0) / total_global, 1),
                    "mag_max": float(r[2]),
                    "mag_promedio": float(r[3]),
                    "prof_promedio_km": float(r[4]),
                }
                for r in rows
            ]
            lider_mag = max(regiones, key=lambda x: (x["mag_max"], x["conteo"])) if regiones else None
        return {
            "skill": "top_regiones",
            "ventana": window_label if hours else rango,
            "top_n": limit_n,
            "total_eventos_analizados": total_global,
            "lider_magnitud_en_top": lider_mag,
            "regiones": regiones,
        }
    except Exception:
        return {"skill": "top_regiones", "error": "Error al consultar las regiones más activas."}
    finally:
        conn.close()


def evento_mas_fuerte(
    rango: str = "30d",
    hours: Optional[int] = None,
    region: Optional[str] = None,
    limit: int = 5,
    window_label: str = "Últimos 30 días",
) -> Dict[str, Any]:
    """
    Skill 3: Devuelve el sismo de mayor magnitud (y el Top N de los más fuertes)
    para CUALQUIER ventana temporal (ej. hoy/24h, última semana/7d, 30 días) y región.
    """
    lim = max(1, min(int(limit), 15))
    where_sql, params = _build_sql_where(hours=hours, region=region)
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id,
                    ROUND(mag::numeric, 2)::float,
                    place,
                    region,
                    time,
                    ROUND(depth_km::numeric, 2)::float,
                    tsunami,
                    sig
                FROM curated.events
                WHERE {where_sql}
                ORDER BY mag DESC, time DESC
                LIMIT %s;
                """,
                [*params, lim],
            )
            rows = cur.fetchall()
            if not rows:
                return {
                    "skill": "evento_mas_fuerte",
                    "encontrado": False,
                    "ventana": window_label,
                    "region_filtro": region or "Global",
                }
            top_row = rows[0]
            top_list = [
                {
                    "id": str(r[0]),
                    "magnitud": float(r[1]),
                    "lugar": str(r[2]),
                    "region": str(r[3]),
                    "fecha_utc": r[4].strftime("%Y-%m-%d %H:%M UTC"),
                    "profundidad_km": float(r[5]),
                    "tsunami": int(r[6]),
                    "sig": int(r[7]),
                }
                for r in rows
            ]
            return {
                "skill": "evento_mas_fuerte",
                "encontrado": True,
                "ventana": window_label,
                "region_filtro": region or "Global",
                "id": str(top_row[0]),
                "magnitud": float(top_row[1]),
                "lugar": str(top_row[2]),
                "region": str(top_row[3]),
                "fecha_utc": top_row[4].strftime("%Y-%m-%d %H:%M UTC"),
                "profundidad_km": float(top_row[5]),
                "tsunami": int(top_row[6]),
                "sig": int(top_row[7]),
                "top_fuertes": top_list,
            }
    except Exception:
        return {"skill": "evento_mas_fuerte", "error": "Error al buscar el sismo más fuerte."}
    finally:
        conn.close()


def serie_horaria(
    rango: str = "24h",
    hours: int = 24,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Skill 4: Resume la evolución horaria y el pico de sismos por hora en la ventana y región indicadas."""
    where_sql, params = _build_sql_where(hours=hours, region=region)
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    date_trunc('hour', time) AS hora,
                    COUNT(*)::int AS conteo,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float AS mag_max,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float AS mag_promedio
                FROM curated.events
                WHERE {where_sql}
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT %s;
                """,
                [*params, max(24, min(int(hours), 168))],
            )
            rows = cur.fetchall()
            puntos = [
                {
                    "hora_utc": r[0].strftime("%Y-%m-%d %H:00 UTC"),
                    "conteo": int(r[1]),
                    "mag_max": float(r[2]),
                    "mag_promedio": float(r[3]),
                }
                for r in rows
            ]
            total_horas = len(puntos)
            max_hora = max(puntos, key=lambda x: (x["conteo"], x["mag_max"])) if puntos else None
        return {
            "skill": "serie_horaria",
            "rango": f"Últimas {hours} horas",
            "region_filtro": region or "Global",
            "horas_analizadas": total_horas,
            "hora_pico": max_hora,
            "puntos_recientes": puntos[:10],
        }
    except Exception:
        return {"skill": "serie_horaria", "error": "Error al consultar la serie horaria."}
    finally:
        conn.close()


def buscar_eventos(filtros: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Skill 5: Busca eventos sísmicos según filtros combinados de tiempo, magnitud, región, tsunami y límite."""
    f = filtros or {}
    minutes = f.get("minutes")
    hours = f.get("hours")
    min_mag = f.get("min_mag")
    max_mag = f.get("max_mag")
    region = f.get("region")
    only_tsunami = bool(f.get("only_tsunami", False))
    order_by_time = bool(f.get("order_by_time", False))
    limit = max(1, min(int(f.get("limit", 10)), 25))
    window_label = f.get("window_label", "Catálogo Completo")

    where_sql, params = _build_sql_where(
        hours=hours,
        region=region,
        min_mag=min_mag,
        max_mag=max_mag,
        only_tsunami=only_tsunami,
        minutes=minutes,
    )
    order_clause = "time DESC, mag DESC" if order_by_time else "mag DESC, time DESC"

    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float
                FROM curated.events
                WHERE {where_sql};
                """,
                params,
            )
            total_matching, m_max, m_avg = cur.fetchone()

            cur.execute(
                f"""
                SELECT
                    id,
                    ROUND(mag::numeric, 2)::float,
                    place,
                    region,
                    time,
                    ROUND(depth_km::numeric, 1)::float,
                    tsunami
                FROM curated.events
                WHERE {where_sql}
                ORDER BY {order_clause}
                LIMIT %s;
                """,
                [*params, limit],
            )
            eventos = [
                {
                    "id": str(r[0]),
                    "magnitud": float(r[1]),
                    "lugar": str(r[2]),
                    "region": str(r[3]),
                    "fecha_utc": r[4].strftime("%Y-%m-%d %H:%M UTC"),
                    "profundidad_km": float(r[5]),
                    "tsunami": int(r[6]),
                }
                for r in cur.fetchall()
            ]
        return {
            "skill": "buscar_eventos",
            "ventana": window_label,
            "filtros": {
                "min_mag": min_mag,
                "max_mag": max_mag,
                "region": region or "Global",
                "only_tsunami": only_tsunami,
            },
            "total_coincidencias": int(total_matching),
            "cantidad_encontrada": len(eventos),
            "magnitud_maxima": float(m_max),
            "magnitud_promedio": float(m_avg),
            "eventos": eventos,
        }
    except Exception:
        return {"skill": "buscar_eventos", "error": "Error al buscar eventos sísmicos."}
    finally:
        conn.close()


def estadisticas(
    mag: float = 0.0,
    profundidad: float = 800.0,
    hours: Optional[int] = None,
    region: Optional[str] = None,
    window_label: str = "Catálogo Completo (30 días)",
) -> Dict[str, Any]:
    """
    Skill 6: Calcula estadísticas descriptivas avanzadas sobre PostgreSQL (`curated.events`),
    incluyendo mediana, percentil 90, distribución por magnitud y distribución por profundidad.
    """
    where_sql, params = _build_sql_where(hours=hours, region=region, min_mag=mag if mag > 0 else None)
    where_sql += " AND depth_km <= %s"
    params.append(float(profundidad))

    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MIN(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mag))::numeric, 2), 0.0)::float,
                    COALESCE(ROUND((PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY mag))::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(MIN(depth_km)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(depth_km)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(MAX(depth_km)::numeric, 2), 0.0)::float,
                    COALESCE(SUM(CASE WHEN mag >= 5.0 THEN 1 ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN mag < 3.0 THEN 1 ELSE 0 END), 0)::int,
                    COALESCE(SUM(tsunami), 0)::int
                FROM curated.events
                WHERE {where_sql};
                """,
                params,
            )
            (
                cnt,
                m_min,
                m_avg,
                m_med,
                m_p90,
                m_max,
                d_min,
                d_avg,
                d_max,
                strong_cnt,
                micro_cnt,
                tsu,
            ) = cur.fetchone()

            cur.execute(
                """
                SELECT COUNT(*)::int
                FROM curated.events
                WHERE time >= (SELECT COALESCE(MAX(time), NOW()) FROM curated.events) - INTERVAL '24 hours';
                """
            )
            cnt_24h = int(cur.fetchone()[0])

            cur.execute(
                f"""
                SELECT
                    CASE
                        WHEN mag < 3.0 THEN '< 3.0 (Micro)'
                        WHEN mag < 4.0 THEN '3.0 - 3.9 (Menor)'
                        WHEN mag < 5.0 THEN '4.0 - 4.9 (Ligero)'
                        WHEN mag < 6.0 THEN '5.0 - 5.9 (Moderado)'
                        WHEN mag < 7.0 THEN '6.0 - 6.9 (Fuerte)'
                        ELSE '7.0+ (Mayor/Gran)'
                    END AS rango,
                    COUNT(*)::int AS conteo,
                    COALESCE(ROUND(AVG(depth_km)::numeric, 1), 0.0)::float AS prof_media
                FROM curated.events
                WHERE {where_sql}
                GROUP BY 1
                ORDER BY MIN(mag) ASC;
                """,
                params,
            )
            distribucion_mag = [
                {
                    "rango": str(r[0]),
                    "conteo": int(r[1]),
                    "porcentaje": round((int(r[1]) * 100.0) / (int(cnt) or 1), 1),
                    "prof_media_km": float(r[2]),
                }
                for r in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT
                    CASE
                        WHEN depth_km < 70.0 THEN 'Superficial (0 - 70 km)'
                        WHEN depth_km < 300.0 THEN 'Intermedio (70 - 300 km)'
                        ELSE 'Profundo (300 - 800 km)'
                    END AS capa,
                    COUNT(*)::int AS conteo,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float AS mag_max,
                    CASE
                        WHEN depth_km < 70.0 THEN 1
                        WHEN depth_km < 300.0 THEN 2
                        ELSE 3
                    END AS orden
                FROM curated.events
                WHERE {where_sql}
                GROUP BY 1, 4
                ORDER BY orden ASC;
                """,
                params,
            )
            distribucion_prof = [
                {
                    "capa": str(r[0]),
                    "conteo": int(r[1]),
                    "porcentaje": round((int(r[1]) * 100.0) / (int(cnt) or 1), 1),
                    "mag_max": float(r[2]),
                }
                for r in cur.fetchall()
            ]

        return {
            "skill": "estadisticas",
            "ventana": window_label,
            "region_filtro": region or "Global",
            "total_eventos": int(cnt),
            "eventos_ultimas_24h": cnt_24h,
            "mag_minima": float(m_min),
            "mag_promedio": float(m_avg),
            "mag_mediana": float(m_med),
            "mag_percentil_90": float(m_p90),
            "mag_maxima": float(m_max),
            "profundidad_minima_km": float(d_min),
            "profundidad_promedio_km": float(d_avg),
            "profundidad_maxima_km": float(d_max),
            "sismos_fuertes_m5": int(strong_cnt),
            "sismos_menores_m3": int(micro_cnt),
            "alertas_tsunami": int(tsu),
            "distribucion_magnitud": distribucion_mag,
            "distribucion_profundidad": distribucion_prof,
        }
    except Exception:
        return {"skill": "estadisticas", "error": "Error al calcular estadísticas en la base de datos."}
    finally:
        conn.close()


def sismos_por_profundidad(
    modo: str = "mas_profundos",
    hours: Optional[int] = None,
    region: Optional[str] = None,
    limit: int = 8,
    window_label: str = "Catálogo Completo",
) -> Dict[str, Any]:
    """
    Consulta los sismos más profundos o más superficiales registrados en la base de datos.
    """
    lim = max(1, min(int(limit), 20))
    where_sql, params = _build_sql_where(hours=hours, region=region)
    order_dir = "ASC" if modo == "mas_superficiales" else "DESC"
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id,
                    ROUND(mag::numeric, 2)::float,
                    place,
                    region,
                    time,
                    ROUND(depth_km::numeric, 1)::float
                FROM curated.events
                WHERE {where_sql}
                ORDER BY depth_km {order_dir}, mag DESC
                LIMIT %s;
                """,
                [*params, lim],
            )
            eventos = [
                {
                    "id": str(r[0]),
                    "magnitud": float(r[1]),
                    "lugar": str(r[2]),
                    "region": str(r[3]),
                    "fecha_utc": r[4].strftime("%Y-%m-%d %H:%M UTC"),
                    "profundidad_km": float(r[5]),
                }
                for r in cur.fetchall()
            ]
        return {
            "skill": "sismos_por_profundidad",
            "modo": modo,
            "ventana": window_label,
            "region_filtro": region or "Global",
            "eventos": eventos,
        }
    except Exception:
        return {"skill": "sismos_por_profundidad", "error": "Error al consultar sismos por profundidad."}
    finally:
        conn.close()


def detectar_anomalias(ventana: str = "72h", region: Optional[str] = None) -> Dict[str, Any]:
    """
    Skill 7: Consulta las anomalías detectadas (z-score y enjambres DBSCAN) en `curated.anomalies`,
    soportando filtrado opcional por región o tipo de anomalía.
    """
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            if region:
                cur.execute(
                    """
                    SELECT tipo, ventana, ROUND(score::numeric, 2)::float, explicacion, detected_at
                    FROM curated.anomalies
                    WHERE ventana ILIKE %s OR explicacion ILIKE %s
                    ORDER BY detected_at DESC, score DESC
                    LIMIT 10;
                    """,
                    (f"%{region}%", f"%{region}%"),
                )
                rows = cur.fetchall()
            else:
                rows = []

            if not rows:
                cur.execute(
                    """
                    SELECT tipo, ventana, ROUND(score::numeric, 2)::float, explicacion, detected_at
                    FROM curated.anomalies
                    ORDER BY detected_at DESC, score DESC
                    LIMIT 10;
                    """
                )
                rows = cur.fetchall()

            anomalias = [
                {
                    "tipo": str(r[0]),
                    "ventana": str(r[1]),
                    "score": float(r[2]),
                    "explicacion": str(r[3]),
                    "detectada_utc": r[4].strftime("%Y-%m-%d %H:%M UTC"),
                }
                for r in rows
            ]
        return {
            "skill": "detectar_anomalias",
            "ventana": ventana,
            "region_filtro": region or "Global",
            "total_anomalias": len(anomalias),
            "anomalias": anomalias,
        }
    except Exception:
        return {"skill": "detectar_anomalias", "error": "Error al consultar anomalías."}
    finally:
        conn.close()


def estado_sistema_y_calidad() -> Dict[str, Any]:
    """
    Meta-Consultas (Sección 6): Audita la frescura de la base de datos, el puntaje de calidad
    de la última corrida de Airflow, los registros inválidos rechazados por regla (incluyendo coordenadas)
    y el historial reciente de ejecuciones de los DAGs en `curated.quality_results` y `curated.pipeline_runs`.
    """
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    run_id,
                    timestamp,
                    volumen_total,
                    aprobadas,
                    rechazadas,
                    ROUND((puntaje * 100)::numeric, 2)::float AS puntaje_pct,
                    frescura_ok,
                    volumen_ok,
                    conteo_por_regla,
                    ROUND(frescura_minutos::numeric, 1)::float AS min_desde_calidad
                FROM curated.quality_results
                ORDER BY timestamp DESC
                LIMIT 1;
                """
            )
            q_row = cur.fetchone()

            cur.execute(
                """
                SELECT
                    COALESCE(SUM(rechazadas) FILTER (WHERE timestamp >= NOW() - INTERVAL '24 hours'), 0)::int AS rechazados_24h,
                    COALESCE(SUM(rechazadas) FILTER (WHERE timestamp >= NOW() - INTERVAL '1 hour'), 0)::int AS rechazados_1h,
                    COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '24 hours')::int AS lotes_evaluados_24h
                FROM curated.quality_results;
                """
            )
            rej_24h, rej_1h, lotes_24h = cur.fetchone()

            cur.execute(
                """
                SELECT
                    dag_id,
                    run_id,
                    estado,
                    fin,
                    COALESCE(data_version, 'N/A') AS version_dato
                FROM curated.pipeline_runs
                ORDER BY fin DESC
                LIMIT 8;
                """
            )
            runs_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE fin >= NOW() - INTERVAL '24 hours' AND estado NOT IN ('SUCCESS', 'COMPLETED'))::int AS fallos_hoy,
                    MAX(fin) AS ultima_actualizacion_pipeline
                FROM curated.pipeline_runs;
                """
            )
            fallos_hoy, max_fin = cur.fetchone()

        por_regla = {}
        if q_row and q_row[8]:
            por_regla = q_row[8] if isinstance(q_row[8], dict) else json.loads(q_row[8])

        rechazos_coordenadas = (
            int(por_regla.get("rango_latitud", 0))
            + int(por_regla.get("rango_longitud", 0))
            + int(por_regla.get("R03_LAT_RANGE", 0))
            + int(por_regla.get("R04_LON_RANGE", 0))
        )

        corridas = [
            {
                "dag_id": str(r[0]),
                "run_id": str(r[1]),
                "estado": str(r[2]),
                "fin_utc": r[3].strftime("%Y-%m-%d %H:%M:%S UTC") if r[3] else "N/A",
                "data_version": str(r[4]),
            }
            for r in runs_rows
        ]

        return {
            "skill": "estado_sistema_y_calidad",
            "ultima_actualizacion_utc": (
                max_fin.strftime("%Y-%m-%d %H:%M:%S UTC")
                if max_fin
                else (q_row[1].strftime("%Y-%m-%d %H:%M:%S UTC") if q_row else "N/A")
            ),
            "frescura_minutos": float(q_row[9]) if q_row else 0.0,
            "ultimo_run_id": str(q_row[0]) if q_row else "N/A",
            "puntaje_calidad_pct": float(q_row[5]) if q_row else 100.0,
            "total_evaluados_ultimo_lote": int(q_row[2]) if q_row else 0,
            "validos_ultimo_lote": int(q_row[3]) if q_row else 0,
            "rechazados_ultimo_lote": int(q_row[4]) if q_row else 0,
            "rechazados_ultima_hora": int(rej_1h),
            "rechazados_hoy_24h": int(rej_24h),
            "rechazados_coordenadas": rechazos_coordenadas,
            "lotes_evaluados_24h": int(lotes_24h),
            "frescura_ok": bool(q_row[6]) if q_row else True,
            "volumen_ok": bool(q_row[7]) if q_row else True,
            "fallos_dag_hoy": int(fallos_hoy or 0),
            "desglose_reglas": por_regla,
            "ultimas_corridas": corridas,
        }
    except Exception:
        return {"skill": "estado_sistema_y_calidad", "error": "Error al auditar el estado del sistema y calidad."}
    finally:
        conn.close()


def consulta_sql_segura(sql_select: str) -> Dict[str, Any]:
    """
    Skill 8: Valida y ejecuta un SELECT de solo lectura sobre tablas permitidas de `curated`
    usando exclusivamente `reader_role` con `statement_timeout` de 3 segundos.
    """
    try:
        safe_sql = validate_safe_sql(sql_select)
    except UnsafeQueryError as exc:
        return {
            "skill": "consulta_sql_segura",
            "rechazada_por_seguridad": True,
            "motivo": exc.message,
        }

    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '3000ms';")
            cur.execute(safe_sql)
            colnames = [desc[0] for desc in cur.description] if cur.description else []
            raw_rows = cur.fetchall()
            filas = [
                {col: (val.isoformat() if hasattr(val, "isoformat") else val) for col, val in zip(colnames, row)}
                for row in raw_rows
            ]
        return {
            "skill": "consulta_sql_segura",
            "rechazada_por_seguridad": False,
            "sql_ejecutado": safe_sql,
            "total_filas": len(filas),
            "filas": filas,
        }
    except Exception:
        return {
            "skill": "consulta_sql_segura",
            "rechazada_por_seguridad": False,
            "error": "Hubo un problema consultando la base de datos en tiempo real. Verifica los parámetros solicitados.",
        }
    finally:
        conn.close()


def consulta_analitica_dinamica(question: str, norm_q: str) -> Optional[Dict[str, Any]]:
    """
    Motor Text-to-SQL Analítico Seguro:
    Traduce preguntas analíticas abiertas en lenguaje natural hacia una consulta SQL SELECT
    de solo lectura sobre `curated.events`, `curated.agg_hourly`, `curated.agg_region`,
    `curated.anomalies`, `curated.quality_results` o `curated.pipeline_runs`,
    la valida con `validate_safe_sql` y la ejecuta en PostgreSQL con `reader_role`.
    """
    s = get_settings()
    schema_prompt = (
        "Eres SismoWatch AI, el experto en SQL PostgreSQL para el sistema sismológico SismoWatch de SuperApp.\n"
        "NUNCA respondas de memoria. Convierte la pregunta del usuario en UNA ÚNICA sentencia SQL SELECT válida sobre el schema `curated`.\n"
        "Tablas disponibles:\n"
        "- curated.events (id text, mag float, mag_type text, place text, time timestamptz, depth_km float, lat float, lon float, tsunami int, sig int, status text, type text, region text)\n"
        "- curated.agg_hourly (hora timestamptz, conteo int, mag_max float, mag_promedio float, tsunami int)\n"
        "- curated.agg_region (region text, fecha date, conteo int, mag_max float)\n"
        "- curated.anomalies (tipo text, ventana text, score float, explicacion text, detected_at timestamptz)\n"
        "- curated.quality_results (run_id text, timestamp timestamptz, total int, validos int, rechazados int, puntaje float, frescura_ok boolean, volumen_ok boolean)\n"
        "- curated.pipeline_runs (dag_id text, run_id text, inicio timestamptz, fin timestamptz, estado text)\n"
        "REGLAS OBLIGATORIAS:\n"
        "1. Responde ÚNICAMENTE con la sentencia SQL pura (sin bloques ```sql ni explicaciones).\n"
        "2. Empieza con SELECT y termina SIEMPRE con LIMIT (máximo LIMIT 15).\n"
        "3. Para 'el último sismo', siempre ordena por time DESC LIMIT 1.\n"
        "4. Para 'hoy' o 'últimas 24 horas', usa: time >= (SELECT MAX(time) FROM curated.events) - INTERVAL '24 hours'.\n"
        "5. Redondea decimales con ROUND(valor::numeric, 2)."
    )

    candidate_sql: Optional[str] = None

    if "tipo de magnitud" in norm_q or "mag_type" in norm_q or ("escala" in norm_q and "cuantos" in norm_q):
        candidate_sql = (
            "SELECT mag_type AS escala_magnitud, COUNT(*)::int AS cantidad_sismos, "
            "ROUND(AVG(mag)::numeric, 2)::float AS mag_promedio, ROUND(MAX(mag)::numeric, 2)::float AS mag_maxima "
            "FROM curated.events GROUP BY mag_type ORDER BY cantidad_sismos DESC LIMIT 10"
        )
    elif "por dia" in norm_q or "diario" in norm_q or "cada dia" in norm_q:
        candidate_sql = (
            "SELECT to_char(date_trunc('day', time), 'YYYY-MM-DD') AS fecha, COUNT(*)::int AS total_sismos, "
            "ROUND(MAX(mag)::numeric, 2)::float AS mag_maxima, ROUND(AVG(mag)::numeric, 2)::float AS mag_promedio, "
            "ROUND(AVG(depth_km)::numeric, 1)::float AS prof_promedio_km "
            "FROM curated.events GROUP BY 1 ORDER BY 1 DESC LIMIT 14"
        )
    elif "significancia" in norm_q or "impacto" in norm_q:
        candidate_sql = (
            "SELECT id, ROUND(mag::numeric, 2)::float AS magnitud, sig AS significancia, place AS ubicacion, "
            "region, ROUND(depth_km::numeric, 1)::float AS profundidad_km, to_char(time, 'YYYY-MM-DD HH24:MI UTC') AS fecha_utc "
            "FROM curated.events ORDER BY sig DESC, mag DESC LIMIT 10"
        )

    # Si no cayó en una regla directa, pedir al LLM (Groq -> Ollama local Qwen 2.5) la sentencia SQL
    if not candidate_sql and s.groq_api_key and not s.groq_api_key.startswith("your_groq") and not s.groq_api_key.startswith("CHANGE_ME"):
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {s.groq_api_key}", "Content-Type": "application/json"},
                json={
                    "model": s.groq_model,
                    "messages": [
                        {"role": "system", "content": schema_prompt},
                        {"role": "user", "content": question},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 220,
                },
                timeout=(2.5, 6.0),
            )
            if resp.status_code == 200:
                raw_sql = resp.json()["choices"][0]["message"]["content"].strip()
                raw_sql = re.sub(r"^```(?:sql)?\s*|\s*```$", "", raw_sql, flags=re.IGNORECASE).strip()
                candidate_sql = raw_sql.rstrip(";")
        except requests.RequestException:
            pass

    if not candidate_sql:
        try:
            resp = requests.post(
                f"{s.ollama_url}/api/chat",
                json={
                    "model": s.ollama_fallback_model,
                    "messages": [
                        {"role": "system", "content": schema_prompt},
                        {"role": "user", "content": question},
                    ],
                    "stream": False,
                    "options": {"temperature": 0.0},
                },
                timeout=(2.0, 6.0),
            )
            if resp.status_code == 200:
                raw_sql = (resp.json().get("message", {}) or {}).get("content", "").strip()
                raw_sql = re.sub(r"^```(?:sql)?\s*|\s*```$", "", raw_sql, flags=re.IGNORECASE).strip()
                candidate_sql = raw_sql.rstrip(";")
        except requests.RequestException:
            pass

    if not candidate_sql:
        return None

    res = consulta_sql_segura(candidate_sql)
    if res.get("rechazada_por_seguridad") or res.get("error") or not res.get("filas"):
        return None
    return res


def buscar_conocimiento(pregunta: str, norm_q: str = "") -> Dict[str, Any]:
    """
    Skill 9: Busca los fragmentos más relevantes en `curated.kb_chunks` usando distancia coseno
    en pgvector (`<=>`) y valida que la consulta pertenezca al dominio sísmico o de SismoWatch.
    """
    q_check = (norm_q or pregunta or "").lower()
    tokens = set(re.findall(r"[a-z0-9]+", q_check))
    has_domain_keyword = bool(tokens.intersection(DOMAIN_KEYWORDS)) or any(
        kw in q_check for kw in DOMAIN_KEYWORDS
    )
    has_region_keyword = any(alias in q_check for alias in REGION_ALIASES)

    if not has_domain_keyword and not has_region_keyword:
        return {
            "skill": "buscar_conocimiento",
            "validado": False,
            "contexto": [],
        }

    q_vec = compute_query_embedding(pregunta)
    vec_str = _vec_literal(q_vec)

    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fuente, contenido, ROUND((1 - (embedding <=> %s::vector))::numeric, 4)::float AS similitud
                FROM curated.kb_chunks
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector ASC
                LIMIT 4;
                """,
                (vec_str, vec_str),
            )
            chunks = [
                {"fuente": str(r[0]), "contenido": str(r[1]), "similitud": float(r[2])}
                for r in cur.fetchall()
            ]
            max_sim = max((c["similitud"] for c in chunks), default=0.0)
            if not has_domain_keyword and not has_region_keyword and max_sim < 0.65:
                return {"skill": "buscar_conocimiento", "validado": False, "contexto": []}
        return {"skill": "buscar_conocimiento", "validado": True, "contexto": chunks}
    except Exception:
        return {"skill": "buscar_conocimiento", "validado": has_domain_keyword or has_region_keyword, "contexto": []}
    finally:
        conn.close()
