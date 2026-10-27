"""
Orquestador del Agente Sísmico Analítico de SismoWatch (SismoWatch AI):
- Entrenado con System Prompt riguroso para que jamás responda de memoria sobre eventos recientes
  y consulte siempre la base de datos PostgreSQL (`curated`) como única fuente de verdad.
- Cubre los 6 bloques de consulta del banco de preguntas:
  1. Consultas de Tiempo Real y Eventos Recientes (`ultimo_sismo_detallado`, `ultimos_sismos`)
  2. Análisis de Magnitudes y Peligrosidad / Tsunamis (`evento_mas_fuerte`, `buscar_eventos`, `kpis`)
  3. Consultas Geoespaciales y por Región (`top_regiones`, `buscar_eventos`, macro-regiones)
  4. Estadísticas Agregadas, KPIs y Tendencias (`kpis`, `serie_horaria`, `comparar_hoy_vs_ayer`, `estadisticas`)
  5. Detección de Anomalías y Comportamiento Inusual (`detectar_anomalias` Z-score / DBSCAN)
  6. Meta-Consultas de Estado del Sistema, Calidad y Base de Datos (`estado_sistema_y_calidad`, `consulta_sql_segura`)
"""
from datetime import datetime, timezone
from difflib import SequenceMatcher
import hashlib
import json
import re
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
import requests
from app.agent import skills
from app.core.config import get_db_conn, get_settings
from app.models.schemas import AdminActionResponse, ChatResponse
from app.services.data_service import get_current_data_version

NUMBER_RE = re.compile(r"(?<![a-zA-Z0-9_])-?\d+(?:\.\d+)?(?![a-zA-Z0-9_])")

# System Prompt oficial de SismoWatch AI (aplicado tanto a Groq como a Ollama local Qwen 2.5)
AGENT_SYSTEM_PROMPT = (
    "Eres SismoWatch AI, el asistente experto en datos sísmicos de SuperApp. "
    "NUNCA respondas de memoria sobre eventos recientes.\n"
    "Tu única fuente de verdad es la base de datos PostgreSQL (curated).\n\n"
    "REGLAS OBLIGATORIAS DE OPERACIÓN:\n"
    "1. Si el usuario pregunta por un sismo específico, un evento reciente, estadísticas, anomalías, "
    "calidad de datos o regiones, DEBES usar la herramienta consulta_sql_segura o las skills disponibles.\n"
    "2. Para 'el último sismo', siempre ordena por time DESC LIMIT 1.\n"
    "3. Para 'estadísticas de hoy', filtra por las últimas 24 horas o time >= CURRENT_DATE.\n"
    "4. Si intentas usar una herramienta y falla, pide disculpas y dile al usuario que hubo un problema "
    "consultando la base de datos en tiempo real.\n"
    "5. Responde siempre en español limpio, organizado y con tablas Markdown (| Columna | Valor |), "
    "PROHIBIDO usar asteriscos (**), guiones bajos (__) o comillas invertidas (`).\n"
    "6. Usa EXCLUSIVAMENTE las cifras exactas devueltas por la base de datos sin alterarlas ni inventar números."
)

# Mapa de normalización léxica para errores ortográficos comunes y jerga/modismos
TYPO_REPLACEMENTS: Dict[str, str] = {
    "komo": "como",
    "cmo": "como",
    "kmo": "como",
    "ke": "que",
    "q": "que",
    "k": "que",
    "kual": "cual",
    "kuales": "cuales",
    "xq": "por que",
    "pq": "por que",
    "porq": "por que",
    "pa": "para",
    "funsiona": "funciona",
    "funsionas": "funcionas",
    "aser": "hacer",
    "hases": "haces",
    "aces": "haces",
    "ayudar": "ayuda",
    "alluda": "ayuda",
    "aiuda": "ayuda",
    "cismo": "sismo",
    "cismos": "sismos",
    "cismes": "sismos",
    "zismo": "sismo",
    "zismos": "sismos",
    "sismoz": "sismos",
    "terremto": "terremoto",
    "teremoto": "terremoto",
    "terremotos": "terremoto",
    "temblores": "temblor",
    "estadistikas": "estadisticas",
    "estadistika": "estadistica",
    "estadisitcas": "estadisticas",
    "metricaz": "metricas",
    "metrikas": "metricas",
    "anomaliaz": "anomalias",
    "rejiones": "regiones",
    "regionez": "regiones",
    "oy": "hoy",
    "haora": "ahora",
    "aora": "ahora",
    "grasias": "gracias",
    "grax": "gracias",
    "grx": "gracias",
    "wenas": "buenas",
    "wuenas": "buenas",
    "holi": "hola",
    "oli": "hola",
    "holis": "hola",
}

# Palabras que indican que el usuario está pidiendo datos reales o estadísticas de la BD (y no solo una definición conceptual)
DATA_QUERY_INDICATORS = (
    "cual fue",
    "cual ha sido",
    "cuales fueron",
    "cuales son",
    "cual es el sismo",
    "cual es el temblor",
    "cual es la magnitud",
    "que sismos",
    "ultimo sismo",
    "ultimo temblor",
    "sismo mas reciente",
    "hace cuantos minutos",
    "a que hora",
    "ha temblado",
    "ultimos",
    "ultimas",
    "recientes",
    "cuantos",
    "cuantas",
    "dame",
    "muestrame",
    "mostrar",
    "lista",
    "tabla",
    "ranking",
    "top",
    "mayor",
    "mayores a",
    "menores a",
    "superaron",
    "mas fuerte",
    "mas grande",
    "mas bravo",
    "sacudon",
    "remezon",
    "meneo",
    "mas profundo",
    "mas superficial",
    "estadistica",
    "estadisticas",
    "metricas",
    "kpi",
    "kpis",
    "buscar",
    "hubo",
    "hemos tenido",
    "tuvieron",
    "activaron",
    "generado alerta",
    "registrado",
    "registrados",
    "presentado",
    "ocurrido",
    "ocurrieron",
    "comparado con ayer",
    "ayer",
    "hoy",
    "24 horas",
    "24h",
    "48 horas",
    "15 minutos",
    "ultima hora",
    "semana",
    "ultimo mes",
    "donde tiembla",
    "donde esta temblando",
    "donde sacude",
    "regiones con mas",
    "regiones mas",
    "zonas mas",
    "cerca de",
    "en que hora",
    "pico de sismos",
    "enjambre",
    "anomalia",
    "patron fuera",
    "madrugada",
    "actualizo la base",
    "frescura",
    "rechazados",
    "descartado",
    "invalidos",
    "puntaje de calidad",
    "fallo algun dag",
    "corrida de airflow",
    "por dia",
    "por hora",
    "analiza",
    "analisis",
)


def normalize_question(text: str) -> str:
    """
    Normaliza una pregunta: minúsculas, sin tildes, sin signos de puntuación redundantes,
    y corrige errores ortográficos / abreviaturas informales comunes.
    """
    lowered = (text or "").strip().lower()
    nfkd = unicodedata.normalize("NFKD", lowered)
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[¿?¡!.,;:()\"']", " ", no_accents)
    tokens = [TYPO_REPLACEMENTS.get(tok, tok) for tok in cleaned.split() if tok]
    return re.sub(r"\s+", " ", " ".join(tokens)).strip()


def clean_response_formatting(text: str) -> str:
    """
    Limpia símbolos innecesarios (como ** o ` sueltos) conservando saltos de línea
    y tablas Markdown limpias (`| Columna | Valor |`).
    """
    if not text:
        return ""
    cleaned = re.sub(r"\*\*+", "", text)
    cleaned = re.sub(r"__+", "", cleaned)
    cleaned = re.sub(r"`+", "", cleaned)
    cleaned = re.sub(r"^#{1,4}\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _extract_numbers(text: str) -> List[float]:
    res: List[float] = []
    for m in NUMBER_RE.findall(text or ""):
        try:
            res.append(float(m))
        except ValueError:
            pass
    return res


def verify_numbers_in_response(text: str, expected_numbers: List[float], tol: float = 0.05) -> bool:
    if not expected_numbers:
        return True
    found_nums = _extract_numbers(text)
    if not found_nums:
        return False
    for exp in expected_numbers:
        if not any(abs(actual - float(exp)) <= tol for actual in found_nums):
            return False
    return True


# -----------------------------------------------------------------------------
# NIVEL 0: Intents conversacionales y FAQ conceptual
# -----------------------------------------------------------------------------
INTENT_PATTERNS: List[Tuple[str, re.Pattern, str]] = [
    (
        "saludo",
        re.compile(
            r"^(hola|ola|hello|hi|buenass|buenas|buenos dias|buenas tardes|buenas noches|saludos|hey|que tal|que mas pues|que hubo|quiubo|que onda|hablame|epale|ey)$"
        ),
        (
            "Hola. Soy SismoWatch AI, el asistente experto en datos sísmicos de SuperApp conectado en tiempo real a la base de datos PostgreSQL. "
            "Puedes hacerme cualquier consulta de los 6 bloques analíticos:\n\n"
            "| Categoría de Consulta | Ejemplos listos para preguntar |\n"
            "| :--- | :--- |\n"
            "| 1. Tiempo Real y Recientes | ¿Cuál fue el último sismo registrado? / ¿Hace cuántos minutos fue el sismo más reciente? |\n"
            "| 2. Magnitudes y Tsunamis | ¿Cuál ha sido el sismo más fuerte de hoy? / ¿Hubo algún sismo con alerta de tsunami? |\n"
            "| 3. Regiones y Geografía | ¿Cuáles son las 3 regiones con más actividad hoy? / ¿Hubo algún sismo en Japón o Alaska? |\n"
            "| 4. Estadísticas y Tendencias | Comparado con ayer, ¿hoy ha temblado más o menos? / ¿Cuál fue el pico de sismos por hora? |\n"
            "| 5. Anomalías (Z-score / DBSCAN) | ¿Hay algún enjambre sísmico activo o patrón inusual en las últimas 12 horas? |\n"
            "| 6. Estado, Calidad y Base de Datos | ¿A qué hora se actualizó la base de datos por última vez? / ¿Cuál fue el puntaje de calidad? |"
        ),
    ),
    (
        "despedida",
        re.compile(
            r"^(adios|hasta luego|chao|nos vemos|bye|hasta pronto|see you|see you later|chau|me voy|todo bien chao)\b"
        ),
        "Hasta luego. El monitoreo sísmico global de SismoWatch AI permanece activo las 24 horas.",
    ),
    (
        "agradecimiento",
        re.compile(
            r"^(gracias|ok|ok, gracias|muchas gracias|mil gracias|excelente gracias|te lo agradezco|muchisimas gracias|thanks|thank you|graciass|buena esa|genial gracias)\b"
        ),
        "Con mucho gusto. Estoy disponible para cualquier otra consulta en tiempo real sobre sismos, regiones, anomalías o estado de la base de datos.",
    ),
    (
        "capacidades",
        re.compile(
            r"\b(que puedes hacer|que haces|ayuda|comandos|como funcionas|que herramientas tienes|que sabes hacer|menu de ayuda|para que sirves|que me puedes decir)\b"
        ),
        (
            "Soy SismoWatch AI y consulto directamente la base de datos PostgreSQL (curated) en tiempo real:\n\n"
            "| Bloque Analítico | Consultas que puedo responder al instante |\n"
            "| :--- | :--- |\n"
            "| 1. Tiempo Real y Eventos Recientes | Último sismo registrado, hace cuántos minutos tembló, últimos 5 sismos, última hora o últimos 15 minutos |\n"
            "| 2. Magnitudes y Peligrosidad | Sismo más fuerte de hoy o de la semana, mayores a 5.5 o 6.0 grados, alertas de tsunami y magnitud promedio |\n"
            "| 3. Geoespacial y Regiones | Top 3 o Top 5 regiones más sísmicas, cuál tuvo el sismo más fuerte, búsquedas en Japón, Chile, México, Alaska o South America |\n"
            "| 4. Estadísticas Agregadas y KPIs | Total de sismos hoy, comparativa hoy vs ayer, hora pico del día o de la semana, sismos menores a M 3.0 |\n"
            "| 5. Anomalías y Enjambres | Enjambres activos (DBSCAN), picos horarios inusuales (Z-score) y explicación de anomalías por región |\n"
            "| 6. Auditoría de Calidad y Pipeline | Última actualización (frescura), puntaje de calidad en Airflow, rechazos por coordenadas y estado de DAGs |"
        ),
    ),
]


def _match_level_0_intent_or_faq(norm_q: str, role: str = "viewer") -> Optional[Tuple[str, str]]:
    """
    Evalúa si la pregunta corresponde a un saludo/despedida/ayuda o a una pregunta conceptual de FAQ.
    Si la pregunta solicita datos específicos de la base de datos, delega directamente al motor SQL.
    """
    for intent_name, pattern, response_txt in INTENT_PATTERNS:
        if pattern.search(norm_q):
            return f"intent:{intent_name}", clean_response_formatting(response_txt)

    if any(ind in norm_q for ind in DATA_QUERY_INDICATORS):
        return None

    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT categoria, pregunta_canonica, patrones, respuesta_plantilla
                FROM curated.faq
                ORDER BY orden ASC;
                """
            )
            best_score = 0.0
            best_match: Optional[Tuple[str, str]] = None

            for cat, canon, patrones_raw, respuesta in cur.fetchall():
                patrones = patrones_raw if isinstance(patrones_raw, list) else json.loads(patrones_raw or "[]")
                candidates = [normalize_question(canon)] + [normalize_question(str(p)) for p in patrones]

                for cand in candidates:
                    if not cand:
                        continue
                    if cand in norm_q or norm_q == cand:
                        return f"faq:{cat}", clean_response_formatting(str(respuesta))
                    ratio = SequenceMatcher(None, norm_q, cand).ratio()
                    if ratio > best_score:
                        best_score = ratio
                        best_match = (f"faq:{cat}", clean_response_formatting(str(respuesta)))

            if best_score >= 0.78 and best_match is not None:
                return best_match
    except Exception:
        pass
    finally:
        conn.close()

    return None


# -----------------------------------------------------------------------------
# NIVEL 1: Caché Exacta y Semántica (incluyendo data_version, rol y TTL)
# -----------------------------------------------------------------------------
def _compute_cache_key(norm_q: str, data_version: str, role: str = "viewer") -> str:
    raw = f"{role}::{norm_q}::{data_version}" if role != "viewer" else f"{norm_q}::{data_version}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _lookup_level_1_cache(norm_q: str, data_version: str, role: str = "viewer") -> Optional[Dict[str, Any]]:
    cache_key = _compute_cache_key(norm_q, data_version, role=role)
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT respuesta
                FROM curated.ai_cache
                WHERE clave_hash = %s AND data_version = %s AND expires_at > NOW()
                LIMIT 1;
                """,
                (cache_key, data_version),
            )
            row = cur.fetchone()
            if row and row[0]:
                resp_obj = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                if resp_obj.get("role", "viewer") == role:
                    return {"cache_type": "exact", "payload": resp_obj}
    except Exception:
        return None
    finally:
        conn.close()
    return None


def _store_in_cache(norm_q: str, data_version: str, payload: Dict[str, Any], role: str = "viewer") -> None:
    s = get_settings()
    cache_key = _compute_cache_key(norm_q, data_version, role=role)
    payload_with_role = {**payload, "role": role}
    q_vec = skills.compute_query_embedding(norm_q)
    vec_str = "[" + ",".join(f"{v:.6f}" for v in q_vec) + "]"

    conn = get_db_conn(role="api_rw")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.ai_cache (
                        clave_hash, pregunta_normalizada, data_version, respuesta,
                        embedding, ttl, created_at, expires_at
                    )
                    VALUES (
                        %s, %s, %s, %s::jsonb, %s::vector, %s, NOW(),
                        NOW() + (%s || ' seconds')::interval
                    )
                    ON CONFLICT (clave_hash) DO UPDATE SET
                        respuesta = EXCLUDED.respuesta,
                        embedding = EXCLUDED.embedding,
                        expires_at = EXCLUDED.expires_at;
                    """,
                    (
                        cache_key,
                        f"[{role}] {norm_q}",
                        data_version,
                        json.dumps(payload_with_role, ensure_ascii=False),
                        vec_str,
                        s.ai_cache_ttl,
                        s.ai_cache_ttl,
                    ),
                )
    except Exception:
        pass
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# SELECCIÓN Y EJECUCIÓN DINÁMICA DE CONSULTAS A LA BASE DE DATOS (6 BLOQUES)
# -----------------------------------------------------------------------------
def _select_and_execute_skill(raw_question: str, norm_q: str) -> Tuple[str, Dict[str, Any], List[float]]:
    """
    Clasifica la pregunta en cualquiera de los 6 bloques analíticos, extrae filtros dinámicos
    (minutos, horas, días, región, magnitud, límite) y ejecuta la consulta sobre PostgreSQL (`curated`).
    """
    filters = skills.parse_natural_query_filters(raw_question, norm_q)
    minutes = filters["minutes"]
    hours = filters["hours"]
    window_label = filters["window_label"]
    region = filters["region"]
    min_mag = filters["min_mag"]
    max_mag = filters["max_mag"]
    limit = filters["limit"]

    # 0. Consulta SQL directa si el usuario escribe SELECT ...
    if norm_q.startswith("select ") or "consulta sql" in norm_q or "ejecuta sql" in norm_q:
        sql_match = re.search(r"(select\s+.+)$", raw_question, flags=re.IGNORECASE | re.DOTALL)
        sql_query = sql_match.group(1) if sql_match else raw_question
        data = skills.consulta_sql_segura(sql_query)
        req_nums = [float(data["total_filas"])] if "total_filas" in data else []
        return "consulta_sql_segura", data, req_nums

    # BLOQUE 6: Meta-Consultas (Estado del Sistema, Frescura, Calidad de Datos, Rechazos y DAGs de Airflow)
    if any(
        k in norm_q
        for k in (
            "actualizo la base de datos",
            "actualizo por ultima vez",
            "ultima actualizacion",
            "frescura del dato",
            "frescura",
            "registros fueron rechazados",
            "fueron rechazados",
            "sismos invalidos",
            "descartado",
            "descartados",
            "regla de calidad",
            "coordenadas",
            "puntaje de calidad",
            "ultima corrida de airflow",
            "corrida de airflow",
            "fallo algun dag",
            "fallaron los dags",
            "estado del pipeline",
            "estado de los dags",
        )
    ):
        data = skills.estado_sistema_y_calidad()
        req_nums = [float(data.get("puntaje_calidad_pct", 100.0))] if "error" not in data else []
        return "estado_sistema_y_calidad", data, req_nums

    # BLOQUE 4b: Comparativa temporal "Comparado con ayer, ¿hoy ha temblado más o menos?"
    if "comparado con ayer" in norm_q or ("ayer" in norm_q and ("mas o menos" in norm_q or "comparar" in norm_q or "diferencia" in norm_q)):
        data = skills.comparar_hoy_vs_ayer(region=region)
        req_nums = [float(data.get("hoy_sismos", 0)), float(data.get("ayer_sismos", 0))] if "error" not in data else []
        return "comparar_hoy_vs_ayer", data, req_nums

    # BLOQUE 1a: El último sismo registrado / hace cuántos minutos fue / a qué hora exacta ocurrió el último temblor / últimos 15 minutos
    if any(
        k in norm_q
        for k in (
            "ultimo sismo registrado",
            "ultimo temblor registrado",
            "cual fue el ultimo sismo",
            "cual fue el ultimo temblor",
            "sismo mas reciente",
            "temblor mas reciente",
            "hace cuantos minutos",
            "a que hora exacta ocurrio el ultimo",
            "a que hora ocurrio el ultimo",
            "ultimos 15 minutos",
            "ha temblado en los ultimos 15",
        )
    ):
        data = skills.ultimo_sismo_detallado()
        req_nums = [float(data["ultimo_evento"]["magnitud"])] if data.get("encontrado") else []
        return "ultimo_sismo_detallado", data, req_nums

    # BLOQUE 3a: Top regiones / dónde está temblando más / del top 5 de regiones cuál tuvo el sismo más fuerte
    if any(
        k in norm_q
        for k in (
            "regiones con mas",
            "regiones mas sismicas",
            "regiones mas activas",
            "zonas mas activas",
            "donde tiembla mas",
            "donde sacude mas",
            "donde esta temblando mas",
            "lugares con mas sismos",
            "paises con mas",
            "top regiones",
            "top 5 de regiones",
            "top 3 de regiones",
            "las 3 regiones",
            "las 5 regiones",
            "mas sismos por region",
            "donde se mueve mas la tierra",
            "zonas calientes",
            "ranking de regiones",
            "ranking de paises",
        )
    ):
        top_n = limit if limit != 8 else 5
        if "las 3 " in norm_q or "top 3" in norm_q:
            top_n = 3
        elif "top 5" in norm_q or "las 5 " in norm_q:
            top_n = 5
        data = skills.top_regiones(
            n=top_n,
            hours=hours,
            min_mag=min_mag,
            window_label=window_label,
        )
        regs = data.get("regiones", [])
        req_nums = [float(regs[0]["conteo"])] if regs else []
        return "top_regiones", data, req_nums

    # BLOQUE 5: Detección de Anomalías y Comportamiento Inusual (Z-score, DBSCAN, enjambres, madrugada, últimas 12 horas)
    if any(
        k in norm_q
        for k in (
            "anomalia",
            "anomalias",
            "enjambre",
            "enjambres",
            "dbscan",
            "z score",
            "z-score",
            "pico de actividad inusual",
            "actividad inusual",
            "patron fuera de lo normal",
            "fuera de lo normal",
            "algo raro",
            "actividad anormal",
            "comportamiento inusual",
        )
    ):
        data = skills.detectar_anomalias(ventana=window_label if hours else "72h", region=region)
        req_nums = [float(data.get("total_anomalias", 0))]
        return "detectar_anomalias", data, req_nums

    # BLOQUE 2a: Sismo más fuerte / de mayor magnitud (cuando no pide umbral numérico explícito min_mag/max_mag)
    if min_mag is None and max_mag is None and any(
        k in norm_q
        for k in (
            "mas fuerte",
            "sismo mayor",
            "terremoto mayor",
            "evento mayor",
            "mayor sismo",
            "mayor terremoto",
            "mayor magnitud",
            "terremoto mas grande",
            "sismo mas grande",
            "maxima magnitud",
            "sacudon",
            "remezon",
            "meneo",
            "temblor mas bravo",
            "sismo mas bravo",
            "el mas duro",
            "temblor mas duro",
            "terremoto mas fuerte",
            "temblor mas fuerte",
            "cual fue el peor",
            "sacudida mas fuerte",
            "mas potente",
            "mas intenso",
        )
    ):
        data = skills.evento_mas_fuerte(
            hours=hours,
            region=region,
            limit=max(5, limit),
            window_label=window_label,
        )
        req_nums = [float(data["magnitud"])] if data.get("encontrado") else []
        return "evento_mas_fuerte", data, req_nums

    # BLOQUE 2b: Sismos con alerta de tsunami
    if "tsunami" in norm_q or "maremoto" in norm_q:
        data = skills.buscar_eventos(
            {
                "hours": hours,
                "region": region,
                "only_tsunami": True,
                "order_by_time": True,
                "limit": 10,
                "window_label": window_label,
            }
        )
        req_nums = [float(data.get("total_coincidencias", 0))]
        return "buscar_eventos", data, req_nums

    # BLOQUE 4c: Serie horaria / en qué hora del día hubo más sismos / pico de sismos por hora en esta semana
    if any(
        k in norm_q
        for k in (
            "serie horaria",
            "por hora",
            "evolucion horaria",
            "hora pico",
            "en que hora",
            "que hora del dia hubo mas",
            "pico de sismos por hora",
            "ultimas horas",
            "actividad por hora",
            "tendencia horaria",
        )
    ):
        data = skills.serie_horaria(hours=hours or 24, region=region)
        req_nums = [float(data.get("horas_analizadas", 0))]
        return "serie_horaria", data, req_nums

    # BLOQUE 1b: Últimos N sismos / qué sismos han ocurrido en la última hora / últimos sismos cerca de X
    has_recent_word = any(w in norm_q for w in ("ultimos", "ultimas", "reciente", "recientes", "acaba de"))
    has_seismic_noun = any(w in norm_q for w in ("sismo", "sismos", "temblor", "terremoto", "evento", "eventos", "registrado", "registrados"))
    if (
        (has_recent_word and has_seismic_noun and min_mag is None and max_mag is None and "cuantos" not in norm_q)
        or any(
            k in norm_q
            for k in (
                "ultimos sismos",
                "sismos recientes",
                "que sismos han ocurrido",
                "que temblor hubo",
                "acaba de temblar",
                "donde acaba de temblar",
                "ultimos registrados",
            )
        )
    ):
        data = skills.ultimos_sismos(
            hours=hours,
            region=region,
            min_mag=min_mag,
            max_mag=max_mag,
            limit=limit,
            window_label=window_label,
            minutes=minutes,
        )
        req_nums = [float(data.get("total_en_ventana", 0))]
        return "ultimos_sismos", data, req_nums

    # 4. Sismos por profundidad (más profundos / más superficiales)
    if any(
        k in norm_q
        for k in (
            "mas profundo",
            "mas profundos",
            "mayor profundidad",
            "mas superficial",
            "mas superficiales",
            "menor profundidad",
            "menos profundo",
        )
    ):
        modo = "mas_superficiales" if ("superficial" in norm_q or "menor" in norm_q or "menos" in norm_q) else "mas_profundos"
        data = skills.sismos_por_profundidad(
            modo=modo,
            hours=hours,
            region=region,
            limit=limit,
            window_label=window_label,
        )
        evs = data.get("eventos", [])
        req_nums = [float(evs[0]["profundidad_km"])] if evs else []
        return "sismos_por_profundidad", data, req_nums

    # BLOQUE 2c y 3b: Búsqueda y conteo de sismos por magnitud específica (ej. > 6.0, > 5.5, < 3.0) o región (Japón, Chile, South America)
    if min_mag is not None or max_mag is not None or region is not None or any(
        k in norm_q
        for k in (
            "buscar sismos",
            "sismos en ",
            "temblor en ",
            "terremoto en ",
            "eventos en ",
            "sismos mayores a",
            "magnitud mayor a",
            "superaron los",
            "cerca de",
            "temblo en ",
            "que paso en ",
        )
    ):
        data = skills.buscar_eventos(
            {
                "minutes": minutes,
                "hours": hours,
                "min_mag": min_mag,
                "max_mag": max_mag,
                "region": region,
                "order_by_time": ("ultimo" in norm_q or "reciente" in norm_q),
                "limit": max(8, limit),
                "window_label": window_label,
            }
        )
        req_nums = [float(data.get("total_coincidencias", 0))]
        return "buscar_eventos", data, req_nums

    # 5b. Consultas analíticas agrupadas (por día, por escala de magnitud, por significancia)
    if any(
        k in norm_q
        for k in (
            "por dia",
            "cada dia",
            "evolucion diaria",
            "tipo de magnitud",
            "escala de magnitud",
            "mayor significancia",
            "mas significativos",
        )
    ):
        dyn_res = skills.consulta_analitica_dinamica(raw_question, norm_q)
        if dyn_res is not None:
            return "consulta_sql_segura", dyn_res, [float(dyn_res.get("total_filas", 0))]

    # BLOQUE 2c y 3b: Búsqueda y conteo de sismos por magnitud específica (ej. > 6.0, > 5.5, < 3.0) o región (Japón, Chile, South America)
    if min_mag is not None or max_mag is not None or region is not None or any(
        k in norm_q
        for k in (
            "buscar sismos",
            "sismos en ",
            "temblor en ",
            "terremoto en ",
            "eventos en ",
            "sismos mayores a",
            "magnitud mayor a",
            "superaron los",
            "cerca de",
            "temblo en ",
            "que paso en ",
        )
    ):
        data = skills.buscar_eventos(
            {
                "minutes": minutes,
                "hours": hours,
                "min_mag": min_mag,
                "max_mag": max_mag,
                "region": region,
                "order_by_time": ("ultimo" in norm_q or "reciente" in norm_q),
                "limit": max(8, limit),
                "window_label": window_label,
            }
        )
        req_nums = [float(data.get("total_coincidencias", 0))]
        return "buscar_eventos", data, req_nums

    # BLOQUE 4a: Estadísticas descriptivas avanzadas / magnitud promedio / resumen estadístico (KPIs)
    if any(
        k in norm_q
        for k in (
            "estadistica",
            "estadisticas",
            "resumen estadistico",
            "magnitud promedio",
            "promedio de magnitud",
            "promedio de profundidad",
            "profundidad media",
            "media de magnitud",
            "mediana",
            "percentil",
            "distribucion de magnitud",
            "distribucion por profundidad",
            "tabla de estadisticas",
            "tabla de metricas",
            "metricas completas",
            "analisis estadistico",
            "analiza los datos",
            "analizar estadisticas",
            "rangos de magnitud",
        )
    ):
        data = skills.estadisticas(
            mag=min_mag or 0.0,
            profundidad=800.0,
            hours=hours,
            region=region,
            window_label=window_label,
        )
        req_nums = [float(data.get("total_eventos", 0)), float(data.get("mag_maxima", 0.0))]
        return "estadisticas", data, req_nums

    # BLOQUE 4d: KPIs / cuántos sismos en total se han registrado hoy / resumen de las últimas 24 horas
    if any(
        k in norm_q
        for k in (
            "kpi",
            "kpis",
            "metricas",
            "cuantos sismos",
            "cuantos temblor",
            "cuantos terremoto",
            "cuantos eventos",
            "resumen",
            "estado actual",
            "ultimas 24",
            "que ha pasado hoy",
            "sismos hoy",
            "temblor hoy",
            "registrado hoy",
            "registrados hoy",
            "como esta la actividad",
            "dame los datos",
            "dame un reporte",
        )
    ):
        data = skills.kpis(
            hours=hours,
            region=region,
            window_label=window_label,
            min_mag=min_mag,
            max_mag=max_mag,
        )
        req_nums = [float(data.get("total_eventos", 0)), float(data.get("magnitud_maxima", 0.0))]
        return "kpis", data, req_nums

    # Validación de dominio sísmico
    kb_data = skills.buscar_conocimiento(raw_question, norm_q=norm_q)
    if not kb_data.get("validado", False):
        return "no_validado", {"validado": False, "pregunta": raw_question}, []

    # Si es una pregunta analítica abierta dentro del dominio sísmico, usar Motor Text-to-SQL Dinámico
    dyn_sql_res = skills.consulta_analitica_dinamica(raw_question, norm_q)
    if dyn_sql_res is not None:
        return "consulta_sql_segura", dyn_sql_res, [float(dyn_sql_res.get("total_filas", 0))]

    kpi_data = skills.kpis(hours=hours, region=region, window_label=window_label)
    combined = {
        "skill": "buscar_conocimiento",
        "kpis_actuales": kpi_data,
        "conocimiento_vectorial": kb_data.get("contexto", []),
    }
    req_nums = [float(kpi_data.get("total_eventos", 0))]
    return "buscar_conocimiento", combined, req_nums


def _format_deterministic_skill_answer(skill_name: str, data: Dict[str, Any]) -> str:
    """
    Formatea una respuesta limpia, organizada y con tablas Markdown completas
    a partir de los datos consultados en PostgreSQL (`curated`).
    Si hubo un fallo técnico en la herramienta, informa con claridad según el System Prompt.
    """
    if data.get("error") and skill_name != "consulta_sql_segura":
        return (
            "Te pido disculpas: hubo un problema consultando la base de datos en tiempo real. "
            f"Detalle: {data.get('error')}"
        )

    if skill_name == "ultimo_sismo_detallado":
        if not data.get("encontrado"):
            return "No se encontraron sismos registrados actualmente en la base de datos."
        ev = data["ultimo_evento"]
        recientes = data.get("ultimos_5_eventos", [])
        rows = "\n".join(
            f"| M {e['magnitud']} | {e['lugar']} | {e['region']} | {e['profundidad_km']} km | {e['fecha_utc']} | Hace {e['hace_minutos']} min |"
            for e in recientes
        )
        return (
            "Pulso Sísmico en Tiempo Real — Último Sismo Registrado en el Sistema (ORDER BY time DESC LIMIT 1):\n\n"
            "| Atributo del Último Sismo | Valor Exacto en Base de Datos |\n"
            "| :--- | :--- |\n"
            f"| ID Oficial USGS | {ev['id']} |\n"
            f"| Magnitud | M {ev['magnitud']} |\n"
            f"| Ubicación Exacta (Dónde fue) | {ev['lugar']} ({ev['region']}) |\n"
            f"| Fecha y Hora Exacta (UTC) | {ev['fecha_utc']} (Hora: {ev['hora_exacta_utc']}) |\n"
            f"| Tiempo Transcurrido | Hace {data.get('hace_minutos', 0.0)} minutos |\n"
            f"| Coordenadas y Profundidad | Lat {ev['lat']}, Lon {ev['lon']} • Profundidad {ev['profundidad_km']} km |\n"
            f"| Sismos en los Últimos 15 Minutos | {data.get('sismos_ultimos_15_min', 0)} sismos |\n"
            f"| Sismos en la Última Hora | {data.get('sismos_ultima_hora', 0)} sismos |\n\n"
            "Últimos 5 Sismos Registrados en el Mundo:\n\n"
            "| Magnitud | Ubicación | Región | Profundidad | Fecha y Hora (UTC) | Tiempo |\n"
            "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
            f"{rows}"
        )

    if skill_name == "ultimos_sismos":
        evs = data.get("eventos", [])
        tot = data.get("total_en_ventana", 0)
        vent = data.get("ventana", "Recientes")
        reg = data.get("region_filtro", "Global")
        if not evs:
            return f"No se registran sismos para el filtro solicitado ({vent}, Zona: {reg})."
        rows = "\n".join(
            f"| M {e['magnitud']} | {e['lugar']} | {e['region']} | {e['profundidad_km']} km | {e['fecha_utc']} |"
            for e in evs
        )
        return (
            f"Últimos Sismos Registrados en la Base de Datos ({vent} • Zona: {reg}):\n\n"
            "| Resumen de la Ventana | Valor Calculado |\n"
            "| :--- | :--- |\n"
            f"| Total de Sismos en el Periodo | {tot} sismos |\n"
            f"| Magnitud Máxima / Promedio | M {data.get('magnitud_maxima', 0.0)} / M {data.get('magnitud_promedio', 0.0)} |\n"
            f"| Profundidad Promedio | {data.get('profundidad_promedio_km', 0.0)} km |\n"
            f"| Alertas de Tsunami | {data.get('alertas_tsunami', 0)} |\n\n"
            f"Listado de los {len(evs)} Sismos Más Recientes (ORDER BY time DESC):\n\n"
            "| Magnitud | Ubicación | Región | Profundidad | Fecha y Hora (UTC) |\n"
            "| :--- | :--- | :--- | :--- | :--- |\n"
            f"{rows}"
        )

    if skill_name == "evento_mas_fuerte":
        vent = data.get("ventana", "Últimos 30 días")
        reg = data.get("region_filtro", "Global")
        if not data.get("encontrado"):
            return f"No se registraron eventos sísmicos en la ventana consultada ({vent} • Zona: {reg})."
        top_list = data.get("top_fuertes", [])
        top_rows = "\n".join(
            f"| {idx + 1} | M {e['magnitud']} | {e['lugar']} | {e['region']} | {e['profundidad_km']} km | {e['fecha_utc']} |"
            for idx, e in enumerate(top_list[:5])
        )
        return (
            f"Sismo de Mayor Magnitud Registrado ({vent} • Zona: {reg}):\n\n"
            "| Atributo del Sismo Mayor | Valor Oficial en Base de Datos |\n"
            "| :--- | :--- |\n"
            f"| Magnitud | M {data['magnitud']} |\n"
            f"| Ubicación Exacta | {data['lugar']} |\n"
            f"| Región Geográfica | {data['region']} |\n"
            f"| Fecha y Hora (UTC) | {data['fecha_utc']} |\n"
            f"| Profundidad Hipocentral | {data['profundidad_km']} km |\n"
            f"| Alerta de Tsunami | {'Activa (1)' if data.get('tsunami') else 'Sin alerta (0)'} |\n"
            f"| Índice de Significancia | {data.get('sig', 0)} |\n\n"
            f"Top {min(5, len(top_list))} Sismos Más Fuertes en ese Periodo ({vent}):\n\n"
            "| # | Magnitud | Ubicación | Región | Profundidad | Fecha y Hora (UTC) |\n"
            "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
            f"{top_rows}"
        )

    if skill_name == "comparar_hoy_vs_ayer":
        reg = data.get("region_filtro", "Global")
        diff = data.get("diferencia_sismos", 0)
        signo = "+" if diff > 0 else ""
        return (
            f"Comparativa de Actividad Sísmica: Hoy (Últimas 24h) vs Ayer (24h-48h previas) • Zona: {reg}\n\n"
            f"Conclusión: {data.get('conclusion', '')} ({signo}{diff} sismos, variación del {signo}{data.get('variacion_pct', 0.0)}%).\n\n"
            "| Indicador Comparativo | Hoy (Últimas 24h) | Ayer (24h a 48h atrás) | Variación |\n"
            "| :--- | :--- | :--- | :--- |\n"
            f"| Total de Sismos Registrados | {data.get('hoy_sismos', 0)} sismos | {data.get('ayer_sismos', 0)} sismos | {signo}{diff} ({signo}{data.get('variacion_pct', 0.0)}%) |\n"
            f"| Magnitud Máxima | M {data.get('hoy_mag_max', 0.0)} | M {data.get('ayer_mag_max', 0.0)} | — |\n"
            f"| Magnitud Promedio | M {data.get('hoy_mag_promedio', 0.0)} | M {data.get('ayer_mag_promedio', 0.0)} | — |"
        )

    if skill_name == "kpis":
        vent = data.get("rango", "Catálogo Activo")
        reg = data.get("region_filtro", "Global")
        return (
            f"Resumen Estadístico y KPIs Sísmicos ({vent} • Zona: {reg}):\n\n"
            "| Métrica Sísmica | Valor en Base de Datos | Detalle |\n"
            "| :--- | :--- | :--- |\n"
            f"| Total de Sismos Evaluados | {data.get('total_eventos', 0)} sismos | {vent} ({reg}) |\n"
            f"| Sismos Hoy (Últimas 24 Horas) | {data.get('eventos_ultimas_24h', 0)} sismos | Actividad del último día |\n"
            f"| Magnitud Máxima (Periodo / Hoy) | M {data.get('magnitud_maxima', 0.0)} / M {data.get('magnitud_maxima_24h', 0.0)} | Mayor terremoto registrado |\n"
            f"| Magnitud Promedio (Periodo / Hoy) | M {data.get('magnitud_promedio', 0.0)} / M {data.get('magnitud_promedio_24h', 0.0)} | Media general de magnitud |\n"
            f"| Sismos Fuertes (M >= 5.0) | {data.get('sismos_fuertes_m5', 0)} sismos | Eventos moderados a mayores |\n"
            f"| Sismos Menores a M 3.0 (Periodo / Hoy) | {data.get('sismos_menores_m3', 0)} / {data.get('sismos_menores_m3_24h', 0)} sismos | Microsismicidad |\n"
            f"| Profundidad Media y Máxima | {data.get('profundidad_promedio_km', 0.0)} km (Máx. {data.get('profundidad_maxima_km', 0.0)} km) | Hipocentro terrestre |\n"
            f"| Alertas de Tsunami | {data.get('alertas_tsunami', 0)} alertas | Boletines de vigilancia oceánica |"
        )

    if skill_name == "top_regiones":
        regs = data.get("regiones", [])
        vent = data.get("ventana", "Últimos 30 días")
        lider = data.get("lider_magnitud_en_top")
        if not regs:
            return f"No hay regiones registradas para la ventana consultada ({vent})."
        rows = "\n".join(
            f"| {idx + 1} | {r['region']} | {r['conteo']} sismos ({r.get('porcentaje', 0.0)}%) | M {r['mag_max']} | M {r.get('mag_promedio', 0.0)} | {r.get('prof_promedio_km', 0.0)} km |"
            for idx, r in enumerate(regs)
        )
        lider_txt = (
            f"\n\nDentro de este Top {len(regs)}, la región que tuvo el sismo más fuerte fue {lider['region']} con magnitud M {lider['mag_max']} ({lider['conteo']} sismos registrados)."
            if lider
            else ""
        )
        return (
            f"Ranking de las {len(regs)} Regiones con Mayor Actividad Sísmica ({vent}):\n\n"
            "| # | Región Geográfica | Cantidad de Sismos (% Global) | Magnitud Máxima | Magnitud Promedio | Profundidad Media |\n"
            "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
            f"{rows}"
            f"{lider_txt}"
        )

    if skill_name == "sismos_por_profundidad":
        evs = data.get("eventos", [])
        modo = data.get("modo", "mas_profundos")
        titulo = "Sismos de Mayor Profundidad" if modo == "mas_profundos" else "Sismos Más Superficiales"
        vent = data.get("ventana", "Catálogo Completo")
        reg = data.get("region_filtro", "Global")
        if not evs:
            return f"No se encontraron sismos para analizar profundidad en {vent} ({reg})."
        rows = "\n".join(
            f"| {idx + 1} | {e['profundidad_km']} km | M {e['magnitud']} | {e['lugar']} | {e['region']} | {e['fecha_utc']} |"
            for idx, e in enumerate(evs)
        )
        return (
            f"{titulo} en la Base de Datos ({vent} • Zona: {reg}):\n\n"
            "| # | Profundidad | Magnitud | Ubicación | Región | Fecha y Hora (UTC) |\n"
            "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
            f"{rows}"
        )

    if skill_name == "detectar_anomalias":
        anoms = data.get("anomalias", [])
        tot = data.get("total_anomalias", 0)
        reg = data.get("region_filtro", "Global")
        if not anoms:
            return f"Se analizaron los patrones estadísticos ({reg}) y no se detectaron anomalías activas (total: 0)."
        rows = "\n".join(
            f"| {a['tipo']} | {a['ventana']} | {a['score']} | {a['explicacion']} | {a.get('detectada_utc', '')} |"
            for a in anoms[:8]
        )
        return (
            f"Detección de Anomalías, Enjambres (DBSCAN) y Picos (Z-score) en Base de Datos ({tot} registros • Zona: {reg}):\n\n"
            "| Algoritmo / Tipo | Región o Ventana | Puntaje (Score) | Explicación Técnica | Detectada (UTC) |\n"
            "| :--- | :--- | :--- | :--- | :--- |\n"
            f"{rows}"
        )

    if skill_name == "estado_sistema_y_calidad":
        corridas = data.get("ultimas_corridas", [])
        c_rows = "\n".join(
            f"| {c['dag_id']} | {c['estado']} | {c['fin_utc']} | {c['data_version']} |"
            for c in corridas[:6]
        )
        return (
            "Auditoría en Tiempo Real del Estado del Sistema, Calidad de Datos y Pipelines (PostgreSQL curated):\n\n"
            "| Indicador Operativo y de Calidad | Valor Oficial en Base de Datos |\n"
            "| :--- | :--- |\n"
            f"| Última Actualización de la Base de Datos | {data.get('ultima_actualizacion_utc', 'N/A')} (Frescura: hace {data.get('frescura_minutos', 0.0)} min) |\n"
            f"| Puntaje de Calidad (Última Corrida Airflow) | {data.get('puntaje_calidad_pct', 100.0)}% (Lote: {data.get('ultimo_run_id', 'N/A')}) |\n"
            f"| Sismos Válidos vs Rechazados (Último Lote) | {data.get('validos_ultimo_lote', 0)} válidos / {data.get('rechazados_ultimo_lote', 0)} rechazados |\n"
            f"| Sismos Inválidos Descartados (Última Hora) | {data.get('rechazados_ultima_hora', 0)} registros descartados |\n"
            f"| Sismos Inválidos Descartados Hoy (24h) | {data.get('rechazados_hoy_24h', 0)} registros en {data.get('lotes_evaluados_24h', 0)} lotes |\n"
            f"| Rechazados por Regla de Coordenadas (Lat/Lon) | {data.get('rechazados_coordenadas', 0)} registros |\n"
            f"| ¿Falló algún DAG del Pipeline Hoy? | {'No (0 fallos en las últimas 24h)' if data.get('fallos_dag_hoy', 0) == 0 else str(data.get('fallos_dag_hoy')) + ' fallos registrados'} |\n\n"
            "Últimas Corridas Registradas de los DAGs de Airflow:\n\n"
            "| DAG | Estado | Fecha de Finalización (UTC) | Versión de Datos |\n"
            "| :--- | :--- | :--- | :--- |\n"
            f"{c_rows}"
        )

    if skill_name == "serie_horaria":
        pico = data.get("hora_pico")
        horas = data.get("horas_analizadas", 0)
        puntos = data.get("puntos_recientes", [])
        reg = data.get("region_filtro", "Global")
        if not pico:
            return f"Se analizaron {horas} horas ({reg}) y no se registraron eventos en ese intervalo."
        rows = "\n".join(
            f"| {p['hora_utc']} | {p['conteo']} sismos | M {p['mag_max']} | M {p['mag_promedio']} |"
            for p in puntos[:10]
        )
        return (
            f"Evolución y Pico Horario de Actividad Sísmica ({data.get('rango', 'Últimas 24h')} • Zona: {reg}):\n\n"
            f"Hora pico con más sismos: {pico['hora_utc']} con {pico['conteo']} sismos registrados (Magnitud máxima M {pico['mag_max']}).\n\n"
            "| Hora UTC | Cantidad de Sismos | Magnitud Máxima | Magnitud Promedio |\n"
            "| :--- | :--- | :--- | :--- |\n"
            f"{rows}"
        )

    if skill_name == "estadisticas":
        dist_mag = data.get("distribucion_magnitud", [])
        dist_prof = data.get("distribucion_profundidad", [])
        vent = data.get("ventana", "Catálogo Completo (30 días)")
        reg = data.get("region_filtro", "Global")
        mag_rows = "\n".join(
            f"| {d['rango']} | {d['conteo']} sismos ({d.get('porcentaje', 0.0)}%) | {d.get('prof_media_km', 0.0)} km |"
            for d in dist_mag
        )
        prof_rows = "\n".join(
            f"| {p['capa']} | {p['conteo']} sismos ({p.get('porcentaje', 0.0)}%) | M {p.get('mag_max', 0.0)} |"
            for p in dist_prof
        )
        return (
            f"Análisis Estadístico Completo en Base de Datos ({vent} • Zona: {reg}):\n\n"
            "| Indicador Estadístico | Valor Calculado |\n"
            "| :--- | :--- |\n"
            f"| Total de Sismos Analizados | {data.get('total_eventos', 0)} sismos |\n"
            f"| Sismos Hoy (Últimas 24 Horas) | {data.get('eventos_ultimas_24h', 0)} sismos |\n"
            f"| Magnitud Promedio / Mínima / Máxima | Promedio M {data.get('mag_promedio', 0.0)} (Mín. M {data.get('mag_minima', 0.0)} / Máx. M {data.get('mag_maxima', 0.0)}) |\n"
            f"| Mediana y Percentil 90 de Magnitud | Mediana M {data.get('mag_mediana', 0.0)} • P90 M {data.get('mag_percentil_90', 0.0)} |\n"
            f"| Sismos Fuertes (M >= 5.0) | {data.get('sismos_fuertes_m5', 0)} sismos |\n"
            f"| Sismos Menores a M 3.0 | {data.get('sismos_menores_m3', 0)} sismos |\n"
            f"| Profundidad Mínima / Media / Máxima | {data.get('profundidad_minima_km', 0.0)} km / {data.get('profundidad_promedio_km', 0.0)} km / {data.get('profundidad_maxima_km', 0.0)} km |\n"
            f"| Alertas de Tsunami | {data.get('alertas_tsunami', 0)} alertas |\n\n"
            "Distribución por Escala de Magnitud:\n\n"
            "| Rango de Magnitud | Cantidad (% del Total) | Profundidad Media |\n"
            "| :--- | :--- | :--- |\n"
            f"{mag_rows}\n\n"
            "Distribución por Profundidad Hipocentral:\n\n"
            "| Capa de Profundidad | Cantidad (% del Total) | Magnitud Máxima |\n"
            "| :--- | :--- | :--- |\n"
            f"{prof_rows}"
        )

    if skill_name == "buscar_eventos":
        evs = data.get("eventos", [])
        tot = data.get("total_coincidencias", 0)
        vent = data.get("ventana", "Catálogo Completo")
        filtros = data.get("filtros", {})
        reg = filtros.get("region", "Global")
        only_tsu = filtros.get("only_tsunami", False)
        min_m = filtros.get("min_mag")
        max_m = filtros.get("max_mag")

        filtro_desc = f"Zona: {reg}"
        if min_m is not None:
            filtro_desc += f" • Magnitud >= M {min_m}"
        if max_m is not None:
            filtro_desc += f" • Magnitud <= M {max_m}"
        if only_tsu:
            filtro_desc += " • Con Alerta de Tsunami (tsunami = 1)"

        if not evs:
            return (
                f"La consulta en la base de datos (`curated.events`) finalizó con 0 sismos registrados "
                f"para el criterio solicitado ({vent} • {filtro_desc})."
            )
        rows = "\n".join(
            f"| M {e['magnitud']} | {e['lugar']} | {e['region']} | {e['profundidad_km']} km | {'Activa (1)' if e.get('tsunami') else 'No (0)'} | {e['fecha_utc']} |"
            for e in evs[:10]
        )
        return (
            f"Resultados de Búsqueda en la Base de Datos ({tot} sismos encontrados en {vent} • {filtro_desc} • Máx. M {data.get('magnitud_maxima', 0.0)} • Promedio M {data.get('magnitud_promedio', 0.0)}):\n\n"
            "| Magnitud | Ubicación | Región | Profundidad | Tsunami | Fecha y Hora (UTC) |\n"
            "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
            f"{rows}"
        )

    if skill_name == "consulta_sql_segura":
        if data.get("rechazada_por_seguridad"):
            return f"Consulta no permitida por seguridad: {data.get('motivo')}"
        if data.get("error"):
            return f"Te pido disculpas: {data['error']}"
        filas = data.get("filas", [])
        if not filas:
            return "La consulta a la base de datos se ejecutó correctamente pero no arrojó registros (0 filas)."
        cols = list(filas[0].keys())
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(":---" for _ in cols) + " |"
        body = "\n".join(
            "| " + " | ".join(str(row.get(c, "")) for c in cols) + " |"
            for row in filas[:12]
        )
        return (
            f"Resultado del Análisis Directo en Base de Datos ({data.get('total_filas', 0)} filas):\n\n"
            f"{header}\n{sep}\n{body}"
        )

    # buscar_conocimiento
    kpis_now = data.get("kpis_actuales", {})
    ctx = data.get("conocimiento_vectorial", [])
    ctx_lines = (
        "\n".join(f"- {clean_response_formatting(c['contenido'])}" for c in ctx[:3])
        if ctx
        else "Resumen general del monitoreo sísmico."
    )
    return (
        "Información del Sistema de Monitoreo Sísmico (SismoWatch AI):\n\n"
        f"{ctx_lines}\n\n"
        "| Métrica Actual en Base de Datos | Valor |\n"
        "| :--- | :--- |\n"
        f"| Total de Sismos (30 días) | {kpis_now.get('total_eventos', 0)} sismos |\n"
        f"| Sismos Hoy (Últimas 24 Horas) | {kpis_now.get('eventos_ultimas_24h', 0)} sismos |\n"
        f"| Magnitud Máxima (30d / Hoy) | M {kpis_now.get('magnitud_maxima', 0.0)} / M {kpis_now.get('magnitud_maxima_24h', 0.0)} |"
    )


def _try_llm_draft(
    question: str,
    skill_name: str,
    skill_data: Dict[str, Any],
    required_numbers: List[float],
) -> Optional[Tuple[str, str, int, int, float]]:
    """
    Para síntesis de conocimiento vectorial, aplica `AGENT_SYSTEM_PROMPT` sobre Groq o sobre el
    modelo local Ollama (`qwen2.5:3b`) verificando siempre las cifras contra SQL.
    Para consultas tabulares directas a la BD, entrega la tabla determinista exacta al 100%.
    """
    if skill_name in {
        "ultimo_sismo_detallado",
        "ultimos_sismos",
        "evento_mas_fuerte",
        "comparar_hoy_vs_ayer",
        "estadisticas",
        "top_regiones",
        "buscar_eventos",
        "sismos_por_profundidad",
        "serie_horaria",
        "detectar_anomalias",
        "estado_sistema_y_calidad",
        "consulta_sql_segura",
        "kpis",
    }:
        return None

    s = get_settings()
    user_prompt = (
        f"Pregunta del usuario: {question}\n"
        f"DATOS_OFICIALES_POSTGRES_CURATED:\n{json.dumps(skill_data, ensure_ascii=False)}"
    )

    if s.groq_api_key and not s.groq_api_key.startswith("your_groq") and not s.groq_api_key.startswith("CHANGE_ME"):
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {s.groq_api_key}", "Content-Type": "application/json"},
                json={
                    "model": s.groq_model,
                    "messages": [
                        {"role": "system", "content": AGENT_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 500,
                },
                timeout=(3.0, 10.0),
            )
            if resp.status_code == 200:
                body = resp.json()
                raw_text = body["choices"][0]["message"]["content"].strip()
                text = clean_response_formatting(raw_text)
                if "|" in text and verify_numbers_in_response(text, required_numbers):
                    usage = body.get("usage", {})
                    t_in = int(usage.get("prompt_tokens", len(user_prompt) // 4))
                    t_out = int(usage.get("completion_tokens", len(text) // 4))
                    cost = (t_in * 0.59 + t_out * 0.79) / 1_000_000.0
                    return text, "groq", t_in, t_out, cost
        except requests.RequestException:
            pass

    # Respaldo con modelo local Ollama (Qwen 2.5) bajo AGENT_SYSTEM_PROMPT
    try:
        resp = requests.post(
            f"{s.ollama_url}/api/chat",
            json={
                "model": s.ollama_fallback_model,
                "messages": [
                    {"role": "system", "content": AGENT_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                "stream": False,
                "options": {"temperature": 0.1},
            },
            timeout=(2.0, 8.0),
        )
        if resp.status_code == 200:
            raw_text = (resp.json().get("message", {}) or {}).get("content", "").strip()
            text = clean_response_formatting(raw_text)
            if "|" in text and verify_numbers_in_response(text, required_numbers):
                t_in = len(user_prompt) // 4
                t_out = len(text) // 4
                return text, "ollama", t_in, t_out, 0.0
    except requests.RequestException:
        pass

    return None


def _log_chat_and_ai_run(
    username: str,
    question: str,
    level: int,
    skill_used: Optional[str],
    provider: str,
    fallback: bool,
    latency_ms: float,
    tokens_in: int = 0,
    tokens_out: int = 0,
    cost_usd: float = 0.0,
) -> None:
    conn = get_db_conn(role="api_rw")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.chat_sessions (
                        username, pregunta, nivel_resolucion, skill_usada, proveedor, fallback, latencia_ms, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
                    """,
                    (username, question[:500], level, skill_used, provider, fallback, round(latency_ms, 2)),
                )
    except Exception:
        pass
    finally:
        conn.close()


def process_agent_chat(question: str, username: str = "viewer", role: str = "viewer") -> ChatResponse:
    """
    Punto de entrada principal de SismoWatch AI.
    Permite hacer consultas completas en tiempo real y análisis estadísticos sobre PostgreSQL (`curated`),
    y rechaza cualquier texto ajeno al dominio sísmico.
    """
    t0 = time.perf_counter()
    norm_q = normalize_question(question)
    data_version = get_current_data_version()

    # 1. Intents conversacionales y FAQ conceptual
    l0_match = _match_level_0_intent_or_faq(norm_q, role=role)
    if l0_match is not None:
        skill_tag, answer_txt = l0_match
        latency_ms = (time.perf_counter() - t0) * 1000.0
        _log_chat_and_ai_run(username, question, 0, skill_tag, "python_intent_faq", False, latency_ms)
        return ChatResponse(
            answer=answer_txt,
            resolution_level=0,
            level_label="Respuesta Directa",
            skill_used=skill_tag,
            provider="python_intent_faq",
            fallback=False,
            cached=False,
            data_version=data_version,
            supporting_data=None,
            latency_ms=round(latency_ms, 2),
        )

    # 2. Caché ligada a data_version
    cached_hit = _lookup_level_1_cache(norm_q, data_version, role=role)
    if cached_hit is not None:
        payload = cached_hit["payload"]
        cache_type = cached_hit["cache_type"]
        latency_ms = (time.perf_counter() - t0) * 1000.0
        _log_chat_and_ai_run(username, question, 1, payload.get("skill_used"), f"cache_{cache_type}", False, latency_ms)
        return ChatResponse(
            answer=clean_response_formatting(str(payload.get("answer", ""))),
            resolution_level=1,
            level_label="Consulta Sincronizada",
            skill_used=payload.get("skill_used"),
            provider=f"cache_{cache_type}",
            fallback=False,
            cached=True,
            data_version=data_version,
            supporting_data=payload.get("supporting_data") if role == "admin" else None,
            latency_ms=round(latency_ms, 2),
        )

    # 3. Ejecución de Consulta a Base de Datos + Validación de Dominio
    skill_name, skill_data, req_numbers = _select_and_execute_skill(question, norm_q)

    if skill_name == "no_validado":
        latency_ms = (time.perf_counter() - t0) * 1000.0
        reject_msg = (
            "No entiendo lo solicitado. Tu mensaje no corresponde a información sísmica ni a estadísticas monitoreadas en SismoWatch.\n\n"
            "Puedes consultarme cualquier dato en tiempo real de la base de datos sísmica, por ejemplo:\n\n"
            "| Categoría | Ejemplo de Consulta |\n"
            "| :--- | :--- |\n"
            "| Tiempo Real | ¿Cuál fue el último sismo registrado en el sistema? |\n"
            "| Magnitudes y Tsunami | ¿Cuál ha sido el sismo más fuerte del día de hoy? |\n"
            "| Regiones Activas | ¿Cuáles son las 3 regiones con más actividad sísmica hoy? |\n"
            "| Comparativas y KPIs | Comparado con ayer, ¿hoy ha temblado más o menos? |\n"
            "| Anomalías y Sistema | ¿Hay algún enjambre sísmico activo o a qué hora se actualizó la base de datos? |"
        )
        _log_chat_and_ai_run(username, question, 0, "rag_domain_guard", "domain_validator", False, latency_ms)
        return ChatResponse(
            answer=reject_msg,
            resolution_level=0,
            level_label="Consulta No Reconocida",
            skill_used="rag_domain_guard",
            provider="domain_validator",
            fallback=False,
            cached=False,
            data_version=data_version,
            supporting_data=None,
            latency_ms=round(latency_ms, 2),
        )

    if skill_name == "consulta_sql_segura" and skill_data.get("rechazada_por_seguridad"):
        ans = _format_deterministic_skill_answer(skill_name, skill_data)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        return ChatResponse(
            answer=ans,
            resolution_level=3,
            level_label="Seguridad de Datos",
            skill_used=skill_name,
            provider="sql_security_guard",
            fallback=True,
            cached=False,
            data_version=data_version,
            supporting_data=None,
            latency_ms=round(latency_ms, 2),
        )

    llm_draft = _try_llm_draft(question, skill_name, skill_data, req_numbers)
    if llm_draft is not None:
        ans_txt, provider, t_in, t_out, cost_usd = llm_draft
        latency_ms = (time.perf_counter() - t0) * 1000.0
        cache_payload = {
            "answer": ans_txt,
            "skill_used": skill_name,
            "supporting_data": skill_data,
        }
        _store_in_cache(norm_q, data_version, cache_payload, role=role)
        _log_chat_and_ai_run(
            username,
            question,
            2,
            skill_name,
            provider,
            provider != "groq",
            latency_ms,
            tokens_in=t_in,
            tokens_out=t_out,
            cost_usd=cost_usd,
        )
        return ChatResponse(
            answer=ans_txt,
            resolution_level=2,
            level_label="Análisis Sísmico",
            skill_used=skill_name,
            provider=provider,
            fallback=(provider != "groq"),
            cached=False,
            data_version=data_version,
            supporting_data=skill_data if role == "admin" else None,
            latency_ms=round(latency_ms, 2),
        )

    fallback_ans = _format_deterministic_skill_answer(skill_name, skill_data)
    latency_ms = (time.perf_counter() - t0) * 1000.0
    cache_payload = {
        "answer": fallback_ans,
        "skill_used": skill_name,
        "supporting_data": skill_data,
    }
    _store_in_cache(norm_q, data_version, cache_payload, role=role)
    _log_chat_and_ai_run(username, question, 3, skill_name, "database_analytics", True, latency_ms)
    return ChatResponse(
        answer=fallback_ans,
        resolution_level=3,
        level_label="Análisis de Base de Datos",
        skill_used=skill_name,
        provider="database_analytics",
        fallback=True,
        cached=False,
        data_version=data_version,
        supporting_data=skill_data if role == "admin" else None,
        latency_ms=round(latency_ms, 2),
    )


def regenerate_daily_report_via_api() -> AdminActionResponse:
    """Regenera el boletín ejecutivo sísmico diario."""
    s = get_settings()
    kpi_summary = skills.kpis("24h", hours=24, window_label="Últimas 24 horas")
    top_regs = skills.top_regiones(5, "30d").get("regiones", [])
    strongest = skills.evento_mas_fuerte("30d")
    anoms = skills.detectar_anomalias("72h").get("anomalias", [])
    today_str = datetime.now(timezone.utc).date().isoformat()

    tot = kpi_summary.get("total_eventos", 0)
    m_max = kpi_summary.get("magnitud_maxima", 0.0)
    m_avg = kpi_summary.get("magnitud_promedio", 0.0)
    tsu = kpi_summary.get("alertas_tsunami", 0)

    zonas_list = [
        f"{r['region']}: {r['conteo']} sismos registrados (magnitud máx. M {r['mag_max']})"
        for r in top_regs
    ] or ["Sin zonas con actividad significativa registrada en el corte."]

    destacados_list = (
        [
            f"M {strongest['magnitud']} en {strongest['lugar']} ({strongest['fecha_utc']}, prof. {strongest['profundidad_km']} km)"
        ]
        if strongest.get("encontrado")
        else ["Sin eventos destacados en el corte actual."]
    )

    anom_txt = (
        "; ".join(f"{a['tipo']} en {a['ventana']} (score {a['score']}): {a['explicacion']}" for a in anoms[:3])
        if anoms
        else "Actividad dentro de parámetros basales."
    )

    report_json = {
        "titulo": f"Boletín Ejecutivo Sísmico Diario SismoWatch — {today_str}",
        "resumen_ejecutivo": (
            f"Durante el corte del {today_str}, SismoWatch registró {tot} eventos sísmicos "
            f"en las últimas 24 horas. La magnitud máxima alcanzó M {m_max} "
            f"(promedio M {m_avg}), con {tsu} eventos asociados a vigilancia de tsunami."
        ),
        "zonas_mas_activas": zonas_list,
        "eventos_destacados": destacados_list,
        "analisis_anomalias": anom_txt,
        "recomendaciones": "Mantener monitoreo continuo sobre las regiones con mayor densidad de réplicas.",
        "cifras_oficiales": {
            "total_eventos": tot,
            "mag_maxima": m_max,
            "mag_promedio": m_avg,
            "eventos_tsunami": tsu,
        },
    }

    provider = "database_summary"
    fallback_flag = False

    texto_md = (
        f"# {report_json['titulo']}\n\n"
        f"## Resumen Ejecutivo\n{report_json['resumen_ejecutivo']}\n\n"
        f"## Zonas Más Activas\n" + "\n".join(f"- {z}" for z in zonas_list) + "\n\n"
        f"## Eventos Destacados\n" + "\n".join(f"- {e}" for e in destacados_list) + "\n\n"
        f"## Análisis de Anomalías\n{anom_txt}\n\n"
        f"## Recomendaciones\n{report_json['recomendaciones']}"
    )

    conn = get_db_conn(role="ai")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.daily_reports (
                        fecha, json_validado, texto, proveedor, fallback, version_prompt, created_at, updated_at
                    )
                    VALUES (%s, %s::jsonb, %s, %s, %s, 'daily_report_v1', NOW(), NOW())
                    ON CONFLICT (fecha) DO UPDATE SET
                        json_validado = EXCLUDED.json_validado,
                        texto = EXCLUDED.texto,
                        proveedor = EXCLUDED.proveedor,
                        fallback = EXCLUDED.fallback,
                        updated_at = NOW();
                    """,
                    (today_str, json.dumps(report_json, ensure_ascii=False), texto_md, provider, fallback_flag),
                )
    finally:
        conn.close()

    airflow_triggered = False
    try:
        auth = (s.airflow_api_user, s.airflow_api_password)
        r = requests.post(
            f"{s.airflow_api_url}/dags/usgs_ai_daily/dagRuns",
            auth=auth,
            json={"conf": {"triggered_by": "admin_api"}},
            timeout=5,
        )
        airflow_triggered = r.status_code in (200, 201)
    except requests.RequestException:
        pass

    return AdminActionResponse(
        status="REGENERATED",
        message=f"Boletín diario del {today_str} actualizado exitosamente.",
        details={
            "fecha": today_str,
            "provider": provider,
            "fallback": fallback_flag,
            "total_eventos": tot,
            "mag_maxima": m_max,
            "airflow_dag_triggered": airflow_triggered,
        },
    )
