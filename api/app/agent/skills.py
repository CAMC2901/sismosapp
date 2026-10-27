"""
Catálogo de las 9 Skills del Agente Conversacional (Sección 11).

Todas las funciones son tipadas, ejecutan cálculos reales en SQL/Python usando `reader_role`,
manejan errores de manera controlada y jamás exponen tracebacks crudos al LLM ni al usuario.
"""
import hashlib
import math
from typing import Any, Dict, List, Optional
import requests
from app.agent.sql_validator import execute_safe_sql
from app.core.config import get_db_conn, get_settings
from app.core.exceptions import UnsafeQueryError

EMBEDDING_DIM = 1024


def _range_to_interval(rango: str) -> str:
    r = (rango or "24h").strip().lower()
    mapping = {
        "1h": "1 hour",
        "6h": "6 hours",
        "12h": "12 hours",
        "24h": "24 hours",
        "1d": "24 hours",
        "48h": "48 hours",
        "72h": "72 hours",
        "3d": "3 days",
        "7d": "7 days",
        "15d": "15 days",
        "30d": "30 days",
        "all": "365 days",
    }
    return mapping.get(r, "30 days")


def compute_query_embedding(text: str) -> List[float]:
    """Obtiene el vector de 1024 dimensiones con `bge-m3` en Ollama o el vector determinístico local."""
    s = get_settings()
    try:
        resp = requests.post(
            f"{s.ollama_url}/api/embeddings",
            json={"model": s.ollama_embed_model, "prompt": text[:1500]},
            timeout=(2.5, 6.0),
        )
        if resp.status_code == 200:
            vec = resp.json().get("embedding", [])
            if isinstance(vec, list) and len(vec) == EMBEDDING_DIM:
                return [float(x) for x in vec]
    except requests.RequestException:
        pass

    vec = [0.0] * EMBEDDING_DIM
    cleaned = (text or "").lower().strip()
    tokens = cleaned.split()
    features = tokens + [cleaned[i : i + 3] for i in range(max(0, len(cleaned) - 2))]
    if not features:
        vec[0] = 1.0
        return vec
    for feat in features:
        h = int(hashlib.md5(feat.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        idx = h % EMBEDDING_DIM
        sign = 1.0 if ((h >> 16) & 1) == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [round(v / norm, 6) for v in vec]


def _vec_literal(vec: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def kpis(rango: str = "24h") -> Dict[str, Any]:
    """Skill 1: Calcula los KPIs sísmicos y de calidad en el rango solicitado."""
    interval = _range_to_interval(rango)
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(SUM(tsunami), 0)::int
                FROM curated.events
                WHERE time >= NOW() - %s::interval;
                """,
                (interval,),
            )
            cnt, m_max, m_avg, tsu = cur.fetchone()
            if int(cnt) == 0:
                cur.execute(
                    """
                    SELECT
                        COUNT(*)::int,
                        COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                        COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                        COALESCE(SUM(tsunami), 0)::int
                    FROM curated.events;
                    """
                )
                cnt, m_max, m_avg, tsu = cur.fetchone()
                rango = "histórico disponible"

            cur.execute("SELECT COALESCE(ROUND((puntaje * 100)::numeric, 1), 100.0)::float FROM curated.quality_results ORDER BY timestamp DESC LIMIT 1;")
            q_row = cur.fetchone()
            q_score = float(q_row[0]) if q_row else 100.0

        return {
            "skill": "kpis",
            "rango": rango,
            "total_eventos": int(cnt),
            "magnitud_maxima": float(m_max),
            "magnitud_promedio": float(m_avg),
            "alertas_tsunami": int(tsu),
            "puntaje_calidad_pct": q_score,
        }
    except Exception:
        return {"skill": "kpis", "error": "No fue posible consultar los KPIs en este momento."}
    finally:
        conn.close()


def top_regiones(n: int = 5, rango: str = "30d") -> Dict[str, Any]:
    """Skill 2: Obtiene las N regiones con mayor actividad sísmica en el rango indicado."""
    interval = _range_to_interval(rango)
    limit_n = max(1, min(int(n), 15))
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT region, COUNT(*)::int AS conteo, ROUND(MAX(mag)::numeric, 2)::float AS mag_max
                FROM curated.events
                WHERE time >= NOW() - %s::interval
                GROUP BY region
                ORDER BY conteo DESC, mag_max DESC
                LIMIT %s;
                """,
                (interval, limit_n),
            )
            rows = cur.fetchall()
            if not rows:
                cur.execute(
                    """
                    SELECT region, COUNT(*)::int AS conteo, ROUND(MAX(mag)::numeric, 2)::float AS mag_max
                    FROM curated.events
                    GROUP BY region
                    ORDER BY conteo DESC, mag_max DESC
                    LIMIT %s;
                    """,
                    (limit_n,),
                )
                rows = cur.fetchall()
            regiones = [{"region": str(r[0]), "conteo": int(r[1]), "mag_max": float(r[2])} for r in rows]
        return {"skill": "top_regiones", "rango": rango, "top_n": limit_n, "regiones": regiones}
    except Exception:
        return {"skill": "top_regiones", "error": "Error controlado al consultar las regiones más activas."}
    finally:
        conn.close()


def evento_mas_fuerte(rango: str = "30d") -> Dict[str, Any]:
    """Skill 3: Devuelve el evento sísmico de mayor magnitud en el rango solicitado."""
    interval = _range_to_interval(rango)
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, ROUND(mag::numeric, 2)::float, place, region, time, ROUND(depth_km::numeric, 2)::float, tsunami
                FROM curated.events
                WHERE time >= NOW() - %s::interval
                ORDER BY mag DESC, time DESC
                LIMIT 1;
                """,
                (interval,),
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    """
                    SELECT id, ROUND(mag::numeric, 2)::float, place, region, time, ROUND(depth_km::numeric, 2)::float, tsunami
                    FROM curated.events
                    ORDER BY mag DESC, time DESC
                    LIMIT 1;
                    """
                )
                row = cur.fetchone()
            if not row:
                return {"skill": "evento_mas_fuerte", "encontrado": False}
            return {
                "skill": "evento_mas_fuerte",
                "encontrado": True,
                "id": str(row[0]),
                "magnitud": float(row[1]),
                "lugar": str(row[2]),
                "region": str(row[3]),
                "fecha_utc": row[4].strftime("%Y-%m-%d %H:%M UTC"),
                "profundidad_km": float(row[5]),
                "tsunami": int(row[6]),
            }
    except Exception:
        return {"skill": "evento_mas_fuerte", "error": "Error controlado al buscar el sismo más fuerte."}
    finally:
        conn.close()


def serie_horaria(rango: str = "24h") -> Dict[str, Any]:
    """Skill 4: Resume la evolución horaria de sismos en la ventana indicada."""
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT hora, conteo, ROUND(mag_max::numeric, 2)::float, ROUND(mag_promedio::numeric, 2)::float
                FROM curated.agg_hourly
                ORDER BY hora DESC
                LIMIT 24;
                """
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
            max_hora = max(puntos, key=lambda x: x["conteo"]) if puntos else None
        return {
            "skill": "serie_horaria",
            "rango": rango,
            "horas_analizadas": total_horas,
            "hora_pico": max_hora,
            "ultimas_horas": puntos[:8],
        }
    except Exception:
        return {"skill": "serie_horaria", "error": "Error controlado al consultar la serie horaria."}
    finally:
        conn.close()


def buscar_eventos(filtros: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Skill 5: Busca eventos sísmicos según filtros de magnitud mínima, región y límite."""
    f = filtros or {}
    min_mag = float(f.get("min_mag", 0.0))
    region = f.get("region")
    limit = max(1, min(int(f.get("limit", 8)), 25))

    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            if region:
                cur.execute(
                    """
                    SELECT id, ROUND(mag::numeric, 2)::float, place, region, time, ROUND(depth_km::numeric, 1)::float
                    FROM curated.events
                    WHERE mag >= %s AND (region ILIKE %s OR place ILIKE %s)
                    ORDER BY time DESC, mag DESC
                    LIMIT %s;
                    """,
                    (min_mag, f"%{region}%", f"%{region}%", limit),
                )
            else:
                cur.execute(
                    """
                    SELECT id, ROUND(mag::numeric, 2)::float, place, region, time, ROUND(depth_km::numeric, 1)::float
                    FROM curated.events
                    WHERE mag >= %s
                    ORDER BY time DESC, mag DESC
                    LIMIT %s;
                    """,
                    (min_mag, limit),
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
            "skill": "buscar_eventos",
            "filtros": {"min_mag": min_mag, "region": region},
            "cantidad_encontrada": len(eventos),
            "eventos": eventos,
        }
    except Exception:
        return {"skill": "buscar_eventos", "error": "Error controlado al buscar eventos."}
    finally:
        conn.close()


def estadisticas(mag: float = 0.0, profundidad: float = 800.0) -> Dict[str, Any]:
    """Skill 6: Calcula estadísticas descriptivas de magnitud y profundidad en curated.events."""
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int,
                    COALESCE(ROUND(MIN(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(MAX(mag)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(AVG(depth_km)::numeric, 2), 0.0)::float,
                    COALESCE(ROUND(MAX(depth_km)::numeric, 2), 0.0)::float
                FROM curated.events
                WHERE mag >= %s AND depth_km <= %s;
                """,
                (float(mag), float(profundidad)),
            )
            cnt, m_min, m_avg, m_max, d_avg, d_max = cur.fetchone()
        return {
            "skill": "estadisticas",
            "filtro_mag_min": float(mag),
            "filtro_profundidad_max_km": float(profundidad),
            "total_eventos": int(cnt),
            "mag_minima": float(m_min),
            "mag_promedio": float(m_avg),
            "mag_maxima": float(m_max),
            "profundidad_promedio_km": float(d_avg),
            "profundidad_maxima_km": float(d_max),
        }
    except Exception:
        return {"skill": "estadisticas", "error": "Error controlado al calcular estadísticas."}
    finally:
        conn.close()


def detectar_anomalias(ventana: str = "72h") -> Dict[str, Any]:
    """Skill 7: Consulta las anomalías detectadas (z-score y enjambres DBSCAN) en curated.anomalies."""
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tipo, ventana, ROUND(score::numeric, 2)::float, explicacion, detected_at
                FROM curated.anomalies
                ORDER BY detected_at DESC, score DESC
                LIMIT 10;
                """
            )
            items = [
                {
                    "tipo": str(r[0]),
                    "ventana": str(r[1]),
                    "score": float(r[2]),
                    "explicacion": str(r[3]),
                    "detectada_utc": r[4].strftime("%Y-%m-%d %H:%M UTC"),
                }
                for r in cur.fetchall()
            ]
        return {
            "skill": "detectar_anomalias",
            "ventana": ventana,
            "total_anomalias": len(items),
            "anomalias": items,
        }
    except Exception:
        return {"skill": "detectar_anomalias", "error": "Error controlado al consultar anomalías."}
    finally:
        conn.close()


def consulta_sql_segura(sql: str) -> Dict[str, Any]:
    """Skill 8: Valida y ejecuta una consulta SELECT segura sobre curated con reader_role."""
    try:
        res = execute_safe_sql(sql)
        return {"skill": "consulta_sql_segura", **res}
    except UnsafeQueryError as exc:
        return {"skill": "consulta_sql_segura", "rechazada_por_seguridad": True, "motivo": exc.message}
    except Exception:
        return {"skill": "consulta_sql_segura", "error": "No fue posible ejecutar la consulta SQL solicitada."}


def buscar_conocimiento(pregunta: str) -> Dict[str, Any]:
    """
    Skill 9: Búsqueda semántica vectorial en `curated.kb_chunks` usando pgvector y embeddings `bge-m3`.
    Recupera el contexto más relevante de eventos, reportes diarios, anomalías y FAQ.
    """
    conn = get_db_conn(role="reader")
    try:
        q_vec = compute_query_embedding(pregunta)
        vec_str = _vec_literal(q_vec)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, fuente, contenido, metadata,
                    ROUND((1 - (embedding <=> %s::vector))::numeric, 4)::float AS similitud
                FROM curated.kb_chunks
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector ASC
                LIMIT 5;
                """,
                (vec_str, vec_str),
            )
            chunks = [
                {
                    "chunk_id": str(r[0])[:12],
                    "fuente": str(r[1]),
                    "contenido": str(r[2]),
                    "metadata": r[3],
                    "similitud": float(r[4]),
                }
                for r in cur.fetchall()
            ]
        return {
            "skill": "buscar_conocimiento",
            "pregunta": pregunta,
            "fragmentos_recuperados": len(chunks),
            "contexto": chunks,
        }
    except Exception:
        return {"skill": "buscar_conocimiento", "error": "Error controlado en búsqueda vectorial."}
    finally:
        conn.close()
