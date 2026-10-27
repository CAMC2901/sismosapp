"""
Indexación vectorial en pgvector (`curated.kb_chunks`) con embeddings `bge-m3` (Sección 11).

Reglas de chunking:
- Chunking exacto por unidad natural, sin ventanas deslizantes ni solapamiento:
  1) 1 evento sísmico (texto plantilla con magnitud, lugar sanitizado, fecha, profundidad)
  2) 1 sección de reporte diario
  3) 1 alerta de anomalía con su explicación
  4) 1 par pregunta-respuesta de FAQ
- Cada chunk lleva un ID determinístico SHA-256 (`hash(fuente + ':' + contenido)`) y metadata JSONB
  (`fecha`, `magnitud`, `tipo`, `id`), haciendo que re-embeber sea 100% idempotente.
"""
import hashlib
import json
import math
import time
from typing import Any, Dict, List, Optional
import requests
from ai.llm_client import record_ai_run
from common.config import get_config, get_db_connection
from common.logging_utils import get_structured_logger, log_event
from common.sanitization import sanitize_place

logger = get_structured_logger("sismowatch.ai.embeddings")
EMBEDDING_DIM = 1024


def compute_deterministic_chunk_id(fuente: str, contenido: str) -> str:
    """Genera el ID determinístico SHA-256 a partir de `fuente + contenido`."""
    raw = f"{fuente.strip().lower()}::{contenido.strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _deterministic_fallback_vector(text: str, dim: int = EMBEDDING_DIM) -> List[float]:
    """
    Genera un vector unitario determinístico de `dim` dimensiones mediante feature hashing
    de palabras y trigramas cuando el servidor Ollama aún está descargando `bge-m3` o no responde.
    """
    vec = [0.0] * dim
    cleaned = (text or "").lower().strip()
    tokens = cleaned.split()
    features = tokens + [cleaned[i : i + 3] for i in range(max(0, len(cleaned) - 2))]
    if not features:
        vec[0] = 1.0
        return vec

    for feat in features:
        h = int(hashlib.md5(feat.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if ((h >> 16) & 1) == 0 else -1.0
        vec[idx] += sign

    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [round(v / norm, 6) for v in vec]


def get_embedding_vector(
    text: str,
    run_id: str = "system",
    db_role: str = "ai",
    record_telemetry: bool = False,
) -> List[float]:
    """
    Obtiene el vector de 1024 dimensiones desde Ollama (`bge-m3`).
    Si Ollama falla o excede el timeout, cae al vector determinístico local sin romper el flujo.
    """
    cfg = get_config()
    base_url = cfg.ollama_url.rstrip("/")
    model = cfg.ollama_embed_model
    t0 = time.perf_counter()

    try:
        resp = requests.post(
            f"{base_url}/api/embeddings",
            json={"model": model, "prompt": text[:2000]},
            timeout=(3.0, 10.0),
        )
        latency_ms = (time.perf_counter() - t0) * 1000.0
        if resp.status_code == 200:
            vec = resp.json().get("embedding", [])
            if isinstance(vec, list) and len(vec) == EMBEDDING_DIM:
                if record_telemetry:
                    record_ai_run(
                        tarea="embedding_bge_m3",
                        modelo=model,
                        proveedor="ollama",
                        tokens_entrada=max(1, len(text) // 4),
                        tokens_salida=0,
                        latencia_ms=latency_ms,
                        costo_estimado=0.0,
                        exito=True,
                        fallback=False,
                        run_id=run_id,
                        db_role=db_role,
                    )
                return [float(x) for x in vec]
    except requests.RequestException:
        pass

    latency_ms = (time.perf_counter() - t0) * 1000.0
    if record_telemetry:
        record_ai_run(
            tarea="embedding_bge_m3",
            modelo="deterministic-hash-1024",
            proveedor="template_fallback",
            tokens_entrada=max(1, len(text) // 4),
            tokens_salida=0,
            latencia_ms=latency_ms,
            costo_estimado=0.0,
            exito=True,
            fallback=True,
            run_id=run_id,
            db_role=db_role,
        )
    return _deterministic_fallback_vector(text, dim=EMBEDDING_DIM)


def _vector_to_pg_literal(vec: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def index_knowledge_base_chunks(
    max_events: int = 80,
    run_id: str = "daily_ai",
) -> Dict[str, int]:
    """
    Construye e indexa de manera idempotente en `curated.kb_chunks` las 4 fuentes naturales:
    1. Eventos destacados/recientes (`event`)
    2. Secciones de reportes diarios (`daily_report`)
    3. Alertas de anomalías (`anomaly`)
    4. Pares pregunta-respuesta de la FAQ (`faq`)
    """
    conn = get_db_connection(role="ai")
    chunks_to_upsert: List[Dict[str, Any]] = []

    try:
        with conn.cursor() as cur:
            # 1. Eventos sísmicos (1 chunk por evento)
            cur.execute(
                """
                SELECT id, ROUND(mag::numeric, 1)::float, place, time, ROUND(depth_km::numeric, 1)::float, region, tsunami
                FROM curated.events
                ORDER BY time DESC, mag DESC
                LIMIT %s;
                """,
                (max_events,),
            )
            for eid, mag, place, ev_time, depth_km, region, tsunami in cur.fetchall():
                safe_place = sanitize_place(place)
                dt_iso = ev_time.strftime("%Y-%m-%d %H:%M UTC")
                tsu_txt = "Con vigilancia de tsunami." if int(tsunami or 0) == 1 else "Sin alerta de tsunami."
                content = (
                    f"Evento sísmico {eid}: magnitud {mag} en {safe_place} (región {region}), "
                    f"registrado el {dt_iso} a una profundidad de {depth_km} km. {tsu_txt}"
                )
                chunks_to_upsert.append(
                    {
                        "id": compute_deterministic_chunk_id("event", content),
                        "fuente": "event",
                        "contenido": content,
                        "metadata": {
                            "id": str(eid),
                            "fecha": ev_time.strftime("%Y-%m-%d"),
                            "magnitud": float(mag),
                            "profundidad_km": float(depth_km),
                            "region": sanitize_place(region),
                            "tipo": "event",
                        },
                    }
                )

            # 2. Secciones de reporte diario (1 chunk por sección del reporte más reciente)
            cur.execute(
                """
                SELECT fecha, json_validado
                FROM curated.daily_reports
                ORDER BY fecha DESC
                LIMIT 3;
                """
            )
            for fecha_rep, rep_json in cur.fetchall():
                rep_dict = rep_json if isinstance(rep_json, dict) else json.loads(rep_json)
                fecha_str = fecha_rep.isoformat()
                sections = [
                    ("resumen_ejecutivo", rep_dict.get("resumen_ejecutivo", "")),
                    ("zonas_mas_activas", "; ".join(rep_dict.get("zonas_mas_activas", []))),
                    ("eventos_destacados", "; ".join(rep_dict.get("eventos_destacados", []))),
                    ("analisis_anomalias", rep_dict.get("analisis_anomalias", "")),
                ]
                for sec_name, sec_body in sections:
                    if not sec_body:
                        continue
                    content = f"Reporte diario ({fecha_str}) — sección {sec_name}: {sec_body}"
                    chunks_to_upsert.append(
                        {
                            "id": compute_deterministic_chunk_id("daily_report", content),
                            "fuente": "daily_report",
                            "contenido": content,
                            "metadata": {
                                "id": f"{fecha_str}_{sec_name}",
                                "fecha": fecha_str,
                                "magnitud": rep_dict.get("cifras_oficiales", {}).get("mag_maxima", 0.0),
                                "tipo": "daily_report",
                                "seccion": sec_name,
                            },
                        }
                    )

            # 3. Alertas de anomalías (1 chunk por anomalía)
            cur.execute(
                """
                SELECT id, tipo, ventana, score, explicacion, detected_at
                FROM curated.anomalies
                ORDER BY detected_at DESC
                LIMIT 20;
                """
            )
            for aid, tipo, ventana, score, explicacion, det_at in cur.fetchall():
                content = f"Anomalía sísmica ({tipo}) en {ventana} [score {score}]: {explicacion}"
                chunks_to_upsert.append(
                    {
                        "id": compute_deterministic_chunk_id("anomaly", content),
                        "fuente": "anomaly",
                        "contenido": content,
                        "metadata": {
                            "id": str(aid),
                            "fecha": det_at.strftime("%Y-%m-%d"),
                            "magnitud": float(score),
                            "tipo": str(tipo),
                            "ventana": ventana,
                        },
                    }
                )

            # 4. Pares pregunta-respuesta de FAQ (1 chunk por FAQ)
            cur.execute(
                """
                SELECT id, categoria, pregunta_canonica, respuesta_plantilla
                FROM curated.faq
                ORDER BY orden ASC;
                """
            )
            for fid, cat, preg, resp_txt in cur.fetchall():
                content = f"Pregunta frecuente [{cat}] {preg} Respuesta: {resp_txt}"
                chunks_to_upsert.append(
                    {
                        "id": compute_deterministic_chunk_id("faq", content),
                        "fuente": "faq",
                        "contenido": content,
                        "metadata": {
                            "id": str(fid),
                            "fecha": "static",
                            "magnitud": 0.0,
                            "tipo": "faq",
                            "categoria": cat,
                        },
                    }
                )

            # Upsert idempotente en curated.kb_chunks
            counts_by_source: Dict[str, int] = {"event": 0, "daily_report": 0, "anomaly": 0, "faq": 0}
            with conn:
                for idx, item in enumerate(chunks_to_upsert):
                    # Registrar telemetría representativa en el primer chunk para no saturar ai_runs
                    vec = get_embedding_vector(
                        item["contenido"],
                        run_id=run_id,
                        db_role="ai",
                        record_telemetry=(idx == 0),
                    )
                    cur.execute(
                        """
                        INSERT INTO curated.kb_chunks (id, fuente, contenido, embedding, metadata, updated_at)
                        VALUES (%s, %s, %s, %s::vector, %s::jsonb, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            fuente = EXCLUDED.fuente,
                            contenido = EXCLUDED.contenido,
                            embedding = EXCLUDED.embedding,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW();
                        """,
                        (
                            item["id"],
                            item["fuente"],
                            item["contenido"],
                            _vector_to_pg_literal(vec),
                            json.dumps(item["metadata"], ensure_ascii=False),
                        ),
                    )
                    counts_by_source[item["fuente"]] = counts_by_source.get(item["fuente"], 0) + 1

        summary = {"total_chunks": len(chunks_to_upsert), **counts_by_source}
        log_event(logger, "info", "kb_chunks_indexed", run_id, summary)
        return summary
    finally:
        conn.close()

