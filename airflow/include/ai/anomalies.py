"""
Detección de anomalías sísmicas (Sección 11 — Automatización 2):
1. Z-score estadístico sobre el conteo horario (`curated.agg_hourly`) para detectar picos de actividad.
2. Clustering espacial DBSCAN (`haversine`) sobre coordenadas `[lat, lon]` para detectar enjambres sísmicos.

Cada anomalía lleva una explicación breve redactada con las cifras calculadas por código
(con plantilla determinista como respaldo) y se persiste en `curated.anomalies` con `ai_role`.
"""
import json
from typing import Any, Dict, List
import numpy as np
from pydantic import BaseModel, Field
from sklearn.cluster import DBSCAN
from ai.llm_client import execute_structured_llm_with_cascade, load_versioned_prompt
from common.config import get_db_connection
from common.logging_utils import get_structured_logger, log_event
from common.sanitization import delimit_untrusted_data, sanitize_place

logger = get_structured_logger("sismowatch.ai.anomalies")


class AnomalyExplanationSchema(BaseModel):
    explicacion: str = Field(..., min_length=15)


def detect_and_store_anomalies(
    hours_window: int = 72,
    z_threshold: float = 1.8,
    run_id: str = "daily_ai",
) -> List[Dict[str, Any]]:
    """
    Ejecuta la detección de anomalías (z-score horario y enjambres espaciales DBSCAN),
    genera sus explicaciones verificadas y las guarda en `curated.anomalies` usando `ai_role`.
    """
    conn = get_db_connection(role="ai")
    detected_anomalies: List[Dict[str, Any]] = []
    sys_prompt = load_versioned_prompt("anomaly_explanation_v1")

    try:
        with conn.cursor() as cur:
            # 1. Consultar serie horaria reciente para z-score
            cur.execute(
                """
                SELECT hora, conteo, mag_max, mag_promedio
                FROM curated.agg_hourly
                WHERE hora >= NOW() - (%s || ' hours')::interval
                ORDER BY hora ASC;
                """,
                (hours_window,),
            )
            hourly_rows = cur.fetchall()

            if len(hourly_rows) >= 4:
                counts = np.array([float(r[1]) for r in hourly_rows], dtype=float)
                mean_c = float(np.mean(counts))
                std_c = float(np.std(counts))
                if std_c > 0:
                    for hora_dt, conteo, mag_max, mag_prom in hourly_rows:
                        z_val = (float(conteo) - mean_c) / std_c
                        if z_val >= z_threshold:
                            ventana_str = hora_dt.strftime("%Y-%m-%d %H:00 UTC")
                            z_round = round(float(z_val), 2)
                            c_int = int(conteo)
                            m_max = round(float(mag_max), 1)

                            def _z_fallback(
                                v_str: str = ventana_str,
                                cnt: int = c_int,
                                z_r: float = z_round,
                                m_m: float = m_max,
                                avg_c: float = round(mean_c, 1),
                            ) -> Dict[str, Any]:
                                return {
                                    "explicacion": (
                                        f"Pico estadístico de actividad sísmica en la ventana {v_str} con {cnt} eventos "
                                        f"(z-score {z_r} frente al promedio horario de {avg_c}) y magnitud máxima de {m_m}."
                                    )
                                }

                            user_p = (
                                f"Tipo: z-score horario. Ventana: {ventana_str}. "
                                f"Eventos en la hora: {c_int}. Promedio horario: {round(mean_c, 1)}. "
                                f"Z-score: {z_round}. Magnitud máxima: {m_max}."
                            )
                            llm_res = execute_structured_llm_with_cascade(
                                tarea="anomaly_explanation_zscore",
                                system_prompt=sys_prompt,
                                user_prompt=user_p,
                                schema_cls=AnomalyExplanationSchema,
                                required_numbers=[float(c_int), z_round],
                                template_fallback_fn=_z_fallback,
                                run_id=run_id,
                                db_role="ai",
                                timeout_sec=10.0,
                            )
                            detected_anomalies.append(
                                {
                                    "tipo": "z-score",
                                    "ventana": ventana_str,
                                    "score": z_round,
                                    "explicacion": llm_res["data"]["explicacion"],
                                    "datos_soporte": {
                                        "conteo_hora": c_int,
                                        "media_horaria": round(mean_c, 2),
                                        "desviacion_estandar": round(std_c, 2),
                                        "mag_max": m_max,
                                        "proveedor_ia": llm_res["provider"],
                                    },
                                }
                            )

            # 2. Consultar eventos recientes para clustering espacial DBSCAN (enjambres)
            cur.execute(
                """
                SELECT id, lat, lon, mag, depth_km, region, time
                FROM curated.events
                WHERE time >= NOW() - (%s || ' hours')::interval
                ORDER BY time DESC
                LIMIT 3000;
                """,
                (hours_window,),
            )
            ev_rows = cur.fetchall()

            if len(ev_rows) >= 5:
                coords_rad = np.radians(np.array([[float(r[1]), float(r[2])] for r in ev_rows]))
                # eps=0.025 rad (~159 km en superficie terrestre), min_samples=5 eventos
                clustering = DBSCAN(eps=0.025, min_samples=5, metric="haversine").fit(coords_rad)
                labels = clustering.labels_
                unique_labels = [lbl for lbl in set(labels) if lbl != -1]

                # Ordenar clusters por cantidad de eventos descendente (top 3 enjambres)
                cluster_summaries = []
                for lbl in unique_labels:
                    idxs = np.where(labels == lbl)[0]
                    cluster_Summaries_rows = [ev_rows[i] for i in idxs]
                    cluster_summaries.append((len(idxs), lbl, cluster_Summaries_rows))

                cluster_summaries.sort(key=lambda x: x[0], reverse=True)

                for count_ev, lbl, c_rows in cluster_summaries[:3]:
                    lats = [float(r[1]) for r in c_rows]
                    lons = [float(r[2]) for r in c_rows]
                    mags = [float(r[3]) for r in c_rows]
                    depths = [float(r[4]) for r in c_rows]
                    regions = [sanitize_place(str(r[5])) for r in c_rows]
                    top_region = max(set(regions), key=regions.count)
                    min_time = min(r[6] for r in c_rows)
                    max_time = max(r[6] for r in c_rows)

                    ventana_swarm = f"{top_region} ({min_time.strftime('%Y-%m-%d')} a {max_time.strftime('%Y-%m-%d')})"
                    max_mag_c = round(max(mags), 1)
                    avg_depth_c = round(float(np.mean(depths)), 1)
                    density_score = round(float(count_ev) * (max_mag_c / 3.0), 2)

                    def _dbscan_fallback(
                        reg: str = top_region,
                        cnt: int = count_ev,
                        mm: float = max_mag_c,
                        ad: float = avg_depth_c,
                    ) -> Dict[str, Any]:
                        return {
                            "explicacion": (
                                f"Enjambre sísmico espacial detectado por DBSCAN en {reg} con {cnt} sismos "
                                f"concentrados, magnitud máxima de {mm} y profundidad media de {ad} km."
                            )
                        }

                    user_p = (
                        f"Tipo: enjambre DBSCAN. Región: {delimit_untrusted_data(top_region)}. "
                        f"Cantidad de sismos en el cluster: {count_ev}. "
                        f"Magnitud máxima: {max_mag_c}. Profundidad media: {avg_depth_c} km."
                    )
                    llm_res = execute_structured_llm_with_cascade(
                        tarea="anomaly_explanation_dbscan",
                        system_prompt=sys_prompt,
                        user_prompt=user_p,
                        schema_cls=AnomalyExplanationSchema,
                        required_numbers=[float(count_ev), max_mag_c],
                        template_fallback_fn=_dbscan_fallback,
                        run_id=run_id,
                        db_role="ai",
                        timeout_sec=10.0,
                    )
                    detected_anomalies.append(
                        {
                            "tipo": "enjambre DBSCAN",
                            "ventana": ventana_swarm[:120],
                            "score": density_score,
                            "explicacion": llm_res["data"]["explicacion"],
                            "datos_soporte": {
                                "cluster_id": int(lbl),
                                "eventos_en_enjambre": int(count_ev),
                                "region": top_region,
                                "centroide_lat": round(float(np.mean(lats)), 4),
                                "centroide_lon": round(float(np.mean(lons)), 4),
                                "mag_max": max_mag_c,
                                "profundidad_media_km": avg_depth_c,
                                "proveedor_ia": llm_res["provider"],
                            },
                        }
                    )

            # Persistir en curated.anomalies
            with conn:
                for anom in detected_anomalies:
                    cur.execute(
                        """
                        INSERT INTO curated.anomalies (tipo, ventana, score, explicacion, datos_soporte, detected_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                        ON CONFLICT (tipo, ventana) DO UPDATE SET
                            score = EXCLUDED.score,
                            explicacion = EXCLUDED.explicacion,
                            datos_soporte = EXCLUDED.datos_soporte,
                            detected_at = NOW();
                        """,
                        (
                            anom["tipo"],
                            anom["ventana"],
                            anom["score"],
                            anom["explicacion"],
                            json.dumps(anom["datos_soporte"], ensure_ascii=False),
                        ),
                    )

        log_event(
            logger,
            "info",
            "anomalies_detection_completed",
            run_id,
            {"anomalies_detected": len(detected_anomalies)},
        )
        return detected_anomalies
    finally:
        conn.close()

