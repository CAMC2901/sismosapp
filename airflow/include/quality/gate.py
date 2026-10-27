"""
Compuerta de calidad por lote (bloqueante) con Pandera y persistencia en curated.quality_results.

Si el esquema está roto, la frescura supera FRESHNESS_MAX_MINUTES, el volumen está
fuera de [VOLUME_MIN, VOLUME_MAX] o el porcentaje de filas inválidas supera INVALID_RATIO_MAX,
registra el resultado en `curated.quality_results` y lanza `QualityGateError` para impedir
que el lote defectuoso se publique en Oro o en PostgreSQL curated.
"""
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from common.config import get_config, get_db_connection
from common.exceptions import QualityGateError
from common.logging_utils import get_structured_logger, log_event
from quality.rules import DEPTH_MAX, DEPTH_MIN, LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, MAG_MAX, MAG_MIN

logger = get_structured_logger("sismowatch.quality_gate")

SILVER_PANDERA_SCHEMA = DataFrameSchema(
    columns={
        "id": Column(str, Check.str_length(min_value=1), nullable=False),
        "mag": Column(float, Check.in_range(MAG_MIN, MAG_MAX), nullable=False),
        "mag_type": Column(str, nullable=True),
        "place": Column(str, Check.str_length(min_value=1), nullable=False),
        "lat": Column(float, Check.in_range(LAT_MIN, LAT_MAX), nullable=False),
        "lon": Column(float, Check.in_range(LON_MIN, LON_MAX), nullable=False),
        "depth_km": Column(float, Check.in_range(DEPTH_MIN, DEPTH_MAX), nullable=False),
        "tsunami": Column(int, Check.isin([0, 1]), nullable=False),
        "sig": Column(int, nullable=False),
        "status": Column(str, nullable=True),
        "type": Column(str, nullable=True),
        "url": Column(str, nullable=True),
        "region": Column(str, Check.str_length(min_value=1), nullable=False),
    },
    strict=False,
    coerce=True,
)


def validate_silver_dataframe_schema(df: pd.DataFrame) -> None:
    """Valida el esquema y dominios de un DataFrame de Plata con Pandera."""
    if df.empty:
        # Verificar al menos que las columnas requeridas existan si se proveen
        required_cols = set(SILVER_PANDERA_SCHEMA.columns.keys())
        if len(df.columns) > 0 and not required_cols.issubset(set(df.columns)):
            missing = required_cols - set(df.columns)
            raise QualityGateError(f"Esquema Pandera inválido: faltan columnas {sorted(missing)}")
        return
    try:
        SILVER_PANDERA_SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        raise QualityGateError(f"Fallo de validación de esquema Pandera en Plata: {exc}") from exc


def save_quality_results(
    run_id: str,
    evaluated_count: int,
    approved_count: int,
    rejected_count: int,
    score: float,
    rule_counts: Dict[str, int],
    rejections_log: List[Dict[str, str]],
    freshness_ok: bool,
    freshness_minutes: float,
    volume_ok: bool,
    volume_total: int,
) -> None:
    """Guarda la auditoría de calidad del lote en `curated.quality_results` usando `transform_role`."""
    conn = get_db_connection(role="transform")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.quality_results (
                        run_id, reglas_evaluadas, aprobadas, rechazadas, puntaje,
                        conteo_por_regla, rechazos_log, frescura_ok, frescura_minutos,
                        volumen_ok, volumen_total, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, NOW())
                    ON CONFLICT (run_id) DO UPDATE
                    SET reglas_evaluadas = EXCLUDED.reglas_evaluadas,
                        aprobadas = EXCLUDED.aprobadas,
                        rechazadas = EXCLUDED.rechazadas,
                        puntaje = EXCLUDED.puntaje,
                        conteo_por_regla = EXCLUDED.conteo_por_regla,
                        rechazos_log = EXCLUDED.rechazos_log,
                        frescura_ok = EXCLUDED.frescura_ok,
                        frescura_minutos = EXCLUDED.frescura_minutos,
                        volumen_ok = EXCLUDED.volumen_ok,
                        volumen_total = EXCLUDED.volumen_total,
                        timestamp = NOW();
                    """,
                    (
                        run_id,
                        evaluated_count,
                        approved_count,
                        rejected_count,
                        round(score, 4),
                        json.dumps(rule_counts, ensure_ascii=False),
                        json.dumps(rejections_log[:500], ensure_ascii=False),
                        freshness_ok,
                        round(freshness_minutes, 2),
                        volume_ok,
                        volume_total,
                    ),
                )
    finally:
        conn.close()


def evaluate_batch_quality_gate(
    run_id: str,
    valid_df: pd.DataFrame,
    rejections_log: List[Dict[str, str]],
    latest_ingestion_ts: Optional[datetime] = None,
    persist_to_db: bool = True,
) -> Dict[str, Any]:
    """
    Ejecuta la compuerta bloqueante por lote:
    1. Esquema Pandera sobre las filas de Plata.
    2. Frescura respecto a `FRESHNESS_MAX_MINUTES`.
    3. Volumen total dentro de `[VOLUME_MIN, VOLUME_MAX]`.
    4. Porcentaje de filas inválidas respecto a `INVALID_RATIO_MAX`.
    """
    cfg = get_config()
    approved_count = len(valid_df)
    domain_rejections = [r for r in rejections_log if r.get("regla") != "duplicate_id"]
    rejected_count = len(domain_rejections)
    total_count = approved_count + rejected_count
    evaluated_rules = total_count if total_count > 0 else 1
    score = (approved_count / total_count) if total_count > 0 else 1.0
    invalid_ratio = (rejected_count / total_count) if total_count > 0 else 0.0

    rule_counts: Dict[str, int] = {}
    for item in rejections_log:
        rule_name = item.get("regla", "unknown_rule")
        rule_counts[rule_name] = rule_counts.get(rule_name, 0) + 1

    now_utc = datetime.now(timezone.utc)
    if latest_ingestion_ts is not None:
        ts_utc = (
            latest_ingestion_ts.astimezone(timezone.utc)
            if latest_ingestion_ts.tzinfo
            else latest_ingestion_ts.replace(tzinfo=timezone.utc)
        )
        freshness_minutes = max(0.0, (now_utc - ts_utc).total_seconds() / 60.0)
    else:
        freshness_minutes = 0.0

    freshness_ok = freshness_minutes <= cfg.freshness_max_minutes
    volume_ok = cfg.volume_min <= total_count <= cfg.volume_max
    ratio_ok = invalid_ratio <= cfg.invalid_ratio_max

    schema_error: Optional[str] = None
    try:
        validate_silver_dataframe_schema(valid_df)
    except QualityGateError as exc:
        schema_error = str(exc)
        rule_counts["pandera_schema_error"] = rule_counts.get("pandera_schema_error", 0) + 1

    if persist_to_db:
        save_quality_results(
            run_id=run_id,
            evaluated_count=evaluated_rules,
            approved_count=approved_count,
            rejected_count=rejected_count,
            score=score if not schema_error else 0.0,
            rule_counts=rule_counts,
            rejections_log=rejections_log,
            freshness_ok=freshness_ok,
            freshness_minutes=freshness_minutes,
            volume_ok=volume_ok,
            volume_total=total_count,
        )

    errors: List[str] = []
    if schema_error:
        errors.append(schema_error)
    if not freshness_ok:
        errors.append(
            f"Frescura del lote ({freshness_minutes:.1f} min) supera el máximo permitido ({cfg.freshness_max_minutes} min)."
        )
    if not volume_ok:
        errors.append(
            f"Volumen del lote ({total_count}) fuera del rango permitido [{cfg.volume_min}, {cfg.volume_max}]."
        )
    if not ratio_ok:
        errors.append(
            f"Ratio de filas inválidas ({invalid_ratio:.2%}) supera el umbral máximo ({cfg.invalid_ratio_max:.2%})."
        )

    if errors:
        msg = " | ".join(errors)
        log_event(logger, "error", "quality_gate_blocked", run_id, {"errors": errors, "score": score})
        raise QualityGateError(f"Compuerta de calidad bloqueó el lote ({run_id}): {msg}")

    result = {
        "run_id": run_id,
        "evaluated": evaluated_rules,
        "approved": approved_count,
        "rejected": rejected_count,
        "score": round(score, 4),
        "freshness_minutes": round(freshness_minutes, 2),
        "freshness_ok": freshness_ok,
        "volume_ok": volume_ok,
        "rule_counts": rule_counts,
    }
    log_event(logger, "info", "quality_gate_passed", run_id, result)
    return result

