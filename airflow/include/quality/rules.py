"""
Reglas de calidad por fila y funciones puras de validación y parseo GeoJSON (Sección 10).

Reglas por fila (descarte con rastro liviano {id, regla, motivo}):
1. `missing_required_fields`: falta `id`, `time` o coordenadas `[lon, lat, depth_km]`.
2. `invalid_timestamp`: `time` o `updated` no es convertible a fecha/hora UTC válida.
3. `invalid_coordinates_range`: latitud fuera de [-90, 90] o longitud fuera de [-180, 180].
4. `invalid_magnitude_range`: magnitud nula o fuera de [-2.0, 10.0].
5. `invalid_depth_range`: profundidad nula o fuera de [-5.0, 800.0] km.
6. `duplicate_id`: id duplicado en el lote (se conserva el registro con mayor `updated`).
"""
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from common.sanitization import derive_region, sanitize_place

LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0
MAG_MIN, MAG_MAX = -2.0, 10.0
DEPTH_MIN, DEPTH_MAX = -5.0, 800.0


def _ms_to_utc_datetime(val: Any) -> Optional[datetime]:
    """Convierte milisegundos epoch (int/float) o ISO string a datetime UTC."""
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return None
        try:
            # Rango razonable entre 1970 y 2100 en milisegundos
            if val < 0 or val > 4102444800000:
                return None
            return datetime.fromtimestamp(float(val) / 1000.0, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(val, str):
        try:
            num = float(val)
            return _ms_to_utc_datetime(num)
        except ValueError:
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                return None
    return None


def _to_float(val: Any) -> Optional[float]:
    if val is None or isinstance(val, bool):
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def evaluate_row_quality(row: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, str]]]:
    """
    Evalúa las reglas de calidad sobre una fila candidata a Plata.
    Nunca lanza excepción hacia afuera (try/except por fila).
    Retorna `(True, None)` si es válida, o `(False, {"id": ..., "regla": ..., "motivo": ...})` si se descarta.
    """
    try:
        raw_id = row.get("id")
        event_id = str(raw_id).strip() if raw_id is not None else ""
        if not event_id:
            return False, {
                "id": "UNKNOWN_ID",
                "regla": "missing_required_fields",
                "motivo": "Falta identificador obligatorio 'id'.",
            }

        raw_time = row.get("time")
        if raw_time is None:
            return False, {
                "id": event_id,
                "regla": "missing_required_fields",
                "motivo": "Falta campo obligatorio 'time'.",
            }

        dt_time = _ms_to_utc_datetime(raw_time)
        if dt_time is None:
            return False, {
                "id": event_id,
                "regla": "invalid_timestamp",
                "motivo": f"Formato de fecha inválido en 'time': {raw_time!r}.",
            }

        raw_lon = row.get("lon")
        raw_lat = row.get("lat")
        raw_depth = row.get("depth_km")
        if raw_lon is None or raw_lat is None:
            return False, {
                "id": event_id,
                "regla": "missing_required_fields",
                "motivo": "Faltan coordenadas obligatorias 'lon' o 'lat'.",
            }

        lon = _to_float(raw_lon)
        lat = _to_float(raw_lat)
        if lon is None or lat is None or not (LAT_MIN <= lat <= LAT_MAX) or not (LON_MIN <= lon <= LON_MAX):
            return False, {
                "id": event_id,
                "regla": "invalid_coordinates_range",
                "motivo": f"Coordenadas fuera de rango permitido: lat={raw_lat}, lon={raw_lon}.",
            }

        mag = _to_float(row.get("mag"))
        if mag is None or not (MAG_MIN <= mag <= MAG_MAX):
            return False, {
                "id": event_id,
                "regla": "invalid_magnitude_range",
                "motivo": f"Magnitud nula o fuera del rango [-2, 10]: mag={row.get('mag')}.",
            }

        depth_km = _to_float(raw_depth)
        if depth_km is None or not (DEPTH_MIN <= depth_km <= DEPTH_MAX):
            return False, {
                "id": event_id,
                "regla": "invalid_depth_range",
                "motivo": f"Profundidad nula o fuera del rango [-5, 800] km: depth_km={raw_depth}.",
            }

        return True, None
    except Exception as exc:
        fallback_id = str(row.get("id", "CORRUPT_ROW")) if isinstance(row, dict) else "CORRUPT_ROW"
        return False, {
            "id": fallback_id,
            "regla": "corrupt_row_exception",
            "motivo": f"Excepción controlada al validar fila: {exc}",
        }


def transform_geojson_feature_to_silver_row(
    feature: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
    """
    Convierte un feature individual de GeoJSON USGS en una fila tipada de Plata.
    Extrae `[lon, lat, depth_km]` de `geometry.coordinates`, convierte `time` y `updated`
    de milisegundos epoch a `datetime` UTC, sanitiza `place` y deriva `region`.
    Protegido con `try/except` por fila para que una fila corrupta nunca tumbe el lote.
    """
    try:
        if not isinstance(feature, dict):
            return None, {
                "id": "INVALID_FEATURE",
                "regla": "missing_required_fields",
                "motivo": "El elemento feature no es un diccionario JSON.",
            }

        raw_id = feature.get("id")
        props = feature.get("properties") or {}
        geom = feature.get("geometry") or {}
        coords = geom.get("coordinates")

        lon = coords[0] if isinstance(coords, (list, tuple)) and len(coords) >= 2 else None
        lat = coords[1] if isinstance(coords, (list, tuple)) and len(coords) >= 2 else None
        depth_km = coords[2] if isinstance(coords, (list, tuple)) and len(coords) >= 3 else None

        candidate = {
            "id": raw_id,
            "mag": props.get("mag"),
            "mag_type": str(props.get("magType") or "unknown")[:32],
            "place": props.get("place"),
            "time": props.get("time"),
            "updated": props.get("updated") if props.get("updated") is not None else props.get("time"),
            "lat": lat,
            "lon": lon,
            "depth_km": depth_km,
            "tsunami": 1 if int(props.get("tsunami") or 0) == 1 else 0,
            "sig": int(props.get("sig") or 0),
            "status": str(props.get("status") or "automatic")[:32],
            "type": str(props.get("type") or "earthquake")[:32],
            "url": str(props.get("url") or ""),
        }

        is_valid, rejection = evaluate_row_quality(candidate)
        if not is_valid:
            return None, rejection

        dt_time = _ms_to_utc_datetime(candidate["time"])
        dt_updated = _ms_to_utc_datetime(candidate["updated"]) or dt_time
        assert dt_time is not None and dt_updated is not None

        clean_place = sanitize_place(candidate["place"])
        region = derive_region(clean_place)

        silver_row = {
            "id": str(raw_id).strip(),
            "mag": float(candidate["mag"]),
            "mag_type": candidate["mag_type"],
            "place": clean_place,
            "time": dt_time,
            "updated": dt_updated,
            "lat": float(candidate["lat"]),
            "lon": float(candidate["lon"]),
            "depth_km": float(candidate["depth_km"]),
            "tsunami": candidate["tsunami"],
            "sig": candidate["sig"],
            "status": candidate["status"],
            "type": candidate["type"],
            "url": candidate["url"],
            "region": region,
            "event_date": dt_time.date(),
        }
        return silver_row, None
    except Exception as exc:
        fid = str(feature.get("id", "CORRUPT_FEATURE")) if isinstance(feature, dict) else "CORRUPT_FEATURE"
        return None, {
            "id": fid,
            "regla": "corrupt_row_exception",
            "motivo": f"Error al transformar feature GeoJSON: {exc}",
        }


def deduplicate_silver_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Deduplica una colección de filas de Plata por `id`, conservando la de mayor `updated`
    y registrando cada duplicado descartado en la lista de rechazos.
    """
    best_by_id: Dict[str, Dict[str, Any]] = {}
    rejections: List[Dict[str, str]] = []

    for row in rows:
        eid = row["id"]
        if eid not in best_by_id:
            best_by_id[eid] = row
        else:
            existing = best_by_id[eid]
            if row["updated"] >= existing["updated"]:
                best_by_id[eid] = row
                rejections.append(
                    {
                        "id": eid,
                        "regla": "duplicate_id",
                        "motivo": f"ID duplicado descartado (updated menor o igual: {existing['updated'].isoformat()}).",
                    }
                )
            else:
                rejections.append(
                    {
                        "id": eid,
                        "regla": "duplicate_id",
                        "motivo": f"ID duplicado descartado (updated menor: {row['updated'].isoformat()}).",
                    }
                )

    return list(best_by_id.values()), rejections

