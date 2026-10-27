"""
Logging estructurado con run_id y redacción automática de credenciales (Sección 14).
"""
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

SECRET_PATTERNS = re.compile(
    r"(password|secret|api_key|token|authorization)\s*[:=]\s*['\"]?([^'\"\s,}]+)",
    re.IGNORECASE,
)


def redact_secrets(text: str) -> str:
    """Oculta cualquier posible secreto o credencial antes de emitir el log."""
    if not text:
        return ""
    return SECRET_PATTERNS.sub(r"\1=***REDACTED***", str(text))


def get_structured_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def log_event(
    logger: logging.Logger,
    level: str,
    event: str,
    run_id: str = "system",
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Emite un registro JSON estructurado con run_id, sin secretos ni payloads crudos."""
    safe_details: Dict[str, Any] = {}
    if details:
        for k, v in details.items():
            if any(sec in k.lower() for sec in ("password", "secret", "key", "token")):
                safe_details[k] = "***REDACTED***"
            elif isinstance(v, str) and len(v) > 500:
                safe_details[k] = redact_secrets(v[:500]) + "...[truncated]"
            elif isinstance(v, str):
                safe_details[k] = redact_secrets(v)
            else:
                safe_details[k] = v

    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level.upper(),
        "run_id": run_id,
        "event": event,
        "details": safe_details,
    }
    msg = json.dumps(record, ensure_ascii=False, default=str)
    getattr(logger, level.lower(), logger.info)(msg)

