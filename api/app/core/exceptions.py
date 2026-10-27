"""
Excepciones de dominio de la API y Manejador Global de Excepciones (Sección 12 y 15).

Garantiza respuestas JSON uniformes para 401, 403, 404, 422, 429 y 503 (BD caída),
sin exponer jamás stack traces internos ni secretos al cliente.
"""
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import psycopg2


class SismoWatchApiError(Exception):
    """Excepción base para errores controlados de la API."""

    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    error_code: str = "INTERNAL_ERROR"

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class AuthenticationError(SismoWatchApiError):
    status_code = status.HTTP_401_UNAUTHORIZED
    error_code = "UNAUTHORIZED"


class PermissionDeniedError(SismoWatchApiError):
    status_code = status.HTTP_403_FORBIDDEN
    error_code = "FORBIDDEN"


class ResourceNotFoundError(SismoWatchApiError):
    status_code = status.HTTP_404_NOT_FOUND
    error_code = "NOT_FOUND"


class UnsafeQueryError(SismoWatchApiError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    error_code = "UNSAFE_SQL_QUERY"


class AIProviderError(SismoWatchApiError):
    status_code = status.HTTP_502_BAD_GATEWAY
    error_code = "AI_PROVIDER_ERROR"


class AIValidationError(SismoWatchApiError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    error_code = "AI_VALIDATION_ERROR"


class RateLimitExceededError(SismoWatchApiError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    error_code = "RATE_LIMIT_EXCEEDED"


class DatabaseUnavailableError(SismoWatchApiError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    error_code = "DATABASE_UNAVAILABLE"


def _uniform_error_payload(
    status_code: int,
    error_code: str,
    message: str,
    path: str,
    details: Optional[Any] = None,
) -> Dict[str, Any]:
    return {
        "status": status_code,
        "error": error_code,
        "detail": message,
        "message": message,
        "path": path,
        "details": details or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def register_exception_handlers(app: FastAPI) -> None:
    """Registra los manejadores globales uniformes para 401, 403, 404, 422, 429, 503 y 500."""

    @app.exception_handler(SismoWatchApiError)
    async def handle_sismowatch_api_error(request: Request, exc: SismoWatchApiError) -> JSONResponse:
        headers = {"WWW-Authenticate": "Bearer"} if exc.status_code == 401 else None
        return JSONResponse(
            status_code=exc.status_code,
            content=_uniform_error_payload(
                status_code=exc.status_code,
                error_code=exc.error_code,
                message=exc.message,
                path=request.url.path,
                details=exc.details,
            ),
            headers=headers,
        )

    @app.exception_handler(psycopg2.OperationalError)
    @app.exception_handler(psycopg2.InterfaceError)
    async def handle_db_operational_error(request: Request, _exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=_uniform_error_payload(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                error_code="DATABASE_UNAVAILABLE",
                message="Servicio de base de datos temporalmente no disponible. Intente nuevamente en unos segundos.",
                path=request.url.path,
            ),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        clean_errors = [
            {"field": ".".join(str(loc) for loc in e.get("loc", [])), "msg": e.get("msg", "")}
            for e in exc.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_uniform_error_payload(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                error_code="VALIDATION_ERROR",
                message="Los parámetros enviados no cumplen el esquema requerido.",
                path=request.url.path,
                details={"errors": clean_errors},
            ),
        )

    @app.exception_handler(HTTPException)
    async def handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        code_map = {
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            422: "VALIDATION_ERROR",
            429: "RATE_LIMIT_EXCEEDED",
            503: "SERVICE_UNAVAILABLE",
        }
        return JSONResponse(
            status_code=exc.status_code,
            content=_uniform_error_payload(
                status_code=exc.status_code,
                error_code=code_map.get(exc.status_code, "HTTP_ERROR"),
                message=str(exc.detail),
                path=request.url.path,
            ),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(request: Request, _exc: Exception) -> JSONResponse:
        # Jamás exponer stack trace ni variables internas al cliente
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_uniform_error_payload(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="INTERNAL_SERVER_ERROR",
                message="Ocurrió un error interno controlado al procesar la solicitud.",
                path=request.url.path,
            ),
        )

