"""
Jerarquía de excepciones propias del dominio SismoWatch (Sección 15).

Regla transversal: Prohibido usar `except Exception` desnudo o silenciar errores.
Cada capa lanza o captura estas excepciones tipadas para decidir si reintenta
(errores transitorios), aplica fallback (IA) o bloquea el pipeline (calidad).
"""


class SismoWatchError(Exception):
    """Excepción base de dominio para todo el sistema SismoWatch."""


class IngestionError(SismoWatchError):
    """Error al consultar, validar estructura GeoJSON o persistir en Bronce."""

    def __init__(self, message: str, is_transient: bool = False) -> None:
        super().__init__(message)
        self.is_transient = is_transient


class QualityGateError(SismoWatchError):
    """Excepción bloqueante cuando un lote incumple las reglas de la compuerta de calidad."""


class PublishError(SismoWatchError):
    """Error transaccional al publicar datos de la capa Oro en PostgreSQL curated."""

    def __init__(self, message: str, is_transient: bool = False) -> None:
        super().__init__(message)
        self.is_transient = is_transient


class AIProviderError(SismoWatchError):
    """Error de comunicación, timeout o rate limit (429) de un proveedor LLM/Embeddings."""

    def __init__(self, message: str, provider: str = "unknown", is_transient: bool = True) -> None:
        super().__init__(message)
        self.provider = provider
        self.is_transient = is_transient


class AIValidationError(SismoWatchError):
    """Error cuando la salida del LLM no cumple el esquema JSON o inventa cifras numéricas."""


class UnsafeQueryError(SismoWatchError):
    """Error de seguridad cuando una consulta SQL viola las políticas de solo lectura."""
