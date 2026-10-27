"""
Esquemas Pydantic v2 de entrada y salida para la API de SismoWatch (Sección 12).
"""
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=64)
    password: str = Field(..., min_length=3, max_length=128)


class UserInfo(BaseModel):
    username: str
    role: Literal["viewer", "admin"]


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_minutes: int
    user: UserInfo


class KpisResponse(BaseModel):
    events_24h: int
    max_magnitude: float
    avg_magnitude: float
    tsunami_alerts: int
    quality_score: float
    last_updated: Optional[datetime] = None
    freshness_minutes: float = 0.0
    data_version: str = "initial"


class SeismicEventItem(BaseModel):
    id: str
    mag: float
    mag_type: Optional[str] = "unknown"
    place: str
    time: datetime
    updated: datetime
    lat: float
    lon: float
    depth_km: float
    tsunami: int
    sig: int
    status: Optional[str] = "automatic"
    type: Optional[str] = "earthquake"
    url: Optional[str] = ""
    region: str


class PaginatedEventsResponse(BaseModel):
    items: List[SeismicEventItem]
    total: int
    page: int
    page_size: int
    total_pages: int


class HourlyPoint(BaseModel):
    hora: datetime
    conteo: int
    mag_max: float
    mag_promedio: float
    tsunami: int


class MagnitudeDistributionPoint(BaseModel):
    rango: str
    conteo: int


class RegionActivityPoint(BaseModel):
    region: str
    conteo: int
    mag_max: float
    mag_promedio: float


class TimeseriesStatsResponse(BaseModel):
    hourly: List[HourlyPoint]
    magnitude_distribution: List[MagnitudeDistributionPoint]
    top_regions: List[RegionActivityPoint]


class DailyReportItem(BaseModel):
    fecha: date
    titulo: str
    resumen_ejecutivo: str
    texto: str
    proveedor: str
    fallback: bool
    version_prompt: str
    json_validado: Dict[str, Any]
    updated_at: datetime


class PaginatedReportsResponse(BaseModel):
    items: List[DailyReportItem]
    total: int
    page: int
    page_size: int


class PipelineStatusResponse(BaseModel):
    overall_status: str
    freshness_minutes: float
    freshness_ok: bool
    quality_score: float
    data_version: str
    dags_summary: List[Dict[str, Any]]
    # Solo se llena cuando el usuario tiene rol admin
    admin_detail: Optional[Dict[str, Any]] = None


class QuarantineBatchItem(BaseModel):
    run_id: str
    timestamp: datetime
    reglas_evaluadas: int
    aprobadas: int
    rechazadas: int
    puntaje: float
    conteo_por_regla: Dict[str, Any]
    rechazos_log: List[Dict[str, Any]]
    frescura_ok: bool
    frescura_minutos: float
    volumen_ok: bool
    volumen_total: int


class QuarantineResponse(BaseModel):
    batches: List[QuarantineBatchItem]
    total_rejected_rows: int


class BackfillTriggerRequest(BaseModel):
    days: int = Field(default=30, ge=1, le=90)
    minmagnitude: float = Field(default=2.5, ge=-1.0, le=9.0)


class AdminActionResponse(BaseModel):
    status: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=600)


class ChatResponse(BaseModel):
    answer: str
    resolution_level: int = Field(..., description="0=Intent/FAQ, 1=Cache, 2=LLM+Skill, 3=Fallback Determinista")
    level_label: str
    skill_used: Optional[str] = None
    provider: str
    fallback: bool = False
    cached: bool = False
    data_version: str
    supporting_data: Optional[Dict[str, Any]] = None
    latency_ms: float
