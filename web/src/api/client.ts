/**
 * Cliente HTTP centralizado con interceptor de errores (Sección 13):
 * - 401 -> limpia sesión y dispara logout automático
 * - 403 -> devuelve mensaje claro de acceso denegado por rol
 * - 503 -> devuelve aviso explícito de servicio de base de datos no disponible
 */

export interface UserSession {
  token: string;
  username: string;
  role: 'viewer' | 'admin';
}

export interface KpisData {
  events_24h: number;
  max_magnitude: number;
  avg_magnitude: number;
  tsunami_alerts: number;
  quality_score: number;
  last_updated: string | null;
  freshness_minutes: number;
  data_version: string;
}

export interface SeismicEvent {
  id: string;
  mag: number;
  mag_type: string;
  place: string;
  time: string;
  updated: string;
  lat: number;
  lon: number;
  depth_km: number;
  tsunami: number;
  sig: number;
  status: string;
  type: string;
  url: string;
  region: string;
  isNew?: boolean;
}

export interface PaginatedEvents {
  items: SeismicEvent[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface TimeseriesStats {
  hourly: Array<{
    hora: string;
    conteo: number;
    mag_max: number;
    mag_promedio: number;
    tsunami: number;
  }>;
  magnitude_distribution: Array<{
    rango: string;
    conteo: number;
  }>;
  top_regions: Array<{
    region: string;
    conteo: number;
    mag_max: number;
    mag_promedio: number;
  }>;
}

export interface DailyReport {
  fecha: string;
  titulo: string;
  resumen_ejecutivo: string;
  texto: string;
  proveedor: string;
  fallback: boolean;
  version_prompt: string;
  json_validado: Record<string, any>;
  updated_at: string;
}

export interface PaginatedReports {
  items: DailyReport[];
  total: number;
  page: number;
  page_size: number;
}

export interface PipelineStatus {
  overall_status: string;
  freshness_minutes: number;
  freshness_ok: boolean;
  quality_score: number;
  data_version: string;
  dags_summary: Array<{
    dag_id: string;
    run_id: string;
    estado: string;
    inicio: string | null;
    fin: string | null;
    resultado: string;
    data_version: string | null;
  }>;
  admin_detail?: {
    recent_pipeline_runs: Array<Record<string, any>>;
    recent_ai_runs: Array<Record<string, any>>;
    latest_quality_gate: Record<string, any>;
  } | null;
}

export interface QuarantineData {
  batches: Array<{
    run_id: string;
    timestamp: string;
    reglas_evaluadas: number;
    aprobadas: number;
    rechazadas: number;
    puntaje: number;
    conteo_por_regla: Record<string, number>;
    rechazos_log: Array<{ id: string; regla: string; motivo: string }>;
    frescura_ok: boolean;
    frescura_minutos: number;
    volumen_ok: boolean;
    volumen_total: number;
  }>;
  total_rejected_rows: number;
}

export interface ChatReply {
  answer: string;
  resolution_level: number;
  level_label: string;
  skill_used: string | null;
  provider: string;
  fallback: boolean;
  cached: boolean;
  data_version: string;
  supporting_data: Record<string, any> | null;
  latency_ms: number;
}

const SESSION_KEY = 'sismowatch_session_v1';

type UnauthorizedListener = () => void;
let onUnauthorizedCallback: UnauthorizedListener | null = null;

export function registerUnauthorizedInterceptor(cb: UnauthorizedListener): void {
  onUnauthorizedCallback = cb;
}

export function getSavedSession(): UserSession | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY) || localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as UserSession;
  } catch {
    return null;
  }
}

export function saveSession(session: UserSession): void {
  const str = JSON.stringify(session);
  sessionStorage.setItem(SESSION_KEY, str);
  localStorage.setItem(SESSION_KEY, str);
}

export function clearSession(): void {
  sessionStorage.removeItem(SESSION_KEY);
  localStorage.removeItem(SESSION_KEY);
}

export class ApiClientError extends Error {
  status: number;
  code: string;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function apiRequest<T>(path: string, options: RequestInit = {}): Promise<T> {
  const session = getSavedSession();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> | undefined),
  };
  if (session?.token) {
    headers['Authorization'] = `Bearer ${session.token}`;
  }

  let response: Response;
  try {
    response = await fetch(path, { ...options, headers });
  } catch {
    throw new ApiClientError(
      503,
      'NETWORK_UNAVAILABLE',
      'Servicio temporalmente no disponible: no fue posible conectar con el servidor API.'
    );
  }

  if (response.status === 401) {
    clearSession();
    if (onUnauthorizedCallback && !path.includes('/auth/login')) {
      onUnauthorizedCallback();
    }
    const errBody = await response.json().catch(() => ({}));
    throw new ApiClientError(
      401,
      'UNAUTHORIZED',
      errBody.message || 'Sesión expirada o credenciales inválidas. Por favor inicie sesión.'
    );
  }

  if (response.status === 403) {
    const errBody = await response.json().catch(() => ({}));
    throw new ApiClientError(
      403,
      'FORBIDDEN',
      errBody.message || 'Acceso denegado (403): su rol no tiene permisos para realizar esta operación.'
    );
  }

  if (response.status === 503) {
    const errBody = await response.json().catch(() => ({}));
    throw new ApiClientError(
      503,
      'SERVICE_UNAVAILABLE',
      errBody.message || 'Servicio no disponible (503): la base de datos o el backend están en mantenimiento.'
    );
  }

  if (!response.ok) {
    const errBody = await response.json().catch(() => ({}));
    throw new ApiClientError(
      response.status,
      errBody.error || 'API_ERROR',
      errBody.message || `Error HTTP ${response.status} al procesar la solicitud.`
    );
  }

  return (await response.json()) as T;
}

export const apiClient = {
  async login(username: string, password: string): Promise<UserSession> {
    const data = await apiRequest<{
      access_token: string;
      user: { username: string; role: 'viewer' | 'admin' };
    }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    const session: UserSession = {
      token: data.access_token,
      username: data.user.username,
      role: data.user.role,
    };
    saveSession(session);
    return session;
  },

  getKpis(): Promise<KpisData> {
    return apiRequest<KpisData>('/api/kpis');
  },

  getEvents(params: {
    start_date?: string;
    end_date?: string;
    min_mag?: number;
    region?: string;
    page?: number;
    page_size?: number;
  }): Promise<PaginatedEvents> {
    const q = new URLSearchParams();
    if (params.start_date) q.set('start_date', params.start_date);
    if (params.end_date) q.set('end_date', params.end_date);
    if (params.min_mag !== undefined && !Number.isNaN(params.min_mag)) {
      q.set('min_mag', String(params.min_mag));
    }
    if (params.region) q.set('region', params.region);
    q.set('page', String(params.page || 1));
    q.set('page_size', String(params.page_size || 60));
    return apiRequest<PaginatedEvents>(`/api/events?${q.toString()}`);
  },

  getTimeseries(params: {
    hours?: number;
    start_date?: string;
    end_date?: string;
    min_mag?: number;
    region?: string;
  }): Promise<TimeseriesStats> {
    const q = new URLSearchParams();
    q.set('hours', String(params.hours || 72));
    if (params.start_date) q.set('start_date', params.start_date);
    if (params.end_date) q.set('end_date', params.end_date);
    if (params.min_mag !== undefined && !Number.isNaN(params.min_mag)) {
      q.set('min_mag', String(params.min_mag));
    }
    if (params.region) q.set('region', params.region);
    return apiRequest<TimeseriesStats>(`/api/stats/timeseries?${q.toString()}`);
  },

  getReports(params: {
    search?: string;
    sort_dir?: 'asc' | 'desc';
    page?: number;
    page_size?: number;
  }): Promise<PaginatedReports> {
    const q = new URLSearchParams();
    if (params.search) q.set('search', params.search);
    q.set('sort_dir', params.sort_dir || 'desc');
    q.set('page', String(params.page || 1));
    q.set('page_size', String(params.page_size || 10));
    return apiRequest<PaginatedReports>(`/api/reports?${q.toString()}`);
  },

  async downloadExport(format: 'csv' | 'html'): Promise<void> {
    const session = getSavedSession();
    const res = await fetch(`/api/reports/export?format=${format}`, {
      headers: session?.token ? { Authorization: `Bearer ${session.token}` } : {},
    });
    if (!res.ok) {
      throw new ApiClientError(res.status, 'EXPORT_ERROR', 'No se pudo descargar la exportación.');
    }
    const blob = await res.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sismowatch_export_${new Date().toISOString().slice(0, 10)}.${format}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
  },

  getPipelineStatus(): Promise<PipelineStatus> {
    return apiRequest<PipelineStatus>('/api/pipeline/status');
  },

  getQuarantine(): Promise<QuarantineData> {
    return apiRequest<QuarantineData>('/api/quality/quarantine');
  },

  triggerBackfill(days: number = 30, minmagnitude: number = 2.5): Promise<{ status: string; message: string }> {
    return apiRequest('/api/admin/backfill', {
      method: 'POST',
      body: JSON.stringify({ days, minmagnitude }),
    });
  },

  regenerateReport(): Promise<{ status: string; message: string }> {
    return apiRequest('/api/admin/reports/regenerate', {
      method: 'POST',
    });
  },

  sendChatMessage(question: string): Promise<ChatReply> {
    return apiRequest<ChatReply>('/api/agent/chat', {
      method: 'POST',
      body: JSON.stringify({ question }),
    });
  },
};
