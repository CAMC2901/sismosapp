import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  Bot,
  CheckCircle2,
  Clock,
  Database,
  Download,
  FileText,
  Filter,
  Globe,
  Layers,
  Lock,
  LogOut,
  MapPin,
  Play,
  Radio,
  RefreshCw,
  Search,
  Send,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  TrendingUp,
  Waves,
} from 'lucide-react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  apiClient,
  ChatReply,
  clearSession,
  DailyReport,
  getSavedSession,
  KpisData,
  PipelineStatus,
  QuarantineData,
  registerUnauthorizedInterceptor,
  SeismicEvent,
  TimeseriesStats,
  UserSession,
} from './api/client';
import { SeismicMap } from './components/SeismicMap';

type ActiveTab = 'dashboard' | 'reports' | 'pipeline' | 'chat' | 'admin';
type SseStatus = 'connected' | 'reconnecting' | 'disconnected';

interface ChatMessage {
  id: string;
  sender: 'user' | 'agent';
  text: string;
  meta?: ChatReply;
  ts: string;
}

const SUGGESTED_QUESTIONS = [
  '¿Qué es SismoWatch y para qué sirve?',
  '¿Cuáles son los KPIs de las últimas 24 horas?',
  '¿Cuál fue el sismo más fuerte registrado?',
  '¿Cuáles son las regiones con mayor actividad sísmica?',
  '¿Qué anomalías (z-score o enjambres DBSCAN) se detectaron?',
  '¿Cómo funciona el control de calidad de datos con Pandera?',
  'SELECT id, mag, region, depth_km FROM curated.events ORDER BY mag DESC LIMIT 5',
];

export const App: React.FC = () => {
  const [session, setSession] = useState<UserSession | null>(() => getSavedSession());
  const [activeTab, setActiveTab] = useState<ActiveTab>('dashboard');

  // Estado de Login
  const [loginUser, setLoginUser] = useState('admin');
  const [loginPass, setLoginPass] = useState('AdminSismo2026!');
  const [loginLoading, setLoginLoading] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  // Estado del Dashboard
  const [kpis, setKpis] = useState<KpisData | null>(null);
  const [events, setEvents] = useState<SeismicEvent[]>([]);
  const [totalEventsCount, setTotalEventsCount] = useState(0);
  const [stats, setStats] = useState<TimeseriesStats | null>(null);
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [dashLoading, setDashLoading] = useState(false);
  const [dashError, setDashError] = useState<string | null>(null);

  // Filtros del Dashboard
  const [filterStartDate, setFilterStartDate] = useState('');
  const [filterEndDate, setFilterEndDate] = useState('');
  const [filterMinMag, setFilterMinMag] = useState<string>('0');
  const [filterRegion, setFilterRegion] = useState('');

  // Tiempo Real SSE
  const [sseStatus, setSseStatus] = useState<SseStatus>('disconnected');
  const [lastLiveBanner, setLastLiveBanner] = useState<string | null>(null);
  const sseRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);

  // Estado de Reportes
  const [reports, setReports] = useState<DailyReport[]>([]);
  const [selectedReport, setSelectedReport] = useState<DailyReport | null>(null);
  const [reportSearch, setReportSearch] = useState('');
  const [reportSort, setReportSort] = useState<'desc' | 'asc'>('desc');
  const [reportsLoading, setReportsLoading] = useState(false);
  const [reportsError, setReportsError] = useState<string | null>(null);

  // Estado del Pipeline
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null);
  const [pipelineLoading, setPipelineLoading] = useState(false);
  const [pipelineError, setPipelineError] = useState<string | null>(null);

  // Estado Admin (Cuarentena/Rechazos y acciones)
  const [quarantine, setQuarantine] = useState<QuarantineData | null>(null);
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminMessage, setAdminMessage] = useState<string | null>(null);
  const [adminError, setAdminError] = useState<string | null>(null);
  const [backfillDays, setBackfillDays] = useState(30);
  const [backfillMinMag, setBackfillMinMag] = useState(2.5);

  // Estado del Agente Conversacional
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([
    {
      id: 'welcome',
      sender: 'agent',
      text: '¡Bienvenido al centro de inteligencia **SismoWatch**! Estoy conectado al schema `curated` con arquitectura en cascada de 4 niveles (Intents/FAQ → Caché Exacta/Semántica `bge-m3` → Skills + LLM → Respaldo Determinista). ¿Qué deseas analizar hoy?',
      ts: new Date().toLocaleTimeString(),
    },
  ]);
  const [chatInput, setChatInput] = useState('');
  const [chatLoading, setChatLoading] = useState(false);

  // Registrar interceptor global de 401
  useEffect(() => {
    registerUnauthorizedInterceptor(() => {
      setSession(null);
      setLoginError('Su sesión expiró o no está autorizada (401). Por favor inicie sesión nuevamente.');
    });
  }, []);

  const handleLogout = useCallback(() => {
    if (sseRef.current) {
      sseRef.current.close();
      sseRef.current = null;
    }
    clearSession();
    setSession(null);
    setActiveTab('dashboard');
  }, []);

  // Carga del Dashboard
  const loadDashboardData = useCallback(async () => {
    if (!session) return;
    setDashLoading(true);
    setDashError(null);
    try {
      const minMagNum = filterMinMag !== '' ? parseFloat(filterMinMag) : undefined;
      const [kpiRes, evRes, stRes] = await Promise.all([
        apiClient.getKpis(),
        apiClient.getEvents({
          start_date: filterStartDate || undefined,
          end_date: filterEndDate || undefined,
          min_mag: minMagNum,
          region: filterRegion || undefined,
          page: 1,
          page_size: 120,
        }),
        apiClient.getTimeseries({
          hours: 168,
          start_date: filterStartDate || undefined,
          end_date: filterEndDate || undefined,
          min_mag: minMagNum,
          region: filterRegion || undefined,
        }),
      ]);
      setKpis(kpiRes);
      setEvents(evRes.items);
      setTotalEventsCount(evRes.total);
      setStats(stRes);
    } catch (err: any) {
      setDashError(err.message || 'Error al cargar los datos del dashboard.');
    } finally {
      setDashLoading(false);
    }
  }, [session, filterStartDate, filterEndDate, filterMinMag, filterRegion]);

  // Carga de Reportes
  const loadReportsData = useCallback(async () => {
    if (!session) return;
    setReportsLoading(true);
    setReportsError(null);
    try {
      const res = await apiClient.getReports({
        search: reportSearch || undefined,
        sort_dir: reportSort,
        page: 1,
        page_size: 20,
      });
      setReports(res.items);
      if (res.items.length > 0 && !selectedReport) {
        setSelectedReport(res.items[0]);
      }
    } catch (err: any) {
      setReportsError(err.message || 'Error al cargar reportes diarios.');
    } finally {
      setReportsLoading(false);
    }
  }, [session, reportSearch, reportSort, selectedReport]);

  // Carga de Estado del Pipeline
  const loadPipelineData = useCallback(async () => {
    if (!session) return;
    setPipelineLoading(true);
    setPipelineError(null);
    try {
      const res = await apiClient.getPipelineStatus();
      setPipelineStatus(res);
    } catch (err: any) {
      setPipelineError(err.message || 'Error al consultar estado del pipeline.');
    } finally {
      setPipelineLoading(false);
    }
  }, [session]);

  // Carga de Vista Admin
  const loadAdminData = useCallback(async () => {
    if (!session || session.role !== 'admin') return;
    setAdminLoading(true);
    setAdminError(null);
    try {
      const [qRes, pRes] = await Promise.all([
        apiClient.getQuarantine(),
        apiClient.getPipelineStatus(),
      ]);
      setQuarantine(qRes);
      setPipelineStatus(pRes);
    } catch (err: any) {
      setAdminError(err.message || 'Error al cargar auditoría administrativa.');
    } finally {
      setAdminLoading(false);
    }
  }, [session]);

  useEffect(() => {
    if (!session) return;
    if (activeTab === 'dashboard') loadDashboardData();
    if (activeTab === 'reports') loadReportsData();
    if (activeTab === 'pipeline') loadPipelineData();
    if (activeTab === 'admin' && session.role === 'admin') loadAdminData();
  }, [session, activeTab, loadDashboardData, loadReportsData, loadPipelineData, loadAdminData]);

  // Conexión SSE en Tiempo Real con reconexión exponencial
  useEffect(() => {
    if (!session) {
      setSseStatus('disconnected');
      return;
    }

    let retryAttempt = 0;
    let isMounted = true;

    const connectSse = () => {
      if (!isMounted) return;
      if (sseRef.current) {
        sseRef.current.close();
      }
      setSseStatus(retryAttempt === 0 ? 'connected' : 'reconnecting');
      const es = new EventSource(`/api/stream?token=${encodeURIComponent(session.token)}`);
      sseRef.current = es;

      es.onopen = () => {
        if (!isMounted) return;
        retryAttempt = 0;
        setSseStatus('connected');
      };

      es.addEventListener('seismic_update', (evt: MessageEvent) => {
        if (!isMounted) return;
        setSseStatus('connected');
        try {
          const payload = JSON.parse(evt.data);
          if (payload.kpis) {
            setKpis(payload.kpis);
          }
          if (Array.isArray(payload.latest_events) && payload.latest_events.length > 0) {
            setEvents((prev) => {
              const existingIds = new Set(prev.map((e) => e.id));
              const incoming: SeismicEvent[] = payload.latest_events
                .filter((e: SeismicEvent) => !existingIds.has(e.id))
                .map((e: SeismicEvent) => ({ ...e, isNew: true }));
              if (incoming.length > 0) {
                setLastLiveBanner(
                  `Nuevos eventos recibidos en vivo: ${incoming.length} sismo(s) actualizado(s)`
                );
                return [...incoming, ...prev].slice(0, 150);
              }
              return prev;
            });
          }
        } catch {
          // Ignorar payload parcial
        }
      });

      es.addEventListener('heartbeat', () => {
        if (!isMounted) return;
        setSseStatus('connected');
      });

      es.onerror = () => {
        if (!isMounted) return;
        es.close();
        setSseStatus('reconnecting');
        retryAttempt += 1;
        const delayMs = Math.min(15000, 2000 * Math.pow(1.6, retryAttempt));
        reconnectTimerRef.current = window.setTimeout(connectSse, delayMs);
      };
    };

    connectSse();

    return () => {
      isMounted = false;
      if (sseRef.current) {
        sseRef.current.close();
      }
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
    };
  }, [session]);

  const handleLoginSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoginLoading(true);
    setLoginError(null);
    try {
      const userSession = await apiClient.login(loginUser, loginPass);
      setSession(userSession);
      setActiveTab('dashboard');
    } catch (err: any) {
      setLoginError(err.message || 'No fue posible iniciar sesión.');
    } finally {
      setLoginLoading(false);
    }
  };

  const handleSendChat = async (questionText?: string) => {
    const q = (questionText ?? chatInput).trim();
    if (!q || chatLoading) return;
    if (!questionText) setChatInput('');

    const userMsg: ChatMessage = {
      id: `u_${Date.now()}`,
      sender: 'user',
      text: q,
      ts: new Date().toLocaleTimeString(),
    };
    setChatMessages((prev) => [...prev, userMsg]);
    setChatLoading(true);

    try {
      const reply = await apiClient.sendChatMessage(q);
      const agentMsg: ChatMessage = {
        id: `a_${Date.now()}`,
        sender: 'agent',
        text: reply.answer,
        meta: reply,
        ts: new Date().toLocaleTimeString(),
      };
      setChatMessages((prev) => [...prev, agentMsg]);
    } catch (err: any) {
      setChatMessages((prev) => [
        ...prev,
        {
          id: `err_${Date.now()}`,
          sender: 'agent',
          text: `⚠️ Error controlado: ${err.message || 'No fue posible procesar la consulta.'}`,
          ts: new Date().toLocaleTimeString(),
        },
      ]);
    } finally {
      setChatLoading(false);
    }
  };

  const handleTriggerBackfill = async () => {
    setAdminLoading(true);
    setAdminMessage(null);
    setAdminError(null);
    try {
      const res = await apiClient.triggerBackfill(backfillDays, backfillMinMag);
      setAdminMessage(res.message);
      await loadAdminData();
    } catch (err: any) {
      setAdminError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const handleRegenerateReport = async () => {
    setAdminLoading(true);
    setAdminMessage(null);
    setAdminError(null);
    try {
      const res = await apiClient.regenerateReport();
      setAdminMessage(res.message);
      await loadAdminData();
    } catch (err: any) {
      setAdminError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const formattedHourly = useMemo(() => {
    if (!stats?.hourly) return [];
    return stats.hourly.map((pt) => ({
      ...pt,
      horaCorta: new Date(pt.hora).toISOString().slice(5, 16).replace('T', ' '),
    }));
  }, [stats]);

  // ---------------------------------------------------------------------------
  // VISTA DE LOGIN (Rutas protegidas: sin sesión no se muestra ninguna ruta de datos)
  // ---------------------------------------------------------------------------
  if (!session) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col justify-center items-center p-4 relative overflow-hidden">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_30%_20%,rgba(249,115,22,0.12),transparent_45%),radial-gradient(circle_at_75%_80%,rgba(239,68,68,0.1),transparent_45%)] pointer-events-none" />
        <div className="max-w-md w-full bg-slate-900/95 backdrop-blur-xl border border-slate-800 rounded-3xl p-8 shadow-2xl relative z-10">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-orange-500 to-red-600 flex items-center justify-center shadow-lg shadow-orange-500/25">
              <Activity className="w-7 h-7 text-white" />
            </div>
            <div>
              <span className="text-xs font-bold uppercase tracking-widest text-orange-400">
                SuperApp Data Product
              </span>
              <h1 className="text-2xl font-extrabold text-white tracking-tight">SismoWatch</h1>
            </div>
          </div>

          <p className="text-sm text-slate-400 mb-6">
            Plataforma global de monitoreo sísmico casi en tiempo real. Arquitectura Medallion
            (USGS → Airflow → Spark → MinIO → PostgreSQL `curated`).
          </p>

          {loginError && (
            <div className="mb-5 p-3.5 rounded-xl bg-red-500/15 border border-red-500/40 text-red-200 text-xs flex items-start gap-2.5">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
              <span>{loginError}</span>
            </div>
          )}

          <form onSubmit={handleLoginSubmit} className="space-y-4">
            <div>
              <label className="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">
                Usuario
              </label>
              <input
                type="text"
                value={loginUser}
                onChange={(e) => setLoginUser(e.target.value)}
                required
                className="w-full px-4 py-2.5 rounded-xl bg-slate-950 border border-slate-800 text-slate-100 text-sm focus:outline-none focus:border-orange-500"
                placeholder="admin o viewer"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">
                Contraseña
              </label>
              <input
                type="password"
                value={loginPass}
                onChange={(e) => setLoginPass(e.target.value)}
                required
                className="w-full px-4 py-2.5 rounded-xl bg-slate-950 border border-slate-800 text-slate-100 text-sm focus:outline-none focus:border-orange-500"
                placeholder="••••••••"
              />
            </div>
            <button
              type="submit"
              disabled={loginLoading}
              className="w-full py-3 rounded-xl bg-gradient-to-r from-orange-500 to-red-600 hover:from-orange-400 hover:to-red-500 text-white font-bold text-sm shadow-lg shadow-orange-600/25 transition flex items-center justify-center gap-2 disabled:opacity-50"
            >
              <Lock className="w-4 h-4" />
              {loginLoading ? 'Autenticando con JWT + bcrypt...' : 'Iniciar Sesión Segura'}
            </button>
          </form>

          <div className="mt-6 pt-5 border-t border-slate-800">
            <p className="text-xs text-slate-400 mb-2.5 font-medium">
              Perfiles sembrados para demostración RBAC:
            </p>
            <div className="grid grid-cols-2 gap-2.5">
              <button
                type="button"
                onClick={() => {
                  setLoginUser('admin');
                  setLoginPass('AdminSismo2026!');
                }}
                className="px-3 py-2 rounded-xl bg-slate-800/80 hover:bg-slate-800 border border-slate-700 text-xs text-slate-200 font-medium transition text-left"
              >
                <div className="font-bold text-orange-400">Rol: admin</div>
                <div className="text-[11px] text-slate-400">Acceso total + Cuarentena</div>
              </button>
              <button
                type="button"
                onClick={() => {
                  setLoginUser('viewer');
                  setLoginPass('ViewerSismo2026!');
                }}
                className="px-3 py-2 rounded-xl bg-slate-800/80 hover:bg-slate-800 border border-slate-700 text-xs text-slate-200 font-medium transition text-left"
              >
                <div className="font-bold text-emerald-400">Rol: viewer</div>
                <div className="text-[11px] text-slate-400">Solo lectura en curated</div>
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ---------------------------------------------------------------------------
  // PLATAFORMA AUTENTICADA
  // ---------------------------------------------------------------------------
  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col">
      {/* Barra Superior */}
      <header className="sticky top-0 z-50 bg-slate-950/90 backdrop-blur-md border-b border-slate-800/80 px-6 py-3.5">
        <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-orange-500 to-red-600 flex items-center justify-center shadow-md shadow-orange-500/20">
              <Activity className="w-6 h-6 text-white" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-extrabold text-lg tracking-tight text-white">SismoWatch</span>
                <span className="text-[11px] px-2 py-0.5 rounded-full bg-orange-500/15 text-orange-400 border border-orange-500/30 font-semibold">
                  SuperApp
                </span>
              </div>
              <p className="text-xs text-slate-400">
                Catálogo Global USGS • Cadencia Micro-Batch 5m • Consumo SSE
              </p>
            </div>
          </div>

          {/* Indicador EN VIVO SSE */}
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex items-center gap-2.5 px-3.5 py-1.5 rounded-full bg-slate-900 border border-slate-800 text-xs">
              <span className="relative flex h-2.5 w-2.5">
                <span
                  className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${
                    sseStatus === 'connected'
                      ? 'bg-emerald-400'
                      : sseStatus === 'reconnecting'
                      ? 'bg-amber-400'
                      : 'bg-red-500'
                  }`}
                />
                <span
                  className={`relative inline-flex rounded-full h-2.5 w-2.5 ${
                    sseStatus === 'connected'
                      ? 'bg-emerald-500'
                      : sseStatus === 'reconnecting'
                      ? 'bg-amber-500'
                      : 'bg-red-500'
                  }`}
                />
              </span>
              <span className="font-bold tracking-wider text-slate-200">EN VIVO</span>
              <span className="text-slate-400">|</span>
              <span
                className={
                  sseStatus === 'connected'
                    ? 'text-emerald-400 font-medium'
                    : sseStatus === 'reconnecting'
                    ? 'text-amber-400 font-medium'
                    : 'text-red-400 font-medium'
                }
              >
                {sseStatus === 'connected'
                  ? 'Conectado (SSE)'
                  : sseStatus === 'reconnecting'
                  ? 'Reconectando...'
                  : 'Desconectado'}
              </span>
              {kpis?.last_updated && (
                <span className="text-slate-400 hidden sm:inline">
                  • Act. {new Date(kpis.last_updated).toLocaleTimeString()}
                </span>
              )}
            </div>

            {/* Perfil y Rol */}
            <div className="flex items-center gap-2 bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs">
              <span className="text-slate-300 font-medium">{session.username}</span>
              <span
                className={`px-2 py-0.5 rounded-md font-bold uppercase text-[10px] ${
                  session.role === 'admin'
                    ? 'bg-orange-500/20 text-orange-300 border border-orange-500/40'
                    : 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/40'
                }`}
              >
                {session.role}
              </span>
              <button
                onClick={handleLogout}
                title="Cerrar sesión"
                className="ml-1 text-slate-400 hover:text-red-400 transition"
              >
                <LogOut className="w-4 h-4" />
              </button>
            </div>
          </div>
        </div>

        {/* Navegación por Pestañas (Controles Admin NO se renderizan para viewer) */}
        <div className="max-w-7xl mx-auto mt-3 flex items-center gap-2 overflow-x-auto pb-1">
          <button
            onClick={() => setActiveTab('dashboard')}
            className={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition ${
              activeTab === 'dashboard'
                ? 'bg-orange-600 text-white shadow-md shadow-orange-600/25'
                : 'bg-slate-900 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            <Globe className="w-4 h-4" />
            Dashboard & Mapa
          </button>

          <button
            onClick={() => setActiveTab('reports')}
            className={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition ${
              activeTab === 'reports'
                ? 'bg-orange-600 text-white shadow-md shadow-orange-600/25'
                : 'bg-slate-900 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            <FileText className="w-4 h-4" />
            Reportes Diarios IA
          </button>

          <button
            onClick={() => setActiveTab('pipeline')}
            className={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition ${
              activeTab === 'pipeline'
                ? 'bg-orange-600 text-white shadow-md shadow-orange-600/25'
                : 'bg-slate-900 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            <Layers className="w-4 h-4" />
            Estado del Pipeline
          </button>

          <button
            onClick={() => setActiveTab('chat')}
            className={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition ${
              activeTab === 'chat'
                ? 'bg-orange-600 text-white shadow-md shadow-orange-600/25'
                : 'bg-slate-900 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            <Bot className="w-4 h-4" />
            Agente IA SismoWatch
          </button>

          {session.role === 'admin' && (
            <button
              onClick={() => setActiveTab('admin')}
              className={`inline-flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition ${
                activeTab === 'admin'
                  ? 'bg-red-600 text-white shadow-md shadow-red-600/25'
                  : 'bg-slate-900 text-orange-300 hover:bg-slate-800 border border-orange-500/30'
              }`}
            >
              <ShieldAlert className="w-4 h-4" />
              Panel Admin & Calidad
            </button>
          )}
        </div>
      </header>

      {/* Banner de actualización en vivo SSE */}
      {lastLiveBanner && (
        <div className="bg-orange-500/15 border-b border-orange-500/30 px-6 py-2 text-xs text-orange-200 flex items-center justify-between">
          <span className="flex items-center gap-2">
            <Radio className="w-3.5 h-3.5 text-orange-400 animate-pulse" />
            {lastLiveBanner}
          </span>
          <button
            onClick={() => setLastLiveBanner(null)}
            className="text-orange-300 hover:text-white underline"
          >
            Cerrar aviso
          </button>
        </div>
      )}

      {/* Contenido Principal */}
      <main className="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
        {/* ===================================================================
            PESTAÑA 1: DASHBOARD & MAPA
        =================================================================== */}
        {activeTab === 'dashboard' && (
          <>
            {/* Barra de Filtros */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-wrap items-end gap-4">
              <div className="flex items-center gap-2 text-xs font-bold uppercase tracking-wider text-orange-400 mr-2 self-center">
                <Filter className="w-4 h-4" />
                Filtros
              </div>
              <div>
                <label className="block text-[11px] text-slate-400 mb-1">Fecha Inicio</label>
                <input
                  type="date"
                  value={filterStartDate}
                  onChange={(e) => setFilterStartDate(e.target.value)}
                  className="px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                />
              </div>
              <div>
                <label className="block text-[11px] text-slate-400 mb-1">Fecha Fin</label>
                <input
                  type="date"
                  value={filterEndDate}
                  onChange={(e) => setFilterEndDate(e.target.value)}
                  className="px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                />
              </div>
              <div>
                <label className="block text-[11px] text-slate-400 mb-1">
                  Magnitud Mínima ({filterMinMag})
                </label>
                <input
                  type="number"
                  step="0.5"
                  min="-2"
                  max="9.5"
                  value={filterMinMag}
                  onChange={(e) => setFilterMinMag(e.target.value)}
                  className="w-28 px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                />
              </div>
              <div className="flex-1 min-w-[180px]">
                <label className="block text-[11px] text-slate-400 mb-1">Región</label>
                <input
                  type="text"
                  value={filterRegion}
                  onChange={(e) => setFilterRegion(e.target.value)}
                  placeholder="Ej. Alaska, Chile, Japan, California..."
                  className="w-full px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                />
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={loadDashboardData}
                  className="px-4 py-1.5 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-semibold transition flex items-center gap-1.5"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${dashLoading ? 'animate-spin' : ''}`} />
                  Actualizar
                </button>
                <button
                  onClick={() => {
                    setFilterStartDate('');
                    setFilterEndDate('');
                    setFilterMinMag('0');
                    setFilterRegion('');
                  }}
                  className="px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs transition"
                >
                  Limpiar
                </button>
              </div>
            </div>

            {dashError && (
              <div className="bg-red-500/15 border border-red-500/40 rounded-2xl p-4 flex items-center justify-between text-sm text-red-200">
                <span className="flex items-center gap-2">
                  <AlertTriangle className="w-5 h-5 text-red-400" />
                  {dashError}
                </span>
                <button
                  onClick={loadDashboardData}
                  className="px-3 py-1.5 rounded-xl bg-red-600 hover:bg-red-500 text-white text-xs font-semibold"
                >
                  Reintentar
                </button>
              </div>
            )}

            {/* 6 KPIs Obligatorios */}
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              {dashLoading && !kpis ? (
                Array.from({ length: 6 }).map((_, i) => (
                  <div
                    key={i}
                    className="h-24 rounded-2xl bg-slate-900 border border-slate-800 animate-pulse p-4"
                  />
                ))
              ) : (
                <>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Eventos (24h)</span>
                      <Activity className="w-4 h-4 text-orange-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-white">
                      {kpis?.events_24h ?? 0}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">
                      Total filtro: {totalEventsCount}
                    </div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Magnitud Máxima</span>
                      <TrendingUp className="w-4 h-4 text-red-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-red-400">
                      M {kpis?.max_magnitude?.toFixed(1) ?? '0.0'}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">Escala USGS</div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Magnitud Promedio</span>
                      <Activity className="w-4 h-4 text-amber-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-amber-300">
                      M {kpis?.avg_magnitude?.toFixed(2) ?? '0.00'}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">Media validada</div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Alertas Tsunami</span>
                      <Waves className="w-4 h-4 text-sky-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-sky-400">
                      {kpis?.tsunami_alerts ?? 0}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">Boletín NOAA/USGS</div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Puntaje Calidad</span>
                      <ShieldCheck className="w-4 h-4 text-emerald-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-emerald-400">
                      {kpis?.quality_score?.toFixed(1) ?? '100.0'}%
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">Compuerta Pandera</div>
                  </div>

                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Última Actualización</span>
                      <Clock className="w-4 h-4 text-purple-400" />
                    </div>
                    <div className="text-base font-bold text-white truncate">
                      {kpis?.last_updated
                        ? new Date(kpis.last_updated).toLocaleTimeString()
                        : 'Sincronizando'}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">
                      Frescura: {kpis?.freshness_minutes ?? 0} min
                    </div>
                  </div>
                </>
              )}
            </div>

            {/* Mapa Leaflet + Feed lateral */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-4">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h2 className="text-sm font-bold text-white flex items-center gap-2">
                      <MapPin className="w-4 h-4 text-orange-400" />
                      Mapa Sísmico Global Interactivo (Leaflet)
                    </h2>
                    <p className="text-xs text-slate-400">
                      Haz clic sobre un marcador o evento de la lista para inspeccionar su hipocentro.
                    </p>
                  </div>
                  <span className="text-xs text-slate-400 font-mono">
                    {events.length} marcadores en vista
                  </span>
                </div>
                <SeismicMap events={events} selectedEventId={selectedEventId} />
              </div>

              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-col h-[500px]">
                <div className="flex items-center justify-between mb-3">
                  <h2 className="text-sm font-bold text-white flex items-center gap-2">
                    <Radio className="w-4 h-4 text-orange-400" />
                    Últimos Eventos en Curated
                  </h2>
                  <span className="text-[11px] text-slate-400">{events.length} mostrados</span>
                </div>

                {events.length === 0 ? (
                  <div className="flex-1 flex flex-col items-center justify-center text-center text-slate-400 text-xs p-6">
                    <Globe className="w-8 h-8 text-slate-600 mb-2" />
                    No se encontraron eventos con los filtros seleccionados.
                  </div>
                ) : (
                  <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                    {events.slice(0, 60).map((ev) => (
                      <button
                        key={ev.id}
                        onClick={() => setSelectedEventId(ev.id)}
                        className={`w-full text-left p-3 rounded-xl border transition flex items-start gap-3 ${
                          ev.isNew ? 'animate-new-event' : ''
                        } ${
                          selectedEventId === ev.id
                            ? 'bg-orange-500/15 border-orange-500/50'
                            : 'bg-slate-950/70 hover:bg-slate-950 border-slate-800/80'
                        }`}
                      >
                        <div
                          className={`px-2 py-1 rounded-lg font-extrabold text-xs shrink-0 ${
                            ev.mag >= 6.0
                              ? 'bg-red-500 text-slate-950'
                              : ev.mag >= 5.0
                              ? 'bg-orange-500 text-slate-950'
                              : ev.mag >= 3.5
                              ? 'bg-amber-400 text-slate-950'
                              : 'bg-slate-800 text-amber-300'
                          }`}
                        >
                          M {ev.mag.toFixed(1)}
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className="text-xs font-semibold text-slate-100 truncate">
                            {ev.place}
                          </div>
                          <div className="text-[11px] text-slate-400 flex items-center justify-between mt-1">
                            <span>{ev.region}</span>
                            <span>{ev.depth_km.toFixed(0)} km prof.</span>
                          </div>
                          <div className="text-[10px] text-slate-500 mt-0.5">
                            {new Date(ev.time).toISOString().replace('T', ' ').slice(0, 16)} UTC
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* 3 Gráficos Recharts */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Serie Temporal por Hora */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                <h3 className="text-sm font-bold text-white mb-1">
                  Serie Temporal por Hora (`agg_hourly`)
                </h3>
                <p className="text-xs text-slate-400 mb-4">
                  Conteo horario de eventos sísmicos validados
                </p>
                <div className="h-60">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={formattedHourly}>
                      <defs>
                        <linearGradient id="colorConteo" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#f97316" stopOpacity={0.5} />
                          <stop offset="95%" stopColor="#f97316" stopOpacity={0.0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis dataKey="horaCorta" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          fontSize: '12px',
                        }}
                      />
                      <Area
                        type="monotone"
                        dataKey="conteo"
                        name="Eventos/hora"
                        stroke="#f97316"
                        fillOpacity={1}
                        fill="url(#colorConteo)"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Distribución por Magnitud */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                <h3 className="text-sm font-bold text-white mb-1">Distribución por Magnitud</h3>
                <p className="text-xs text-slate-400 mb-4">
                  Frecuencia de sismos según escala de magnitud
                </p>
                <div className="h-60">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={stats?.magnitude_distribution || []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis dataKey="rango" stroke="#94a3b8" fontSize={10} />
                      <YAxis stroke="#94a3b8" fontSize={10} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          fontSize: '12px',
                        }}
                      />
                      <Bar dataKey="conteo" name="Eventos" radius={[6, 6, 0, 0]}>
                        {(stats?.magnitude_distribution || []).map((_, index) => {
                          const colors = ['#fbbf24', '#f59e0b', '#f97316', '#ea580c', '#ef4444', '#dc2626'];
                          return <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />;
                        })}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Regiones con Más Actividad */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                <h3 className="text-sm font-bold text-white mb-1">
                  Regiones con Mayor Actividad (`agg_region`)
                </h3>
                <p className="text-xs text-slate-400 mb-4">Top regiones por número de eventos</p>
                <div className="h-60">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      layout="vertical"
                      data={(stats?.top_regions || []).slice(0, 7)}
                      margin={{ left: 10, right: 10 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis type="number" stroke="#94a3b8" fontSize={10} />
                      <YAxis
                        type="category"
                        dataKey="region"
                        stroke="#94a3b8"
                        fontSize={10}
                        width={105}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          fontSize: '12px',
                        }}
                      />
                      <Bar dataKey="conteo" name="Eventos" fill="#fb923c" radius={[0, 6, 6, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </>
        )}

        {/* ===================================================================
            PESTAÑA 2: REPORTES DIARIOS DE IA
        =================================================================== */}
        {activeTab === 'reports' && (
          <div className="space-y-6">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-wrap items-center justify-between gap-4">
              <div className="flex items-center gap-3 flex-1 min-w-[240px]">
                <div className="relative flex-1 max-w-md">
                  <Search className="w-4 h-4 text-slate-400 absolute left-3 top-2.5" />
                  <input
                    type="text"
                    value={reportSearch}
                    onChange={(e) => setReportSearch(e.target.value)}
                    placeholder="Buscar en reportes por región, fecha o proveedor..."
                    className="w-full pl-9 pr-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                  />
                </div>
                <select
                  value={reportSort}
                  onChange={(e) => setReportSort(e.target.value as 'desc' | 'asc')}
                  className="px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                >
                  <option value="desc">Más recientes primero</option>
                  <option value="asc">Más antiguos primero</option>
                </select>
                <button
                  onClick={loadReportsData}
                  className="px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-xs text-slate-200"
                >
                  Buscar
                </button>
              </div>

              <div className="flex items-center gap-2">
                <button
                  onClick={() => apiClient.downloadExport('csv')}
                  className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold transition"
                >
                  <Download className="w-3.5 h-3.5" />
                  Exportar CSV
                </button>
                <button
                  onClick={() => apiClient.downloadExport('html')}
                  className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-semibold transition"
                >
                  <Download className="w-3.5 h-3.5" />
                  Exportar HTML / PDF
                </button>
              </div>
            </div>

            {reportsError && (
              <div className="bg-red-500/15 border border-red-500/40 rounded-2xl p-4 text-xs text-red-200">
                {reportsError}
              </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3">
                <h2 className="text-sm font-bold text-white">
                  Historial de Reportes (`curated.daily_reports`)
                </h2>
                {reportsLoading ? (
                  <div className="h-40 rounded-xl bg-slate-950 animate-pulse" />
                ) : reports.length === 0 ? (
                  <div className="text-xs text-slate-400 p-6 text-center">
                    Aún no hay reportes generados para este criterio.
                  </div>
                ) : (
                  reports.map((rep) => (
                    <button
                      key={rep.fecha}
                      onClick={() => setSelectedReport(rep)}
                      className={`w-full text-left p-3.5 rounded-xl border transition ${
                        selectedReport?.fecha === rep.fecha
                          ? 'bg-orange-500/15 border-orange-500/50'
                          : 'bg-slate-950 border-slate-800 hover:bg-slate-900'
                      }`}
                    >
                      <div className="flex items-center justify-between text-xs font-bold text-white">
                        <span>{rep.fecha}</span>
                        <span className="px-2 py-0.5 rounded bg-slate-800 text-orange-300 font-mono text-[10px]">
                          {rep.proveedor}
                        </span>
                      </div>
                      <p className="text-xs text-slate-300 mt-1 line-clamp-2">
                        {rep.resumen_ejecutivo}
                      </p>
                      <div className="text-[10px] text-slate-500 mt-1.5 flex items-center justify-between">
                        <span>Prompt: {rep.version_prompt}</span>
                        <span>{rep.fallback ? 'Respaldo determinista' : 'LLM Primario'}</span>
                      </div>
                    </button>
                  ))
                )}
              </div>

              <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-6">
                {selectedReport ? (
                  <div className="space-y-4">
                    <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800 pb-4">
                      <div>
                        <span className="text-xs text-orange-400 font-bold uppercase">
                          Reporte Ejecutivo Validado • {selectedReport.fecha}
                        </span>
                        <h2 className="text-lg font-extrabold text-white mt-0.5">
                          {selectedReport.titulo}
                        </h2>
                      </div>
                      <div className="flex items-center gap-2 text-xs">
                        <span className="px-2.5 py-1 rounded-lg bg-slate-800 text-slate-200">
                          Proveedor: <strong>{selectedReport.proveedor}</strong>
                        </span>
                        <span className="px-2.5 py-1 rounded-lg bg-slate-800 text-slate-300">
                          {selectedReport.version_prompt}
                        </span>
                      </div>
                    </div>

                    <div className="p-4 rounded-xl bg-slate-950 border border-slate-800 text-sm text-slate-200 leading-relaxed">
                      {selectedReport.resumen_ejecutivo}
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="p-4 rounded-xl bg-slate-950 border border-slate-800">
                        <h3 className="text-xs font-bold uppercase text-orange-400 mb-2">
                          Zonas Más Activas
                        </h3>
                        <ul className="space-y-1.5 text-xs text-slate-300 list-disc list-inside">
                          {(selectedReport.json_validado.zonas_mas_activas || []).map(
                            (z: string, idx: number) => (
                              <li key={idx}>{z}</li>
                            )
                          )}
                        </ul>
                      </div>
                      <div className="p-4 rounded-xl bg-slate-950 border border-slate-800">
                        <h3 className="text-xs font-bold uppercase text-red-400 mb-2">
                          Eventos Destacados
                        </h3>
                        <ul className="space-y-1.5 text-xs text-slate-300 list-disc list-inside">
                          {(selectedReport.json_validado.eventos_destacados || []).map(
                            (e: string, idx: number) => (
                              <li key={idx}>{e}</li>
                            )
                          )}
                        </ul>
                      </div>
                    </div>

                    <div className="p-4 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-300 space-y-2">
                      <div>
                        <strong className="text-amber-400">Análisis de Anomalías: </strong>
                        {selectedReport.json_validado.analisis_anomalias}
                      </div>
                      <div>
                        <strong className="text-emerald-400">Recomendaciones: </strong>
                        {selectedReport.json_validado.recomendaciones}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="text-center text-slate-400 text-xs py-12">
                    Selecciona un reporte de la lista para visualizar su detalle completo.
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ===================================================================
            PESTAÑA 3: ESTADO DEL PIPELINE
        =================================================================== */}
        {activeTab === 'pipeline' && (
          <div className="space-y-6">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-wrap items-center justify-between gap-4">
              <div>
                <h2 className="text-base font-bold text-white flex items-center gap-2">
                  <Layers className="w-5 h-5 text-orange-400" />
                  Estado de Orquestación Apache Airflow y Frescura de Datos
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">
                  Vista adaptada al rol actual (<strong>{session.role}</strong>):{' '}
                  {session.role === 'admin'
                    ? 'Detalle administrativo completo habilitado.'
                    : 'Resumen operativo de frescura y DAGs.'}
                </p>
              </div>
              <button
                onClick={loadPipelineData}
                className="px-4 py-2 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-semibold flex items-center gap-1.5"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${pipelineLoading ? 'animate-spin' : ''}`} />
                Refrescar Estado
              </button>
            </div>

            {pipelineError && (
              <div className="bg-red-500/15 border border-red-500/40 rounded-2xl p-4 text-xs text-red-200">
                {pipelineError}
              </div>
            )}

            {pipelineStatus && (
              <>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Salud Global Pipeline</span>
                    <div className="mt-1 flex items-center gap-2">
                      <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                      <span className="text-lg font-extrabold text-white">
                        {pipelineStatus.overall_status}
                      </span>
                    </div>
                  </div>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Frescura del Dato</span>
                    <div className="mt-1 text-lg font-extrabold text-white">
                      {pipelineStatus.freshness_minutes} min
                    </div>
                  </div>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Puntaje Calidad Lote</span>
                    <div className="mt-1 text-lg font-extrabold text-emerald-400">
                      {pipelineStatus.quality_score}%
                    </div>
                  </div>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Versión Oro (`data_version`)</span>
                    <div className="mt-1 text-sm font-mono font-bold text-orange-300 truncate">
                      {pipelineStatus.data_version}
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {pipelineStatus.dags_summary.map((dag) => (
                    <div
                      key={dag.dag_id}
                      className="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-2"
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-sm font-bold text-white">{dag.dag_id}</span>
                        <span
                          className={`px-2.5 py-0.5 rounded-full text-[11px] font-bold ${
                            dag.estado === 'SUCCESS'
                              ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30'
                              : dag.estado === 'RUNNING'
                              ? 'bg-sky-500/20 text-sky-300 border border-sky-500/30'
                              : 'bg-red-500/20 text-red-300 border border-red-500/30'
                          }`}
                        >
                          {dag.estado}
                        </span>
                      </div>
                      <p className="text-xs text-slate-300">{dag.resultado}</p>
                      <div className="text-[11px] text-slate-400 flex items-center justify-between pt-1 border-t border-slate-800/80">
                        <span>Run: {dag.run_id}</span>
                        <span>
                          {dag.fin ? new Date(dag.fin).toLocaleString() : 'En ejecución'}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        )}

        {/* ===================================================================
            PESTAÑA 4: AGENTE CONVERSACIONAL DE IA
        =================================================================== */}
        {activeTab === 'chat' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-center gap-2.5">
                <Sparkles className="w-5 h-5 text-orange-400" />
                <h2 className="text-sm font-bold text-white">Arquitectura en Cascada (4 Niveles)</h2>
              </div>
              <div className="space-y-2 text-xs text-slate-300">
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <strong className="text-emerald-400">Nivel 0 (Sin LLM):</strong> Intents en Python y
                  FAQ extensa de 13 categorías (regex + fuzzy match).
                </div>
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <strong className="text-sky-400">Nivel 1 (Sin LLM):</strong> Caché exacta (SHA-256)
                  y semántica (`bge-m3` pgvector) ligada a `data_version`.
                </div>
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <strong className="text-orange-400">Nivel 2 (Con LLM + Skills):</strong> Ejecuta 1
                  de las 9 skills en SQL/Python y redacta verificando cifras.
                </div>
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <strong className="text-purple-400">Nivel 3 (Respaldo):</strong> Plantilla
                  determinista con datos exactos de la skill si el LLM falla.
                </div>
              </div>

              <div>
                <h3 className="text-xs font-bold uppercase text-slate-400 mb-2">
                  Consultas Rápidas de Prueba
                </h3>
                <div className="space-y-1.5">
                  {SUGGESTED_QUESTIONS.map((q, idx) => (
                    <button
                      key={idx}
                      onClick={() => handleSendChat(q)}
                      disabled={chatLoading}
                      className="w-full text-left px-3 py-2 rounded-xl bg-slate-950 hover:bg-slate-800 border border-slate-800 text-xs text-slate-200 transition"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col h-[580px]">
              <div className="flex-1 overflow-y-auto space-y-4 pr-2">
                {chatMessages.map((msg) => (
                  <div
                    key={msg.id}
                    className={`flex flex-col ${
                      msg.sender === 'user' ? 'items-end' : 'items-start'
                    }`}
                  >
                    <div
                      className={`max-w-[85%] rounded-2xl p-4 text-xs leading-relaxed whitespace-pre-wrap ${
                        msg.sender === 'user'
                          ? 'bg-orange-600 text-white'
                          : 'bg-slate-950 border border-slate-800 text-slate-100'
                      }`}
                    >
                      {msg.text}
                      {msg.meta && (
                        <div className="mt-3 pt-2.5 border-t border-slate-800 flex flex-wrap items-center gap-2 text-[10px] text-slate-400">
                          <span className="px-2 py-0.5 rounded bg-slate-900 text-orange-300 font-semibold border border-slate-700">
                            {msg.meta.level_label}
                          </span>
                          {msg.meta.skill_used && (
                            <span className="px-2 py-0.5 rounded bg-slate-900 text-sky-300 font-mono">
                              Skill: {msg.meta.skill_used}
                            </span>
                          )}
                          <span>Latencia: {msg.meta.latency_ms} ms</span>
                          <span>Versión: {msg.meta.data_version}</span>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
                {chatLoading && (
                  <div className="text-xs text-slate-400 animate-pulse">
                    Evaluando cascada N0 → N1 → N2 → N3...
                  </div>
                )}
              </div>

              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  handleSendChat();
                }}
                className="mt-4 flex items-center gap-2 pt-3 border-t border-slate-800"
              >
                <input
                  type="text"
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  placeholder="Pregunta sobre sismos, regiones, anomalías, FAQ o escribe SELECT ... FROM curated.events LIMIT 5"
                  className="flex-1 px-4 py-2.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-100 focus:outline-none focus:border-orange-500"
                />
                <button
                  type="submit"
                  disabled={chatLoading || !chatInput.trim()}
                  className="px-5 py-2.5 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-bold flex items-center gap-1.5 disabled:opacity-50"
                >
                  <Send className="w-3.5 h-3.5" />
                  Enviar
                </button>
              </form>
            </div>
          </div>
        )}

        {/* ===================================================================
            PESTAÑA 5: PANEL ADMIN (ESTRICTAMENTE OCULTO PARA VIEWER)
        =================================================================== */}
        {activeTab === 'admin' && session.role === 'admin' && (
          <div className="space-y-6">
            {adminMessage && (
              <div className="bg-emerald-500/15 border border-emerald-500/40 rounded-2xl p-4 text-xs text-emerald-200 flex items-center justify-between">
                <span>{adminMessage}</span>
                <button onClick={() => setAdminMessage(null)} className="underline">
                  Cerrar
                </button>
              </div>
            )}
            {adminError && (
              <div className="bg-red-500/15 border border-red-500/40 rounded-2xl p-4 text-xs text-red-200">
                {adminError}
              </div>
            )}

            {/* Acciones Administrativas */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <h2 className="text-sm font-bold text-white flex items-center gap-2">
                  <Database className="w-4 h-4 text-orange-400" />
                  Disparar Backfill Histórico (`usgs_backfill` vía Airflow REST)
                </h2>
                <p className="text-xs text-slate-400">
                  Descarga por tramos desde `fdsnws/event/1/query`, transforma en Spark con
                  sobrescritura dinámica de partición y hace upsert idempotente en `curated`.
                </p>
                <div className="flex items-end gap-3">
                  <div>
                    <label className="block text-[11px] text-slate-400 mb-1">Días (1-90)</label>
                    <input
                      type="number"
                      min="1"
                      max="90"
                      value={backfillDays}
                      onChange={(e) => setBackfillDays(Number(e.target.value))}
                      className="w-24 px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-white"
                    />
                  </div>
                  <div>
                    <label className="block text-[11px] text-slate-400 mb-1">Magnitud Mín.</label>
                    <input
                      type="number"
                      step="0.5"
                      value={backfillMinMag}
                      onChange={(e) => setBackfillMinMag(Number(e.target.value))}
                      className="w-24 px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-white"
                    />
                  </div>
                  <button
                    onClick={handleTriggerBackfill}
                    disabled={adminLoading}
                    className="px-4 py-2 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-bold flex items-center gap-1.5"
                  >
                    <Play className="w-3.5 h-3.5" />
                    Lanzar Backfill
                  </button>
                </div>
              </div>

              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <h2 className="text-sm font-bold text-white flex items-center gap-2">
                  <Sparkles className="w-4 h-4 text-orange-400" />
                  Regenerar Reporte Ejecutivo Diario de IA
                </h2>
                <p className="text-xs text-slate-400">
                  Recalcula las cifras SQL del día, verifica la no invención numérica y actualiza
                  `curated.daily_reports` además de disparar `usgs_ai_daily`.
                </p>
                <button
                  onClick={handleRegenerateReport}
                  disabled={adminLoading}
                  className="px-4 py-2 rounded-xl bg-red-600 hover:bg-red-500 text-white text-xs font-bold flex items-center gap-1.5"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${adminLoading ? 'animate-spin' : ''}`} />
                  Regenerar Reporte Diario Ahora
                </button>
              </div>
            </div>

            {/* Registro de Auditoría de Rechazos por Calidad (Sin tabla de cuarentena pesada) */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-sm font-bold text-white">
                    Auditoría de Calidad y Log de Rechazos (`curated.quality_results`)
                  </h2>
                  <p className="text-xs text-slate-400">
                    Sin tabla de cuarentena duplicada: el JSON crudo permanece inmutable en MinIO
                    Bronce y aquí se audita `{`{id, regla, motivo}`}` por lote.
                  </p>
                </div>
                <span className="px-3 py-1 rounded-xl bg-slate-950 border border-slate-800 text-xs text-orange-300 font-bold">
                  Total rechazados auditados: {quarantine?.total_rejected_rows ?? 0}
                </span>
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse text-xs">
                  <thead>
                    <tr className="border-b border-slate-800 text-slate-400">
                      <th className="py-2 px-3">Run ID</th>
                      <th className="py-2 px-3">Timestamp</th>
                      <th className="py-2 px-3">Evaluadas</th>
                      <th className="py-2 px-3">Aprobadas</th>
                      <th className="py-2 px-3">Rechazadas</th>
                      <th className="py-2 px-3">Puntaje</th>
                      <th className="py-2 px-3">Detalle de Rechazos (`id / regla / motivo`)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(quarantine?.batches || []).map((b) => (
                      <tr key={b.run_id} className="border-b border-slate-800/60">
                        <td className="py-2.5 px-3 font-mono text-slate-200">{b.run_id}</td>
                        <td className="py-2.5 px-3 text-slate-400">
                          {new Date(b.timestamp).toLocaleString()}
                        </td>
                        <td className="py-2.5 px-3">{b.reglas_evaluadas}</td>
                        <td className="py-2.5 px-3 text-emerald-400 font-semibold">
                          {b.aprobadas}
                        </td>
                        <td className="py-2.5 px-3 text-red-400 font-semibold">{b.rechazadas}</td>
                        <td className="py-2.5 px-3 font-bold">{b.puntaje}%</td>
                        <td className="py-2.5 px-3 text-slate-300">
                          {b.rechazos_log.length === 0 ? (
                            <span className="text-emerald-400">0 rechazos en este lote</span>
                          ) : (
                            <div className="space-y-1 max-h-28 overflow-y-auto">
                              {b.rechazos_log.slice(0, 5).map((r, i) => (
                                <div key={i} className="text-[11px] font-mono text-red-300">
                                  [{r.id}] <strong>{r.regla}</strong>: {r.motivo}
                                </div>
                              ))}
                            </div>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
};
