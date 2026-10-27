import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  Bot,
  CheckCircle2,
  Clock,
  Compass,
  Database,
  Download,
  FileText,
  Filter,
  Flame,
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
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
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
type TimeChartMode = 'daily' | 'hourly';

interface ChatMessage {
  id: string;
  sender: 'user' | 'agent';
  text: string;
  meta?: ChatReply;
  ts: string;
}

const SEISMIC_SUGGESTED_QUESTIONS = [
  '¿Cuál fue el último sismo registrado en el sistema?',
  '¿Hace cuántos minutos fue el sismo más reciente?',
  '¿Cuál ha sido el sismo más fuerte del día de hoy?',
  '¿Cuáles son las 3 regiones con más actividad sísmica hoy?',
  'Del top 5 de regiones más sísmicas hoy, ¿cuál tuvo el sismo más fuerte?',
  'Comparado con ayer, ¿hoy ha temblado más o menos?',
  '¿Cuál ha sido el pico de sismos por hora en esta semana?',
  '¿Hay algún enjambre sísmico activo en este momento?',
  '¿A qué hora se actualizó la base de datos por última vez?',
  '¿Cuál fue el puntaje de calidad en la última corrida de Airflow?',
];

function cleanInlineText(line: string): string {
  return line.replace(/\*\*+/g, '').replace(/__+/g, '').replace(/`+/g, '');
}

function renderFormattedChatText(rawText: string): React.ReactNode {
  const lines = (rawText || '').split('\n');
  const blocks: React.ReactNode[] = [];
  let i = 0;

  while (i < lines.length) {
    const trimmed = lines[i].trim();
    if (trimmed.startsWith('|') && trimmed.endsWith('|')) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].trim().startsWith('|') && lines[i].trim().endsWith('|')) {
        tableLines.push(lines[i].trim());
        i++;
      }
      if (tableLines.length >= 2) {
        const parseRow = (rowStr: string) =>
          rowStr
            .slice(1, -1)
            .split('|')
            .map((cell) => cleanInlineText(cell.trim()));

        const headers = parseRow(tableLines[0]);
        const dataRows = tableLines
          .slice(1)
          .filter((r) => !/^\|[\s:|-]+\|$/.test(r))
          .map(parseRow);

        blocks.push(
          <div
            key={`tbl_${i}`}
            className="my-2.5 overflow-x-auto rounded-xl border border-slate-700/80 bg-slate-900/90"
          >
            <table className="w-full text-left border-collapse text-xs">
              <thead>
                <tr className="bg-slate-800/90 text-orange-300 border-b border-slate-700">
                  {headers.map((h, hIdx) => (
                    <th key={hIdx} className="py-2 px-3 font-bold">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataRows.map((row, rIdx) => (
                  <tr
                    key={rIdx}
                    className="border-b border-slate-800/70 last:border-0 hover:bg-slate-800/40"
                  >
                    {row.map((cell, cIdx) => (
                      <td
                        key={cIdx}
                        className={`py-2 px-3 ${
                          cIdx === 0 ? 'font-semibold text-slate-100' : 'text-slate-300'
                        }`}
                      >
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
        continue;
      }
    }

    const cleanLine = cleanInlineText(lines[i]);
    if (cleanLine.trim() === '') {
      blocks.push(<div key={`sp_${i}`} className="h-2" />);
    } else {
      blocks.push(
        <p key={`ln_${i}`} className="leading-relaxed">
          {cleanLine}
        </p>
      );
    }
    i++;
  }

  return <div className="space-y-1">{blocks}</div>;
}

function buildWelcomeMessage(username?: string): ChatMessage[] {
  return [
    {
      id: `welcome_${Date.now()}`,
      sender: 'agent',
      text:
        `Hola, ${username || 'usuario'}. Soy el Asistente Sísmico de SismoWatch.\n\n` +
        `Puedes preguntarme en lenguaje natural sobre la actividad sísmica global y te entregaré tablas organizadas con estadísticas y métricas actualizadas:\n` +
        `| Tema de Consulta | Qué información puedo entregarte |\n` +
        `| :--- | :--- |\n` +
        `| Resumen y Estadísticas | Conteo de sismos (30 días y 24h), magnitudes, profundidades y alertas de tsunami |\n` +
        `| Terremotos Destacados | Ficha del sismo más fuerte y búsqueda por magnitud o país |\n` +
        `| Regiones y Tendencias | Zonas donde más tiembla, evolución por hora y enjambres sísmicos |\n` +
        `| Dudas Sismológicas | Qué significa la magnitud, profundidad hipocentral y alertas oceánicas |`,
      ts: new Date().toLocaleTimeString(),
    },
  ];
}

function getMagnitudeSeverityLabel(mag: number): string {
  if (mag >= 7.0) return 'Terremoto Mayor (7.0+)';
  if (mag >= 6.0) return 'Sismo Fuerte (6.0 - 6.9)';
  if (mag >= 5.0) return 'Sismo Moderado (5.0 - 5.9)';
  if (mag >= 4.0) return 'Sismo Ligero (4.0 - 4.9)';
  return 'Actividad Menor (< 4.0)';
}

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
  const [timeChartMode, setTimeChartMode] = useState<TimeChartMode>('daily');

  // Filtros del Dashboard
  const [filterStartDate, setFilterStartDate] = useState('');
  const [filterEndDate, setFilterEndDate] = useState('');
  const [filterMinMag, setFilterMinMag] = useState<string>('0');
  const [filterRegion, setFilterRegion] = useState('');

  // Tiempo Real
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

  // Estado del Monitoreo Operativo
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null);
  const [pipelineLoading, setPipelineLoading] = useState(false);
  const [pipelineError, setPipelineError] = useState<string | null>(null);

  // Estado Admin
  const [quarantine, setQuarantine] = useState<QuarantineData | null>(null);
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminMessage, setAdminMessage] = useState<string | null>(null);
  const [adminError, setAdminError] = useState<string | null>(null);
  const [backfillDays, setBackfillDays] = useState(30);
  const [backfillMinMag, setBackfillMinMag] = useState(2.5);

  // Estado del Asistente Sísmico
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>(() =>
    buildWelcomeMessage(session?.username)
  );
  const [chatInput, setChatInput] = useState('');
  const [chatLoading, setChatLoading] = useState(false);

  useEffect(() => {
    setChatMessages(buildWelcomeMessage(session?.username));
    setChatInput('');
    setQuarantine(null);
    setPipelineStatus(null);
    setAdminMessage(null);
    setAdminError(null);
  }, [session?.username, session?.role]);

  useEffect(() => {
    registerUnauthorizedInterceptor(() => {
      setSession(null);
      setChatMessages([]);
      setLoginError('Su sesión expiró. Por favor inicie sesión nuevamente.');
    });
  }, []);

  const handleLogout = useCallback(() => {
    if (sseRef.current) {
      sseRef.current.close();
      sseRef.current = null;
    }
    clearSession();
    setSession(null);
    setChatMessages([]);
    setChatInput('');
    setQuarantine(null);
    setPipelineStatus(null);
    setActiveTab('dashboard');
  }, []);

  // Carga sincronizada del Dashboard
  const loadDashboardData = useCallback(async () => {
    if (!session) return;
    setDashLoading(true);
    setDashError(null);
    try {
      const minMagNum = filterMinMag !== '' ? parseFloat(filterMinMag) : undefined;
      const commonFilters = {
        start_date: filterStartDate || undefined,
        end_date: filterEndDate || undefined,
        min_mag: minMagNum,
        region: filterRegion || undefined,
      };
      const [kpiRes, evRes, stRes] = await Promise.all([
        apiClient.getKpis(commonFilters),
        apiClient.getEvents({
          ...commonFilters,
          page: 1,
          page_size: 250,
        }),
        apiClient.getTimeseries({
          hours: 72,
          ...commonFilters,
        }),
      ]);
      setKpis(kpiRes);
      setEvents(evRes.items);
      setTotalEventsCount(evRes.total);
      setStats(stRes);
    } catch (err: any) {
      setDashError(err.message || 'Error al cargar los datos sísmicos.');
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
      setReportsError(err.message || 'Error al cargar los boletines diarios.');
    } finally {
      setReportsLoading(false);
    }
  }, [session, reportSearch, reportSort, selectedReport]);

  // Carga de Estado del Servicio
  const loadPipelineData = useCallback(async () => {
    if (!session) return;
    setPipelineLoading(true);
    setPipelineError(null);
    try {
      const res = await apiClient.getPipelineStatus();
      setPipelineStatus(res);
    } catch (err: any) {
      setPipelineError(err.message || 'Error al consultar el estado del servicio.');
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
      setAdminError(err.message || 'Error al cargar el panel de administración.');
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

  // Conexión en vivo
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
          if (payload.kpis && !filterStartDate && !filterEndDate && !filterRegion && filterMinMag === '0') {
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
                  `Actualización en vivo: ${incoming.length} nuevo(s) sismo(s) detectado(s)`
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
  }, [session, filterStartDate, filterEndDate, filterRegion, filterMinMag]);

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
          text: `No fue posible procesar la consulta en este momento: ${err.message || 'Intenta nuevamente.'}`,
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
    return stats.hourly.slice(-48).map((pt) => ({
      ...pt,
      etiqueta: new Date(pt.hora).toISOString().slice(5, 16).replace('T', ' ') + 'h',
    }));
  }, [stats]);

  const formattedDaily = useMemo(() => {
    if (!stats?.daily_trend) return [];
    return stats.daily_trend.map((pt) => ({
      ...pt,
      etiqueta: pt.fecha.slice(5), // MM-DD
    }));
  }, [stats]);

  const totalFilteredCount = kpis?.total_filtered_events ?? totalEventsCount;
  const shallowPct =
    totalFilteredCount > 0
      ? Math.round(((kpis?.shallow_events_count ?? 0) * 100) / totalFilteredCount)
      : 0;
  const strongPct =
    totalFilteredCount > 0
      ? (((kpis?.strong_events_count ?? 0) * 100) / totalFilteredCount).toFixed(1)
      : '0.0';

  // ---------------------------------------------------------------------------
  // VISTA DE LOGIN
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
                SuperApp • Monitoreo Global
              </span>
              <h1 className="text-2xl font-extrabold text-white tracking-tight">SismoWatch</h1>
            </div>
          </div>

          <p className="text-sm text-slate-400 mb-6">
            Plataforma de vigilancia sísmica global, análisis estadístico de terremotos, alertas de
            tsunami y asistente inteligente de consulta.
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
                placeholder="Ingresa tu usuario"
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
              {loginLoading ? 'Verificando acceso...' : 'Iniciar Sesión'}
            </button>
          </form>

          <div className="mt-6 pt-5 border-t border-slate-800">
            <p className="text-xs text-slate-400 mb-2.5 font-medium">
              Acceso rápido por perfil de usuario:
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
                <div className="font-bold text-orange-400">Administrador</div>
                <div className="text-[11px] text-slate-400">Gestión completa</div>
              </button>
              <button
                type="button"
                onClick={() => {
                  setLoginUser('viewer');
                  setLoginPass('ViewerSismo2026!');
                }}
                className="px-3 py-2 rounded-xl bg-slate-800/80 hover:bg-slate-800 border border-slate-700 text-xs text-slate-200 font-medium transition text-left"
              >
                <div className="font-bold text-emerald-400">Analista (Viewer)</div>
                <div className="text-[11px] text-slate-400">Consulta y análisis</div>
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ---------------------------------------------------------------------------
  // PLATAFORMA PRINCIPAL
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
                Centro de Monitoreo Sísmico Global • Servicio Geológico USGS
              </p>
            </div>
          </div>

          {/* Indicador EN VIVO */}
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
                  ? 'Monitoreo Activo'
                  : sseStatus === 'reconnecting'
                  ? 'Sincronizando...'
                  : 'Sin conexión'}
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
                {session.role === 'admin' ? 'Admin' : 'Analista'}
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

        {/* Navegación por Pestañas */}
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
            Mapa y Estadísticas Sísmicas
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
            Asistente Sísmico IA
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
            Boletines Sísmicos Diarios
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
            Estado de Actualización
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
              Administración
            </button>
          )}
        </div>
      </header>

      {/* Banner de actualización en vivo */}
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
            PESTAÑA 1: DASHBOARD SÍSMICO, MAPA Y GRÁFICAS AMPLIADAS
        =================================================================== */}
        {activeTab === 'dashboard' && (
          <>
            {/* Barra de Filtros */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-wrap items-end gap-4">
              <div className="flex items-center gap-2 text-xs font-bold uppercase tracking-wider text-orange-400 mr-2 self-center">
                <Filter className="w-4 h-4" />
                Filtrar Sismos
              </div>
              <div>
                <label className="block text-[11px] text-slate-400 mb-1">Fecha Inicial</label>
                <input
                  type="date"
                  value={filterStartDate}
                  onChange={(e) => setFilterStartDate(e.target.value)}
                  className="px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                />
              </div>
              <div>
                <label className="block text-[11px] text-slate-400 mb-1">Fecha Final</label>
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
              <div className="flex-1 min-w-[200px]">
                <label className="block text-[11px] text-slate-400 mb-1">País o Región</label>
                <input
                  type="text"
                  value={filterRegion}
                  onChange={(e) => setFilterRegion(e.target.value)}
                  placeholder="Ej. Alaska, Chile, Japan, Panama, California..."
                  className="w-full px-3 py-1.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-200"
                />
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={loadDashboardData}
                  className="px-4 py-1.5 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-semibold transition flex items-center gap-1.5"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${dashLoading ? 'animate-spin' : ''}`} />
                  Aplicar Filtro
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
                  Restablecer
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

            {/* 8 Tarjetas de Métricas y Estadísticas Sísmicas Sincronizadas */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {dashLoading && !kpis ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <div
                    key={i}
                    className="h-28 rounded-2xl bg-slate-900 border border-slate-800 animate-pulse p-4"
                  />
                ))
              ) : (
                <>
                  {/* 1. Total de Sismos en Vista */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Sismos Registrados</span>
                      <Activity className="w-4 h-4 text-orange-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-white">
                      {totalFilteredCount.toLocaleString()}
                    </div>
                    <div className="text-[11px] text-orange-300 mt-1 font-medium">
                      Últimas 24 horas: {kpis?.events_24h ?? 0} sismos
                    </div>
                  </div>

                  {/* 2. Magnitud Máxima */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Magnitud Máxima</span>
                      <TrendingUp className="w-4 h-4 text-red-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-red-400">
                      M {kpis?.max_magnitude?.toFixed(1) ?? '0.0'}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1 truncate">
                      {getMagnitudeSeverityLabel(kpis?.max_magnitude ?? 0)}
                    </div>
                  </div>

                  {/* 3. Magnitud Promedio y Mínima */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Magnitud Promedio</span>
                      <Activity className="w-4 h-4 text-amber-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-amber-300">
                      M {kpis?.avg_magnitude?.toFixed(2) ?? '0.00'}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">
                      Mínima registrada: M {kpis?.min_magnitude?.toFixed(2) ?? '0.00'}
                    </div>
                  </div>

                  {/* 4. Sismos Fuertes (M >= 5.0) */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Sismos Fuertes (M ≥ 5.0)</span>
                      <Flame className="w-4 h-4 text-orange-500" />
                    </div>
                    <div className="text-2xl font-extrabold text-orange-400">
                      {kpis?.strong_events_count ?? 0}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">
                      Representan el {strongPct}% del total
                    </div>
                  </div>

                  {/* 5. Profundidad Promedio y Máxima */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Profundidad Media</span>
                      <Layers className="w-4 h-4 text-indigo-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-indigo-300">
                      {kpis?.avg_depth_km?.toFixed(1) ?? '0.0'} km
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">
                      Profundidad máx.: {kpis?.max_depth_km?.toFixed(0) ?? '0'} km
                    </div>
                  </div>

                  {/* 6. Sismos Superficiales (< 70 km) */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Sismos Superficiales</span>
                      <Compass className="w-4 h-4 text-emerald-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-emerald-400">
                      {kpis?.shallow_events_count?.toLocaleString() ?? 0}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1">
                      {shallowPct}% a menos de 70 km de prof.
                    </div>
                  </div>

                  {/* 7. Región Más Activa */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Zona Más Activa</span>
                      <MapPin className="w-4 h-4 text-pink-400" />
                    </div>
                    <div
                      className="text-lg font-extrabold text-white truncate"
                      title={kpis?.most_active_region}
                    >
                      {kpis?.most_active_region || 'Global'}
                    </div>
                    <div className="text-[11px] text-pink-300 mt-1 font-medium">
                      {kpis?.most_active_region_count ?? 0} sismos en la zona
                    </div>
                  </div>

                  {/* 8. Alertas de Tsunami y Estado */}
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                      <span>Alertas de Tsunami</span>
                      <Waves className="w-4 h-4 text-sky-400" />
                    </div>
                    <div className="text-2xl font-extrabold text-sky-400">
                      {kpis?.tsunami_alerts ?? 0}
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1 flex items-center justify-between">
                      <span>Vigilancia oceánica</span>
                      <span className="text-emerald-400 font-semibold">
                        Calidad {kpis?.quality_score?.toFixed(0) ?? '100'}%
                      </span>
                    </div>
                  </div>
                </>
              )}
            </div>

            {/* Mapa Sísmico + Lista de Eventos */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-4">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h2 className="text-sm font-bold text-white flex items-center gap-2">
                      <MapPin className="w-4 h-4 text-orange-400" />
                      Mapa Sísmico Global Interactivo
                    </h2>
                    <p className="text-xs text-slate-400">
                      Haz clic en cualquier círculo del mapa o de la lista lateral para ver los
                      detalles del epicentro.
                    </p>
                  </div>
                  <span className="text-xs text-slate-300 font-semibold bg-slate-950 px-3 py-1 rounded-lg border border-slate-800">
                    {events.length} sismos en el mapa
                  </span>
                </div>
                <SeismicMap events={events} selectedEventId={selectedEventId} />
              </div>

              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-col h-[520px]">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h2 className="text-sm font-bold text-white flex items-center gap-2">
                      <Radio className="w-4 h-4 text-orange-400" />
                      Últimos Sismos Registrados
                    </h2>
                    <p className="text-[11px] text-slate-400">
                      Ordenados del más reciente al más antiguo
                    </p>
                  </div>
                  <span className="text-[11px] text-orange-300 font-semibold">
                    {totalFilteredCount.toLocaleString()} totales
                  </span>
                </div>

                {events.length === 0 ? (
                  <div className="flex-1 flex flex-col items-center justify-center text-center text-slate-400 text-xs p-6">
                    <Globe className="w-8 h-8 text-slate-600 mb-2" />
                    No se encontraron sismos con los filtros seleccionados.
                  </div>
                ) : (
                  <div className="flex-1 overflow-y-auto space-y-2 pr-1">
                    {events.slice(0, 80).map((ev) => (
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
                            <span>Prof. {ev.depth_km.toFixed(0)} km</span>
                          </div>
                          <div className="text-[10px] text-slate-500 mt-0.5 flex items-center justify-between">
                            <span>
                              {new Date(ev.time).toISOString().replace('T', ' ').slice(0, 16)} UTC
                            </span>
                            {ev.tsunami === 1 && (
                              <span className="text-sky-400 font-bold">🌊 Tsunami</span>
                            )}
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* ===============================================================
                SECCIÓN DE 4 GRÁFICAS GRANDES Y EXPLICATIVAS (2 COLUMNAS AMPLIAS)
            =============================================================== */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* GRÁFICA 1: Evolución Temporal (Diaria 30d o Por Hora 48h) + Magnitud Máxima */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5">
                <div className="flex flex-wrap items-start justify-between gap-2 mb-4">
                  <div>
                    <h3 className="text-base font-bold text-white">
                      1. Evolución de Sismos y Magnitud Máxima en el Tiempo
                    </h3>
                    <p className="text-xs text-slate-400 mt-0.5">
                      Compara la cantidad de sismos registrados (área naranja) con la magnitud
                      máxima alcanzada en cada periodo (línea roja).
                    </p>
                  </div>
                  <div className="flex items-center bg-slate-950 border border-slate-800 rounded-xl p-1 text-xs">
                    <button
                      type="button"
                      onClick={() => setTimeChartMode('daily')}
                      className={`px-3 py-1 rounded-lg font-semibold transition ${
                        timeChartMode === 'daily'
                          ? 'bg-orange-600 text-white'
                          : 'text-slate-400 hover:text-slate-200'
                      }`}
                    >
                      Por Día (30d)
                    </button>
                    <button
                      type="button"
                      onClick={() => setTimeChartMode('hourly')}
                      className={`px-3 py-1 rounded-lg font-semibold transition ${
                        timeChartMode === 'hourly'
                          ? 'bg-orange-600 text-white'
                          : 'text-slate-400 hover:text-slate-200'
                      }`}
                    >
                      Por Hora (48h)
                    </button>
                  </div>
                </div>

                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart
                      data={timeChartMode === 'daily' ? formattedDaily : formattedHourly}
                      margin={{ top: 10, right: 15, left: 0, bottom: 10 }}
                    >
                      <defs>
                        <linearGradient id="colorSismos" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#f97316" stopOpacity={0.55} />
                          <stop offset="95%" stopColor="#f97316" stopOpacity={0.05} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis
                        dataKey="etiqueta"
                        stroke="#94a3b8"
                        fontSize={11}
                        tickMargin={8}
                        minTickGap={20}
                      />
                      <YAxis
                        yAxisId="left"
                        stroke="#fb923c"
                        fontSize={11}
                        label={{
                          value: 'Cantidad de Sismos',
                          angle: -90,
                          position: 'insideLeft',
                          fill: '#fb923c',
                          fontSize: 11,
                        }}
                      />
                      <YAxis
                        yAxisId="right"
                        orientation="right"
                        domain={[0, 9]}
                        stroke="#f87171"
                        fontSize={11}
                        label={{
                          value: 'Magnitud Máx. (M)',
                          angle: 90,
                          position: 'insideRight',
                          fill: '#f87171',
                          fontSize: 11,
                        }}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          borderRadius: '12px',
                          fontSize: '12px',
                        }}
                      />
                      <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
                      <Area
                        yAxisId="left"
                        type="monotone"
                        dataKey="conteo"
                        name="Cantidad de Sismos"
                        stroke="#f97316"
                        strokeWidth={2}
                        fillOpacity={1}
                        fill="url(#colorSismos)"
                      />
                      <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="mag_max"
                        name="Magnitud Máxima (M)"
                        stroke="#ef4444"
                        strokeWidth={2.5}
                        dot={{ r: 3, fill: '#ef4444' }}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* GRÁFICA 2: Distribución de Sismos por Escala de Magnitud */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5">
                <div className="mb-4">
                  <h3 className="text-base font-bold text-white">
                    2. Frecuencia de Sismos según Escala de Magnitud
                  </h3>
                  <p className="text-xs text-slate-400 mt-0.5">
                    Clasifica los sismos desde microsismos menores a M 3.0 hasta terremotos fuertes
                    y mayores (M 6.0+).
                  </p>
                </div>

                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={stats?.magnitude_distribution || []}
                      margin={{ top: 10, right: 15, left: 0, bottom: 15 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis
                        dataKey="rango"
                        stroke="#cbd5e1"
                        fontSize={11}
                        tickMargin={8}
                      />
                      <YAxis
                        stroke="#94a3b8"
                        fontSize={11}
                        label={{
                          value: 'Número de Sismos',
                          angle: -90,
                          position: 'insideLeft',
                          fill: '#94a3b8',
                          fontSize: 11,
                        }}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          borderRadius: '12px',
                          fontSize: '12px',
                        }}
                        formatter={(value: any) => [`${value} sismos`, 'Cantidad']}
                      />
                      <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
                      <Bar
                        dataKey="conteo"
                        name="Cantidad de Sismos por Rango"
                        radius={[8, 8, 0, 0]}
                      >
                        {(stats?.magnitude_distribution || []).map((_, index) => {
                          const colors = [
                            '#38bdf8',
                            '#fbbf24',
                            '#f59e0b',
                            '#f97316',
                            '#ef4444',
                            '#b91c1c',
                          ];
                          return <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />;
                        })}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* GRÁFICA 3: Regiones Geográficas con Mayor Actividad y su Magnitud */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5">
                <div className="mb-4">
                  <h3 className="text-base font-bold text-white">
                    3. Zonas Geográficas con Mayor Actividad Sísmica
                  </h3>
                  <p className="text-xs text-slate-400 mt-0.5">
                    Ranking de las 8 regiones con mayor cantidad de sismos registrados y su magnitud
                    máxima alcanzada.
                  </p>
                </div>

                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart
                      layout="vertical"
                      data={(stats?.top_regions || []).slice(0, 8)}
                      margin={{ top: 5, right: 25, left: 15, bottom: 5 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis type="number" stroke="#94a3b8" fontSize={11} />
                      <YAxis
                        type="category"
                        dataKey="region"
                        stroke="#e2e8f0"
                        fontSize={11}
                        width={135}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          borderRadius: '12px',
                          fontSize: '12px',
                        }}
                        formatter={(val: any, name: string) => {
                          if (name.includes('Magnitud')) return [`M ${val}`, name];
                          return [`${val} sismos`, name];
                        }}
                      />
                      <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
                      <Bar
                        dataKey="conteo"
                        name="Total de Sismos"
                        fill="#fb923c"
                        radius={[0, 8, 8, 0]}
                        barSize={20}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* GRÁFICA 4: Clasificación por Profundidad Hipocentral (Superficial, Intermedio, Profundo) */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between">
                <div>
                  <h3 className="text-base font-bold text-white">
                    4. Distribución por Profundidad del Sismo (Hipocentro)
                  </h3>
                  <p className="text-xs text-slate-400 mt-0.5 mb-4">
                    Los sismos superficiales (0 a 70 km) generan mayor sacudida en la superficie que
                    los intermedios o profundos.
                  </p>
                </div>

                <div className="h-52">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={stats?.depth_distribution || []}
                      margin={{ top: 5, right: 15, left: 0, bottom: 5 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis dataKey="categoria" stroke="#cbd5e1" fontSize={12} />
                      <YAxis stroke="#94a3b8" fontSize={11} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0f172a',
                          borderColor: '#334155',
                          borderRadius: '12px',
                          fontSize: '12px',
                        }}
                        formatter={(value: any) => [`${value} sismos`, 'Cantidad']}
                      />
                      <Bar dataKey="conteo" name="Cantidad de Sismos" radius={[8, 8, 0, 0]} barSize={48}>
                        {(stats?.depth_distribution || []).map((_, idx) => {
                          const depthColors = ['#10b981', '#6366f1', '#a855f7'];
                          return <Cell key={`dp-${idx}`} fill={depthColors[idx % depthColors.length]} />;
                        })}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Tarjetas de resumen por categoría de profundidad */}
                <div className="grid grid-cols-3 gap-3 mt-3 pt-3 border-t border-slate-800">
                  {(stats?.depth_distribution || []).map((d, idx) => (
                    <div
                      key={idx}
                      className="bg-slate-950 border border-slate-800/90 rounded-xl p-2.5 text-xs"
                    >
                      <div className="font-bold text-slate-200">{d.categoria}</div>
                      <div className="text-[10px] text-slate-400">{d.rango_km}</div>
                      <div className="text-sm font-extrabold text-orange-400 mt-1">
                        {d.conteo} ({d.porcentaje}%)
                      </div>
                      <div className="text-[10px] text-slate-400 mt-0.5">
                        Máx. M {d.mag_max.toFixed(1)}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </>
        )}

        {/* ===================================================================
            PESTAÑA 2: ASISTENTE SÍSMICO IA (LIMPIO Y ENFOCADO EN SISMOS/DATOS)
        =================================================================== */}
        {activeTab === 'chat' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <Sparkles className="w-5 h-5 text-orange-400" />
                  <h2 className="text-sm font-bold text-white">Asistente Sísmico SismoWatch</h2>
                </div>
                <span className="px-2.5 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">
                  En Línea
                </span>
              </div>

              <p className="text-xs text-slate-300 leading-relaxed">
                Consulta en lenguaje natural (formal o coloquial) sobre terremotos recientes,
                regiones con mayor actividad, profundidades, alertas de tsunami y tablas
                estadísticas sincronizadas con el mapa.
              </p>

              <div className="grid grid-cols-2 gap-2 text-[11px]">
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <div className="font-bold text-orange-400">Estadísticas</div>
                  <div className="text-slate-400 mt-0.5">Tablas por magnitud y profundidad</div>
                </div>
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <div className="font-bold text-red-400">Terremotos</div>
                  <div className="text-slate-400 mt-0.5">El sismo más fuerte y búsquedas</div>
                </div>
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <div className="font-bold text-amber-400">Zonas Activas</div>
                  <div className="text-slate-400 mt-0.5">Ranking de países y regiones</div>
                </div>
                <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800">
                  <div className="font-bold text-sky-400">Tendencias</div>
                  <div className="text-slate-400 mt-0.5">Actividad por hora y enjambres</div>
                </div>
              </div>

              <div>
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-xs font-bold uppercase text-slate-400">
                    Preguntas Frecuentes Sugeridas
                  </h3>
                  <button
                    type="button"
                    onClick={() => setChatMessages(buildWelcomeMessage(session.username))}
                    className="text-[11px] text-orange-400 hover:underline"
                  >
                    Limpiar conversación
                  </button>
                </div>
                <div className="space-y-1.5">
                  {SEISMIC_SUGGESTED_QUESTIONS.map((q, idx) => (
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

            <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col h-[600px]">
              <div className="flex-1 overflow-y-auto space-y-4 pr-2">
                {chatMessages.map((msg) => (
                  <div
                    key={msg.id}
                    className={`flex flex-col ${
                      msg.sender === 'user' ? 'items-end' : 'items-start'
                    }`}
                  >
                    <div
                      className={`max-w-[90%] rounded-2xl p-4 text-xs leading-relaxed ${
                        msg.sender === 'user'
                          ? 'bg-orange-600 text-white whitespace-pre-wrap'
                          : 'bg-slate-950 border border-slate-800 text-slate-100'
                      }`}
                    >
                      {msg.sender === 'agent' ? renderFormattedChatText(msg.text) : msg.text}
                      <div className="mt-2 text-[10px] opacity-60 text-right">{msg.ts}</div>
                    </div>
                  </div>
                ))}
                {chatLoading && (
                  <div className="text-xs text-slate-400 animate-pulse">
                    Consultando datos sísmicos y preparando tabla...
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
                  placeholder="Pregunta sobre sismos, estadísticas, regiones activas, magnitudes o tsunamis..."
                  className="flex-1 px-4 py-2.5 rounded-xl bg-slate-950 border border-slate-800 text-xs text-slate-100 focus:outline-none focus:border-orange-500"
                />
                <button
                  type="submit"
                  disabled={chatLoading || !chatInput.trim()}
                  className="px-5 py-2.5 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-bold flex items-center gap-1.5 disabled:opacity-50"
                >
                  <Send className="w-3.5 h-3.5" />
                  Preguntar
                </button>
              </form>
            </div>
          </div>
        )}

        {/* ===================================================================
            PESTAÑA 3: BOLETINES SÍSMICOS DIARIOS
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
                    placeholder="Buscar boletín por región o fecha..."
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
                  Descargar CSV
                </button>
                <button
                  onClick={() => apiClient.downloadExport('html')}
                  className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-semibold transition"
                >
                  <Download className="w-3.5 h-3.5" />
                  Descargar Reporte Imprimible
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
                <h2 className="text-sm font-bold text-white">Historial de Boletines Sísmicos</h2>
                {reportsLoading ? (
                  <div className="h-40 rounded-xl bg-slate-950 animate-pulse" />
                ) : reports.length === 0 ? (
                  <div className="text-xs text-slate-400 p-6 text-center">
                    No hay boletines disponibles para este criterio.
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
                        <span>Corte del {rep.fecha}</span>
                        <span className="px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 text-[10px]">
                          Verificado
                        </span>
                      </div>
                      <p className="text-xs text-slate-300 mt-1 line-clamp-2">
                        {cleanInlineText(rep.resumen_ejecutivo)}
                      </p>
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
                          Boletín Sísmico Diario • {selectedReport.fecha}
                        </span>
                        <h2 className="text-lg font-extrabold text-white mt-0.5">
                          {selectedReport.titulo}
                        </h2>
                      </div>
                    </div>

                    <div className="p-4 rounded-xl bg-slate-950 border border-slate-800 text-sm text-slate-200 leading-relaxed">
                      {renderFormattedChatText(selectedReport.resumen_ejecutivo)}
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
                          Sismos Destacados
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
                        <strong className="text-amber-400">Comportamiento y Anomalías: </strong>
                        {selectedReport.json_validado.analisis_anomalias}
                      </div>
                      <div>
                        <strong className="text-emerald-400">Recomendación de Vigilancia: </strong>
                        {selectedReport.json_validado.recomendaciones}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="text-center text-slate-400 text-xs py-12">
                    Selecciona un boletín de la lista para visualizar su detalle completo.
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ===================================================================
            PESTAÑA 4: ESTADO DE ACTUALIZACIÓN DEL SERVICIO
        =================================================================== */}
        {activeTab === 'pipeline' && (
          <div className="space-y-6">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-wrap items-center justify-between gap-4">
              <div>
                <h2 className="text-base font-bold text-white flex items-center gap-2">
                  <Layers className="w-5 h-5 text-orange-400" />
                  Estado Operativo y Frescura de Datos Sísmicos
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">
                  Monitoreo automático del catálogo sísmico global cada 5 minutos.
                </p>
              </div>
              <button
                onClick={loadPipelineData}
                className="px-4 py-2 rounded-xl bg-orange-600 hover:bg-orange-500 text-white text-xs font-semibold flex items-center gap-1.5"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${pipelineLoading ? 'animate-spin' : ''}`} />
                Actualizar Estado
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
                    <span className="text-xs text-slate-400">Estado del Servicio</span>
                    <div className="mt-1 flex items-center gap-2">
                      <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                      <span className="text-lg font-extrabold text-white">
                        {pipelineStatus.overall_status === 'HEALTHY' ? 'Operativo' : pipelineStatus.overall_status}
                      </span>
                    </div>
                  </div>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Antigüedad del Último Corte</span>
                    <div className="mt-1 text-lg font-extrabold text-white">
                      {pipelineStatus.freshness_minutes} min
                    </div>
                  </div>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Calidad de Datos Validados</span>
                    <div className="mt-1 text-lg font-extrabold text-emerald-400">
                      {pipelineStatus.quality_score}%
                    </div>
                  </div>
                  <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4">
                    <span className="text-xs text-slate-400">Frecuencia de Sincronización</span>
                    <div className="mt-1 text-sm font-bold text-orange-300">
                      Cada 5 minutos
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
                        <span className="text-sm font-bold text-white">{dag.dag_id}</span>
                        <span
                          className={`px-2.5 py-0.5 rounded-full text-[11px] font-bold ${
                            dag.estado === 'SUCCESS'
                              ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30'
                              : dag.estado === 'RUNNING'
                              ? 'bg-sky-500/20 text-sky-300 border border-sky-500/30'
                              : 'bg-red-500/20 text-red-300 border border-red-500/30'
                          }`}
                        >
                          {dag.estado === 'SUCCESS' ? 'Completado' : dag.estado}
                        </span>
                      </div>
                      <p className="text-xs text-slate-300">{dag.resultado}</p>
                      <div className="text-[11px] text-slate-400 flex items-center justify-between pt-1 border-t border-slate-800/80">
                        <span>Última ejecución</span>
                        <span>
                          {dag.fin ? new Date(dag.fin).toLocaleString() : 'En proceso'}
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
            PESTAÑA 5: PANEL DE ADMINISTRACIÓN (SOLO ROL ADMIN)
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

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <h2 className="text-sm font-bold text-white flex items-center gap-2">
                  <Database className="w-4 h-4 text-orange-400" />
                  Recargar Catálogo Histórico de Sismos
                </h2>
                <p className="text-xs text-slate-400">
                  Actualiza el historial sísmico desde USGS para la cantidad de días y magnitud
                  mínima seleccionadas.
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
                    Ejecutar Recarga
                  </button>
                </div>
              </div>

              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <h2 className="text-sm font-bold text-white flex items-center gap-2">
                  <Sparkles className="w-4 h-4 text-orange-400" />
                  Actualizar Boletín Sísmico Diario
                </h2>
                <p className="text-xs text-slate-400">
                  Recalcula las estadísticas del día y genera una nueva versión del boletín
                  ejecutivo diario.
                </p>
                <button
                  onClick={handleRegenerateReport}
                  disabled={adminLoading}
                  className="px-4 py-2 rounded-xl bg-red-600 hover:bg-red-500 text-white text-xs font-bold flex items-center gap-1.5"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${adminLoading ? 'animate-spin' : ''}`} />
                  Actualizar Boletín Ahora
                </button>
              </div>
            </div>

            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-sm font-bold text-white">
                    Control de Calidad de Lotes Recientes
                  </h2>
                  <p className="text-xs text-slate-400">
                    Resumen de validaciones aplicadas a los datos sísmicos recibidos.
                  </p>
                </div>
                <span className="px-3 py-1 rounded-xl bg-slate-950 border border-slate-800 text-xs text-orange-300 font-bold">
                  Registros descartados: {quarantine?.total_rejected_rows ?? 0}
                </span>
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse text-xs">
                  <thead>
                    <tr className="border-b border-slate-800 text-slate-400">
                      <th className="py-2 px-3">Lote</th>
                      <th className="py-2 px-3">Fecha y Hora</th>
                      <th className="py-2 px-3">Evaluadas</th>
                      <th className="py-2 px-3">Aprobadas</th>
                      <th className="py-2 px-3">Descartadas</th>
                      <th className="py-2 px-3">Puntaje</th>
                      <th className="py-2 px-3">Observaciones</th>
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
                            <span className="text-emerald-400">100% válido</span>
                          ) : (
                            <div className="space-y-1 max-h-28 overflow-y-auto">
                              {b.rechazos_log.slice(0, 5).map((r, i) => (
                                <div key={i} className="text-[11px] text-red-300">
                                  [{r.id}] {r.regla}: {r.motivo}
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
