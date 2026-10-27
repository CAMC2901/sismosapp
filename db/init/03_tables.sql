-- =============================================================================
-- 03_tables.sql — Modelo de Datos Completo (raw, staging, curated) + Índices + Permisos
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. SCHEMA: raw
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ingestions (
    ingestion_id VARCHAR(128) PRIMARY KEY,
    file_path TEXT NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id VARCHAR(128) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_ingestions_ts ON raw.ingestions (timestamp DESC);

-- -----------------------------------------------------------------------------
-- 2. SCHEMA: staging
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.events_silver (
    id VARCHAR(64) PRIMARY KEY,
    mag DOUBLE PRECISION NOT NULL,
    mag_type VARCHAR(32),
    place TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    updated TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    tsunami INTEGER NOT NULL DEFAULT 0,
    sig INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32),
    type VARCHAR(32),
    url TEXT,
    region VARCHAR(255) NOT NULL,
    event_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 3. SCHEMA: curated
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.events (
    id VARCHAR(64) PRIMARY KEY,
    mag DOUBLE PRECISION NOT NULL,
    mag_type VARCHAR(32),
    place TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    updated TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    tsunami INTEGER NOT NULL DEFAULT 0,
    sig INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32),
    type VARCHAR(32),
    url TEXT,
    region VARCHAR(255) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_events_time ON curated.events (time DESC);
CREATE INDEX IF NOT EXISTS idx_curated_events_mag ON curated.events (mag DESC);
CREATE INDEX IF NOT EXISTS idx_curated_events_region ON curated.events (region);
CREATE INDEX IF NOT EXISTS idx_curated_events_time_mag ON curated.events (time DESC, mag DESC);

CREATE TABLE IF NOT EXISTS curated.agg_hourly (
    hora TIMESTAMPTZ PRIMARY KEY,
    conteo INTEGER NOT NULL DEFAULT 0,
    mag_max DOUBLE PRECISION NOT NULL DEFAULT 0,
    mag_promedio DOUBLE PRECISION NOT NULL DEFAULT 0,
    tsunami INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_agg_hourly_hora ON curated.agg_hourly (hora DESC);

CREATE TABLE IF NOT EXISTS curated.agg_region (
    region VARCHAR(255) NOT NULL,
    fecha DATE NOT NULL,
    conteo INTEGER NOT NULL DEFAULT 0,
    mag_max DOUBLE PRECISION NOT NULL DEFAULT 0,
    mag_promedio DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (region, fecha)
);

CREATE INDEX IF NOT EXISTS idx_curated_agg_region_fecha ON curated.agg_region (fecha DESC);
CREATE INDEX IF NOT EXISTS idx_curated_agg_region_region ON curated.agg_region (region);

CREATE TABLE IF NOT EXISTS curated.quality_results (
    run_id VARCHAR(128) PRIMARY KEY,
    reglas_evaluadas INTEGER NOT NULL,
    aprobadas INTEGER NOT NULL,
    rechazadas INTEGER NOT NULL,
    puntaje DOUBLE PRECISION NOT NULL,
    conteo_por_regla JSONB NOT NULL DEFAULT '{}'::jsonb,
    rechazos_log JSONB NOT NULL DEFAULT '[]'::jsonb,
    frescura_ok BOOLEAN NOT NULL DEFAULT TRUE,
    frescura_minutos DOUBLE PRECISION DEFAULT 0,
    volumen_ok BOOLEAN NOT NULL DEFAULT TRUE,
    volumen_total INTEGER NOT NULL DEFAULT 0,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_quality_ts ON curated.quality_results (timestamp DESC);

CREATE TABLE IF NOT EXISTS curated.pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(128) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    estado VARCHAR(32) NOT NULL,
    inicio TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fin TIMESTAMPTZ,
    resultado TEXT,
    data_version VARCHAR(64),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_id, run_id)
);

CREATE INDEX IF NOT EXISTS idx_curated_pipeline_runs_dag_fin ON curated.pipeline_runs (dag_id, inicio DESC);

CREATE TABLE IF NOT EXISTS curated.anomalies (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(64) NOT NULL,
    ventana VARCHAR(128) NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    explicacion TEXT NOT NULL,
    datos_soporte JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tipo, ventana)
);

CREATE INDEX IF NOT EXISTS idx_curated_anomalies_detected ON curated.anomalies (detected_at DESC);

CREATE TABLE IF NOT EXISTS curated.daily_reports (
    fecha DATE PRIMARY KEY,
    json_validado JSONB NOT NULL,
    texto TEXT NOT NULL,
    proveedor VARCHAR(64) NOT NULL,
    fallback BOOLEAN NOT NULL DEFAULT FALSE,
    version_prompt VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_daily_reports_fecha ON curated.daily_reports (fecha DESC);

CREATE TABLE IF NOT EXISTS curated.ai_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(128),
    tarea VARCHAR(64) NOT NULL,
    modelo VARCHAR(64) NOT NULL,
    proveedor VARCHAR(64) NOT NULL,
    tokens_entrada INTEGER NOT NULL DEFAULT 0,
    tokens_salida INTEGER NOT NULL DEFAULT 0,
    latencia_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    costo_estimado DOUBLE PRECISION NOT NULL DEFAULT 0,
    exito BOOLEAN NOT NULL DEFAULT TRUE,
    fallback BOOLEAN NOT NULL DEFAULT FALSE,
    error_msg TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_ai_runs_created ON curated.ai_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS curated.ai_cache (
    clave_hash VARCHAR(128) PRIMARY KEY,
    pregunta_normalizada TEXT NOT NULL,
    data_version VARCHAR(64) NOT NULL,
    respuesta JSONB NOT NULL,
    embedding vector(1024),
    ttl INTEGER NOT NULL DEFAULT 3600,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_curated_ai_cache_data_version ON curated.ai_cache (data_version);
CREATE INDEX IF NOT EXISTS idx_curated_ai_cache_embedding_hnsw
    ON curated.ai_cache USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS curated.faq (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(64) NOT NULL,
    pregunta_canonica TEXT NOT NULL,
    patrones JSONB NOT NULL DEFAULT '[]'::jsonb,
    respuesta_plantilla TEXT NOT NULL,
    orden INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (categoria, pregunta_canonica)
);

CREATE TABLE IF NOT EXISTS curated.kb_chunks (
    id VARCHAR(64) PRIMARY KEY,
    fuente VARCHAR(64) NOT NULL,
    contenido TEXT NOT NULL,
    embedding vector(1024),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_kb_chunks_fuente ON curated.kb_chunks (fuente);
CREATE INDEX IF NOT EXISTS idx_curated_kb_chunks_embedding_hnsw
    ON curated.kb_chunks USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS curated.users (
    username VARCHAR(64) PRIMARY KEY,
    password_hash VARCHAR(255) NOT NULL,
    rol VARCHAR(32) NOT NULL CHECK (rol IN ('viewer', 'admin')),
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tabla de sesiones/auditoría de chat (permitida para api_rw_role según Sección 7)
CREATE TABLE IF NOT EXISTS curated.chat_sessions (
    id SERIAL PRIMARY KEY,
    username VARCHAR(64) NOT NULL,
    pregunta TEXT NOT NULL,
    nivel_resolucion INTEGER NOT NULL,
    skill_usada VARCHAR(64),
    proveedor VARCHAR(64),
    fallback BOOLEAN NOT NULL DEFAULT FALSE,
    latencia_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 4. ASIGNACIÓN DE PRIVILEGIOS MÍNIMOS POR ROL (Sección 7)
-- -----------------------------------------------------------------------------

-- ingest_role: escribe solo en raw (y actualiza estado de ejecución en pipeline_runs para callbacks)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA raw TO ingest_role;
GRANT USAGE ON SCHEMA curated TO ingest_role;
GRANT SELECT, INSERT, UPDATE ON curated.pipeline_runs TO ingest_role;
GRANT USAGE, SELECT ON SEQUENCE curated.pipeline_runs_id_seq TO ingest_role;

-- transform_role: lee raw, escribe staging y curated (publicación del oro y calidad)
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO transform_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA staging TO transform_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON curated.events, curated.agg_hourly, curated.agg_region, curated.quality_results, curated.pipeline_runs TO transform_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA curated TO transform_role;

-- ai_role: lee curated; escribe anomalies, daily_reports, ai_runs, kb_chunks (y pipeline_runs en callbacks)
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO ai_role;
GRANT INSERT, UPDATE, DELETE ON curated.anomalies, curated.daily_reports, curated.ai_runs, curated.kb_chunks, curated.pipeline_runs TO ai_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA curated TO ai_role;

-- reader_role: SOLO SELECT sobre curated; ningún acceso a raw ni staging
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM reader_role;
REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM reader_role;
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO reader_role;

-- api_rw_role: lectura en curated + escritura mínima SOLO sobre ai_cache, chat_sessions yai_runs
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM api_rw_role;
REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM api_rw_role;
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO api_rw_role;
GRANT INSERT, UPDATE, DELETE ON curated.ai_cache, curated.chat_sessions, curated.ai_runs TO api_rw_role;
GRANT USAGE, SELECT ON SEQUENCE curated.chat_sessions_id_seq, curated.ai_runs_id_seq TO api_rw_role;
