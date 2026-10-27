-- =============================================================================
-- 01_schemas.sql — Extensiones y Schemas de Base de Datos para SismoWatch
-- Decisión de arquitectura: Separamos la base en 3 schemas (raw, staging, curated)
-- para aislar permisos por rol y garantizar que la capa de servicio (API) solo
-- tenga visibilidad sobre datos validados en curated.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

