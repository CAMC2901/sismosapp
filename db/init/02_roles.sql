-- =============================================================================
-- 02_roles.sql — Roles de Base de Datos con Mínimo Privilegio (Sección 7)
-- Cada componente del sistema usa su propio rol con permisos estrictamente
-- acotados a su responsabilidad en la arquitectura Medallion.
-- Las contraseñas se sincronizan con las variables de .env en 05_apply_env.sh.
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ingest_role') THEN
        CREATE ROLE ingest_role WITH LOGIN PASSWORD 'sw_ingest_local_9812';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'transform_role') THEN
        CREATE ROLE transform_role WITH LOGIN PASSWORD 'sw_transform_local_9812';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ai_role') THEN
        CREATE ROLE ai_role WITH LOGIN PASSWORD 'sw_ai_local_9812';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader_role') THEN
        CREATE ROLE reader_role WITH LOGIN PASSWORD 'sw_reader_local_9812';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_rw_role') THEN
        CREATE ROLE api_rw_role WITH LOGIN PASSWORD 'sw_apirw_local_9812';
    END IF;
END
$$;

-- Revocar acceso por defecto de PUBLIC sobre raw y staging
REVOKE ALL ON SCHEMA raw FROM PUBLIC;
REVOKE ALL ON SCHEMA staging FROM PUBLIC;
REVOKE ALL ON SCHEMA curated FROM PUBLIC;

-- Revocar explícitamente todo acceso a raw y staging para reader_role y api_rw_role
REVOKE ALL ON SCHEMA raw FROM reader_role, api_rw_role, ai_role;
REVOKE ALL ON SCHEMA staging FROM reader_role, api_rw_role, ai_role, ingest_role;

-- Permisos de uso de schemas según rol
GRANT USAGE ON SCHEMA raw TO ingest_role, transform_role;
GRANT USAGE ON SCHEMA staging TO transform_role;
GRANT USAGE ON SCHEMA curated TO transform_role, ai_role, reader_role, api_rw_role;
