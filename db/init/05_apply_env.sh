#!/usr/bin/env bash
# =============================================================================
# 05_apply_env.sh — Sincroniza credenciales de roles de BD, base de metadatos
# de Airflow y usuarios sembrados (admin/viewer) desde variables de entorno (.env)
# =============================================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ALTER ROLE ingest_role WITH PASSWORD '${DB_INGEST_PASSWORD:-sw_ingest_local_9812}';
    ALTER ROLE transform_role WITH PASSWORD '${DB_TRANSFORM_PASSWORD:-sw_transform_local_9812}';
    ALTER ROLE ai_role WITH PASSWORD '${DB_AI_PASSWORD:-sw_ai_local_9812}';
    ALTER ROLE reader_role WITH PASSWORD '${DB_READER_PASSWORD:-sw_reader_local_9812}';
    ALTER ROLE api_rw_role WITH PASSWORD '${DB_API_RW_PASSWORD:-sw_apirw_local_9812}';

    INSERT INTO curated.users (username, password_hash, rol, activo)
    VALUES
        ('${ADMIN_USERNAME:-admin}', crypt('${ADMIN_PASSWORD:-AdminSismo2026!}', gen_salt('bf', 12)), 'admin', TRUE),
        ('${VIEWER_USERNAME:-viewer}', crypt('${VIEWER_PASSWORD:-ViewerSismo2026!}', gen_salt('bf', 12)), 'viewer', TRUE)
    ON CONFLICT (username) DO UPDATE
    SET password_hash = EXCLUDED.password_hash,
        rol = EXCLUDED.rol,
        activo = TRUE;
EOSQL

# Crear base de datos separada para los metadatos internos de Apache Airflow
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tc "SELECT 1 FROM pg_database WHERE datname = 'airflow_meta'" | grep -q 1 || \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE airflow_meta OWNER \"$POSTGRES_USER\";"

