#!/usr/bin/env bash
# =============================================================================
# 06_openmetadata_db.sh — Crea la base de datos aislada `openmetadata_db` y el
# usuario `openmetadata_user` para el servidor de gobierno OpenMetadata, sin
# tocar ni alterar la base `sismowatch` ni `airflow_meta`.
# =============================================================================
set -e

OM_USER="${OM_DB_USER:-openmetadata_user}"
OM_PASS="${OM_DB_PASSWORD:-openmetadata_password}"
OM_DB="${OM_DB_NAME:-openmetadata_db}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${OM_USER}') THEN
            CREATE ROLE ${OM_USER} WITH LOGIN PASSWORD '${OM_PASS}';
        ELSE
            ALTER ROLE ${OM_USER} WITH LOGIN PASSWORD '${OM_PASS}';
        END IF;
    END
    \$\$;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tc "SELECT 1 FROM pg_database WHERE datname = '${OM_DB}'" | grep -q 1 || \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE ${OM_DB} OWNER ${OM_USER};"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${OM_DB}" -c "GRANT ALL ON SCHEMA public TO ${OM_USER}; ALTER SCHEMA public OWNER TO ${OM_USER};"

