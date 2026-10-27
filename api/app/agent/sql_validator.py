"""
Validador y ejecutor de SQL seguro para la skill `consulta_sql_segura` (Sección 11).

Reglas obligatorias:
1. Solo sentencias `SELECT` (sin DDL/DML ni llamadas a procedimientos).
2. Rechaza múltiples sentencias (`;` intermedio) y comentarios sospechosos (`--`, `/*`, `*/`).
3. Solo permite consultar tablas autorizadas del schema `curated` (prohibido `raw`, `staging`,
   `pg_catalog`, `information_schema` y `curated.users`).
4. Exige `LIMIT` obligatorio (máximo 100 filas).
5. Ejecuta con `reader_role` y `statement_timeout` explícito de 3000 ms.
"""
import re
from typing import Any, Dict, List
from app.core.config import get_db_conn
from app.core.exceptions import UnsafeQueryError

FORBIDDEN_KEYWORDS_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY|CALL|DO|EXECUTE|PREPARE|"
    r"PG_SLEEP|PG_READ_FILE|LO_IMPORT|LO_EXPORT|SET\s+ROLE|SET\s+SESSION)\b",
    re.IGNORECASE,
)

FORBIDDEN_SCHEMAS_AND_TABLES_RE = re.compile(
    r"\b(raw\s*\.|staging\s*\.|pg_catalog|information_schema|pg_shadow|pg_authid|pg_roles|curated\s*\.\s*users|\busers\b)",
    re.IGNORECASE,
)

ALLOWED_CURATED_TABLES = {
    "curated.events",
    "curated.agg_hourly",
    "curated.agg_region",
    "curated.quality_results",
    "curated.pipeline_runs",
    "curated.anomalies",
    "curated.daily_reports",
    "curated.faq",
}

TABLE_REF_RE = re.compile(r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_.]+)", re.IGNORECASE)
LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)


def validate_safe_sql(sql: str) -> str:
    """
    Valida estáticamente una consulta SQL y lanza `UnsafeQueryError` si viola cualquier política.
    Devuelve la consulta normalizada con `LIMIT` seguro garantizado.
    """
    if not sql or not isinstance(sql, str) or not sql.strip():
        raise UnsafeQueryError("La consulta SQL está vacía.")

    cleaned = sql.strip()

    # 1. Rechazar comentarios sospechosos (-- o /* */)
    if "--" in cleaned or "/*" in cleaned or "*/" in cleaned:
        raise UnsafeQueryError("Consulta rechazada: no se permiten comentarios SQL ('--' ni '/* */').")

    # 2. Rechazar múltiples sentencias (permitimos un único ';' al final si existe, que removemos)
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if ";" in cleaned:
        raise UnsafeQueryError("Consulta rechazada: no se permiten múltiples sentencias SQL separadas por ';'.")

    # 3. Verificar que comience exclusivamente con SELECT
    if not re.match(r"^SELECT\b", cleaned, flags=re.IGNORECASE):
        raise UnsafeQueryError("Consulta rechazada: únicamente se permiten sentencias SELECT de solo lectura.")

    # 4. Rechazar palabras clave DDL/DML o funciones peligrosas
    bad_kw = FORBIDDEN_KEYWORDS_RE.search(cleaned)
    if bad_kw:
        raise UnsafeQueryError(f"Consulta rechazada: palabra clave prohibida detectada ('{bad_kw.group(1)}').")

    # 5. Rechazar acceso a raw, staging, catálogos del sistema o tabla de usuarios
    bad_obj = FORBIDDEN_SCHEMAS_AND_TABLES_RE.search(cleaned)
    if bad_obj:
        raise UnsafeQueryError(
            f"Consulta rechazada: acceso no autorizado al objeto o schema '{bad_obj.group(1)}'."
        )

    # 6. Verificar que todas las tablas referenciadas en FROM / JOIN pertenezcan a la lista blanca de curated
    referenced_tables = [m.lower() for m in TABLE_REF_RE.findall(cleaned)]
    if not referenced_tables:
        raise UnsafeQueryError("Consulta rechazada: debe consultar al menos una tabla válida del schema 'curated'.")

    for tbl in referenced_tables:
        if tbl not in ALLOWED_CURATED_TABLES:
            raise UnsafeQueryError(
                f"Consulta rechazada: la tabla '{tbl}' no está permitida. "
                f"Use únicamente tablas públicas con prefijo 'curated.' ({sorted(ALLOWED_CURATED_TABLES)})."
            )

    # 7. LIMIT obligatorio (si no tiene LIMIT es rechazada según política o acotada si excede 100)
    limit_match = LIMIT_RE.search(cleaned)
    if not limit_match:
        raise UnsafeQueryError("Consulta rechazada: toda consulta debe incluir una cláusula LIMIT explícita.")

    limit_val = int(limit_match.group(1))
    if limit_val <= 0 or limit_val > 100:
        raise UnsafeQueryError("Consulta rechazada: el valor de LIMIT debe estar entre 1 y 100.")

    return cleaned


def execute_safe_sql(sql: str) -> Dict[str, Any]:
    """
    Valida y ejecuta la consulta SQL sobre PostgreSQL usando exclusivamente `reader_role`
    y un timeout de sentencia de 3 segundos.
    """
    validated_sql = validate_safe_sql(sql)
    conn = get_db_conn(role="reader", timeout=5)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '3000ms';")
                cur.execute(validated_sql)
                col_names = [desc[0] for desc in (cur.description or [])]
                rows: List[Dict[str, Any]] = []
                for r in cur.fetchall():
                    row_dict = {}
                    for idx, col in enumerate(col_names):
                        val = r[idx]
                        if hasattr(val, "isoformat"):
                            row_dict[col] = val.isoformat()
                        else:
                            row_dict[col] = val
                    rows.append(row_dict)
                return {
                    "sql_ejecutado": validated_sql,
                    "columnas": col_names,
                    "filas": rows,
                    "total_filas": len(rows),
                }
    finally:
        conn.close()

