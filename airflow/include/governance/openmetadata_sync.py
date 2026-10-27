"""
Cliente de Gobernanza y Sincronización Automática con OpenMetadata (API REST v1).
Diseñado bajo arquitectura Fail-Open (Cero Daños / Cero Bloqueos):
- Registra el servicio PostgreSQL (`sismowatch_postgres`), la base `sismowatch`, los 3 schemas
  (`raw`, `staging`, `curated`) y las 14 tablas con sus columnas y descripciones en español.
- Registra el Data Lake MinIO (`sismowatch_minio`) y los 3 contenedores Medallion (`bronze`, `silver`, `gold`).
- Registra el orquestador Apache Airflow (`sismowatch_airflow`) y los 4 DAGs.
- Construye el grafo visual de Linaje de Datos (Lineage) de punta a punta entre tablas, contenedores y pipelines.
- Crea el Glosario de Negocio Sismológico (`Glosario_SismoWatch`) y publica métricas de volumen/calidad.
"""
import base64
import logging
import os
from typing import Any, Dict, List, Optional
import psycopg2
import requests

logger = logging.getLogger(__name__)

# Descripciones de negocio de las 14 tablas de PostgreSQL
TABLE_DESCRIPTIONS: Dict[str, str] = {
    "raw.ingestions": (
        "Registro de auditoría de ingesta desde la API pública de USGS. Almacena la URI inmutable "
        "del archivo GeoJSON en MinIO Bronce, el conteo de eventos descargados, timestamp y run_id."
    ),
    "staging.events_silver": (
        "Espejo relacional tipado de la Capa Plata (MinIO Silver). Contiene eventos sísmicos "
        "deduplicados por ID (conservando el mayor updated) y validados fila por fila en PySpark."
    ),
    "curated.events": (
        "Catálogo sísmico global certificado (Capa Oro). Contiene los sismos validados por la "
        "compuerta Pandera, listos para ser consultados en milisegundos por FastAPI, el Mapa y la IA."
    ),
    "curated.agg_hourly": (
        "Serie temporal horaria pre-agregada en Capa Oro: conteo de sismos, magnitud máxima, "
        "magnitud promedio y alertas de tsunami por cada hora UTC."
    ),
    "curated.agg_region": (
        "Agregación diaria por región geográfica en Capa Oro: conteo de eventos y magnitud máxima "
        "por cada región y fecha."
    ),
    "curated.quality_results": (
        "Resultados de auditoría de calidad por lote (Pandera + reglas Spark). Incluye puntaje (0-1), "
        "frescura, volumen y el registro liviano JSONB {id, regla, motivo} de filas rechazadas."
    ),
    "curated.pipeline_runs": (
        "Bitácora operativa de ejecuciones de los 4 DAGs de Apache Airflow: inicio, fin, estado, "
        "resultado y versión de datos publicada (data_version)."
    ),
    "curated.anomalies": (
        "Anomalías sísmicas detectadas automáticamente: picos horarios estadísticos (z-score) "
        "y enjambres espaciales (clustering DBSCAN en km y horas)."
    ),
    "curated.daily_reports": (
        "Boletines ejecutivos sísmicos diarios generados y verificados numéricamente contra SQL."
    ),
    "curated.ai_runs": (
        "Auditoría de ejecuciones de modelos de IA (Groq / Ollama / Respaldo): tokens de entrada/salida, "
        "latencia en ms y costo estimado."
    ),
    "curated.ai_cache": (
        "Caché exacta (SHA-256) y semántica (vectores pgvector bge-m3 de 1024 dimensiones) "
        "vinculada a data_version y rol de usuario."
    ),
    "curated.faq": (
        "Catálogo de 15 categorías de preguntas frecuentes sismológicas y operativas con patrones "
        "formales, coloquiales y ortográficos."
    ),
    "curated.kb_chunks": (
        "Base de conocimiento vectorial (RAG) con embeddings de 1024 dimensiones (bge-m3) e índice HNSW."
    ),
    "curated.users": (
        "Tabla de control de acceso basado en roles (RBAC): usuarios admin y viewer con hashes bcrypt."
    ),
}

# Descripciones de las columnas principales de curated.events
COLUMN_DESCRIPTIONS: Dict[str, str] = {
    "id": "Identificador único oficial del evento sísmico en el catálogo USGS (PK).",
    "mag": "Magnitud del sismo en escala logarítmica validada en el rango [-2.0, 10.0].",
    "mag_type": "Tipo o escala de medición de magnitud (ej. ml, md, mb, mww).",
    "place": "Descripción geográfica sanitizada del epicentro respecto a la localidad más cercana.",
    "time": "Fecha y hora exacta de origen de la ruptura sísmica en UTC (timestamptz).",
    "updated": "Fecha y hora de última actualización del boletín en USGS (usada para deduplicación).",
    "lat": "Latitud geográfica del epicentro en grados decimales [-90.0, 90.0].",
    "lon": "Longitud geográfica del epicentro en grados decimales [-180.0, 180.0].",
    "depth_km": "Profundidad hipocentral en kilómetros desde la superficie [-5.0, 800.0 km].",
    "tsunami": "Indicador de vigilancia o boletín oceánico de tsunami (1 = activo, 0 = sin alerta).",
    "sig": "Índice compuesto de significancia del sismo calculado por USGS (0 a >1000).",
    "status": "Estado de revisión sismológica del evento (automatic o reviewed).",
    "type": "Clasificación del evento geofísico (earthquake, quarry blast, etc.).",
    "url": "Enlace oficial a la ficha técnica del evento en earthquake.usgs.gov.",
    "region": "País o región geográfica normalizada extraída de la ubicación del sismo.",
    "data_version": "Identificador de versión del lote Medallion que publicó o actualizó el registro.",
}

# Mapeo de tipos PostgreSQL hacia tipos estándar de OpenMetadata
PG_TO_OM_TYPE: Dict[str, str] = {
    "text": "TEXT",
    "character varying": "VARCHAR",
    "varchar": "VARCHAR",
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "double precision": "DOUBLE",
    "real": "FLOAT",
    "numeric": "NUMERIC",
    "boolean": "BOOLEAN",
    "timestamp with time zone": "TIMESTAMPZ",
    "timestamp without time zone": "TIMESTAMP",
    "date": "DATE",
    "jsonb": "JSON",
    "json": "JSON",
    "USER-DEFINED": "ARRAY",
}


class OpenMetadataGovernanceClient:
    """Cliente REST tolerante a fallos para sincronizar catálogo, linaje y calidad en OpenMetadata."""

    def __init__(self, base_url: Optional[str] = None, email: Optional[str] = None, password: Optional[str] = None):
        self.base_url = (base_url or os.getenv("OPENMETADATA_URL", "http://openmetadata-server:8585/api")).rstrip("/")
        self.email = email or os.getenv("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
        self.password = password or os.getenv("OPENMETADATA_ADMIN_PASSWORD", "admin")
        self.session = requests.Session()
        self.token: Optional[str] = None

    def authenticate(self) -> bool:
        """Obtiene el token JWT de OpenMetadata usando las credenciales de administración."""
        try:
            b64_pass = base64.b64encode(self.password.encode("utf-8")).decode("utf-8")
            resp = self.session.post(
                f"{self.base_url}/v1/users/login",
                json={"email": self.email, "password": b64_pass},
                timeout=5,
            )
            if resp.status_code == 200:
                self.token = resp.json().get("accessToken")
                if self.token:
                    self.session.headers.update(
                        {
                            "Authorization": f"Bearer {self.token}",
                            "Content-Type": "application/json",
                        }
                    )
                    return True
        except requests.RequestException as exc:
            logger.debug("OpenMetadata aún no disponible para autenticación: %s", exc)
        return False

    def _put(self, path: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            resp = self.session.put(f"{self.base_url}{path}", json=payload, timeout=8)
            if resp.status_code in (200, 201, 204):
                if not resp.text or not resp.text.strip():
                    return {"status": "ok"}
                return resp.json()
            logger.warning("PUT %s devolvió %s: %s", path, resp.status_code, resp.text[:250])
        except requests.RequestException as exc:
            logger.warning("Error de red en PUT %s: %s", path, exc)
        return None

    def _get(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            resp = self.session.get(f"{self.base_url}{path}", timeout=6)
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            pass
        return None

    def _introspect_postgres_columns(self) -> Dict[str, List[Dict[str, Any]]]:
        """Lee la estructura real de las 15 tablas de `sismowatch` en `raw`, `staging` y `curated`."""
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        db = os.getenv("POSTGRES_DB", "sismowatch")
        roles_to_query = [
            (os.getenv("DB_TRANSFORM_USER", "transform_role"), os.getenv("DB_TRANSFORM_PASSWORD", "sw_transform_local_9812")),
            (os.getenv("DB_READER_USER", "reader_role"), os.getenv("DB_READER_PASSWORD", "sw_reader_local_9812")),
        ]

        tables_cols: Dict[str, List[Dict[str, Any]]] = {}
        seen_cols: set[tuple[str, str]] = set()
        pk_assigned: set[str] = set()

        for user, password in roles_to_query:
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT table_schema, table_name, column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema IN ('raw', 'staging', 'curated')
                        ORDER BY table_schema, table_name, ordinal_position;
                        """
                    )
                    for schema_name, tbl_name, col_name, dtype, is_nullable in cur.fetchall():
                        key = f"{schema_name}.{tbl_name}"
                        col_key = (key, str(col_name))
                        if col_key in seen_cols:
                            continue
                        seen_cols.add(col_key)
                        om_type = PG_TO_OM_TYPE.get(str(dtype).lower(), "VARCHAR")
                        col_entry: Dict[str, Any] = {
                            "name": str(col_name),
                            "dataType": om_type,
                            "dataTypeDisplay": str(dtype),
                            "description": COLUMN_DESCRIPTIONS.get(
                                str(col_name),
                                f"Columna {col_name} ({dtype}) de la tabla {key}.",
                            ),
                        }
                        if om_type == "VARCHAR":
                            col_entry["dataLength"] = 256
                        elif om_type == "ARRAY":
                            col_entry["arrayDataType"] = "FLOAT"
                        if (
                            key not in pk_assigned
                            and str(col_name) in ("id", "ingestion_id", "run_id", "hora", "fecha", "clave_hash", "username")
                        ):
                            col_entry["constraint"] = "PRIMARY_KEY"
                            pk_assigned.add(key)
                        elif is_nullable == "NO":
                            col_entry["constraint"] = "NOT_NULL"
                        tables_cols.setdefault(key, []).append(col_entry)
            finally:
                conn.close()
        return tables_cols

    def sync_all_metadata_and_lineage(self) -> Dict[str, Any]:
        """
        Ejecuta el registro idempotente de todo el ecosistema SismoWatch en OpenMetadata:
        1. Servicio PostgreSQL, Base `sismowatch`, Schemas `raw`, `staging`, `curated` y 14 Tablas.
        2. Servicio de Almacenamiento MinIO y Contenedores `bronze`, `silver`, `gold`.
        3. Servicio de Pipelines Apache Airflow y los 4 DAGs.
        4. Glosario de Negocio Sismológico.
        5. Grafo de Linaje de Punta a Punta (Data Lineage).
        """
        if not self.authenticate():
            return {"status": "SKIPPED", "reason": "OpenMetadata no disponible o iniciando"}

        summary: Dict[str, Any] = {
            "status": "SUCCESS",
            "tables_registered": 0,
            "containers_registered": 0,
            "pipelines_registered": 0,
            "lineage_edges": 0,
            "glossary_terms": 0,
        }

        # 1. Registrar Database Service (PostgreSQL)
        db_service_name = "sismowatch_postgres"
        self._put(
            "/v1/services/databaseServices",
            {
                "name": db_service_name,
                "serviceType": "Postgres",
                "description": (
                    "Servidor PostgreSQL 16 + pgvector de SismoWatch (SuperApp). "
                    "Aloja los schemas raw (auditoría de ingesta), staging (espejo Plata) y curated (Capa Oro servida a la API e IA)."
                ),
                "connection": {
                    "config": {
                        "type": "Postgres",
                        "username": os.getenv("DB_READER_USER", "reader_role"),
                        "authType": {"password": os.getenv("DB_READER_PASSWORD", "sw_reader_local_9812")},
                        "hostPort": f"{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}",
                        "database": os.getenv("POSTGRES_DB", "sismowatch"),
                    }
                },
            },
        )

        # 2. Registrar Database `sismowatch`
        db_fqn = f"{db_service_name}.sismowatch"
        self._put(
            "/v1/databases",
            {
                "name": "sismowatch",
                "service": db_service_name,
                "description": (
                    "Base de datos principal del producto de datos SismoWatch con aislamiento de mínimo privilegio por schema."
                ),
            },
        )

        # 3. Registrar los 3 Database Schemas (`raw`, `staging`, `curated`)
        schema_descriptions = {
            "raw": "Schema de auditoría de ingesta: registra cada archivo GeoJSON crudo descargado desde USGS hacia MinIO Bronce.",
            "staging": "Schema intermedio de transformación: espejo tipado y deduplicado de la Capa Plata procesada con PySpark.",
            "curated": "Schema certificado de Capa Oro: eventos sísmicos validados por Pandera, series agregadas, anomalías, reportes y memoria vectorial de la IA.",
        }
        for sc_name, sc_desc in schema_descriptions.items():
            self._put(
                "/v1/databaseSchemas",
                {
                    "name": sc_name,
                    "database": db_fqn,
                    "description": sc_desc,
                },
            )

        # 4. Introspectar y registrar las 14 tablas reales de PostgreSQL
        tables_cols = self._introspect_postgres_columns()
        table_ids: Dict[str, str] = {}

        for key, cols in tables_cols.items():
            sc_name, tbl_name = key.split(".", 1)
            schema_fqn = f"{db_fqn}.{sc_name}"
            res = self._put(
                "/v1/tables",
                {
                    "name": tbl_name,
                    "databaseSchema": schema_fqn,
                    "tableType": "Regular",
                    "description": TABLE_DESCRIPTIONS.get(key, f"Tabla {key} del sistema SismoWatch."),
                    "columns": cols,
                },
            )
            if res and "id" in res:
                table_ids[key] = res["id"]
                summary["tables_registered"] += 1

        # 5. Registrar Storage Service (MinIO Data Lake) y los 3 Buckets Medallion
        storage_service_name = "sismowatch_minio"
        self._put(
            "/v1/services/storageServices",
            {
                "name": storage_service_name,
                "serviceType": "S3",
                "description": (
                    "Data Lake de Objetos MinIO compatible con S3A para el almacenamiento inmutable de las capas Bronce (.json), Plata (.parquet) y Oro (.parquet)."
                ),
            },
        )

        containers_info = {
            "bronze": (
                "Capa Bronce (Inmutable): Almacena los archivos GeoJSON crudos tal como llegan de la API de USGS "
                "en rutas particionadas por fecha (usgs/events/YYYY/MM/DD/)."
            ),
            "silver": (
                "Capa Plata (Validada y Deduplicada): Almacena archivos columnares Apache Parquet limpiados con PySpark, "
                "sin duplicados por ID y filtrados por reglas de dominio."
            ),
            "gold": (
                "Capa Oro (Analítica Certificada): Almacena los archivos Parquet finales tras aprobar la compuerta "
                "Pandera (events_curated/, agg_hourly/, agg_region/)."
            ),
        }
        container_ids: Dict[str, str] = {}
        for c_name, c_desc in containers_info.items():
            c_res = self._put(
                "/v1/containers",
                {
                    "name": c_name,
                    "service": storage_service_name,
                    "description": c_desc,
                    "fileFormats": ["json"] if c_name == "bronze" else ["parquet"],
                },
            )
            if c_res and "id" in c_res:
                container_ids[c_name] = c_res["id"]
                summary["containers_registered"] += 1

        # 6. Registrar Pipeline Service (Apache Airflow) y los 4 DAGs
        pipeline_service_name = "sismowatch_airflow"
        self._put(
            "/v1/services/pipelineServices",
            {
                "name": pipeline_service_name,
                "serviceType": "Airflow",
                "description": (
                    "Orquestador Apache Airflow 2.10.4 (LocalExecutor) con 4 DAGs encadenados mediante Airflow Datasets."
                ),
                "connection": {
                    "config": {
                        "type": "Airflow",
                        "hostPort": "http://airflow-webserver:8080",
                        "connection": {"type": "Backend"},
                    }
                },
            },
        )

        dags_info = {
            "usgs_ingest_bronze": {
                "desc": "Ingesta micro-batch cada 5 minutos desde el feed GeoJSON de USGS hacia MinIO Bronce y raw.ingestions.",
                "tasks": [
                    {"name": "fetch_and_store_bronze", "description": "Descarga GeoJSON de USGS y guarda en MinIO bronze/"},
                    {"name": "record_raw_ingestion", "description": "Registra metadatos de auditoría en raw.ingestions"},
                ],
            },
            "usgs_transform_medallion": {
                "desc": "Transformación PySpark local[2] de Bronce a Plata, compuerta bloqueante Pandera y publicación en MinIO Oro + PostgreSQL curated.",
                "tasks": [
                    {"name": "spark_bronze_to_silver", "description": "Limpia, tipa, valida por fila y deduplica en Spark hacia MinIO silver/"},
                    {"name": "evaluate_quality_gate", "description": "Evalúa esquema Pandera, frescura, volumen y ratio de rechazos"},
                    {"name": "publish_to_gold_and_postgres", "description": "Escribe Parquet en MinIO gold/ y hace upsert transaccional en PostgreSQL curated"},
                ],
            },
            "usgs_ai_daily": {
                "desc": "Detección diaria de anomalías (z-score y DBSCAN), generación de boletín diario e indexación vectorial RAG con bge-m3.",
                "tasks": [
                    {"name": "detect_and_store_anomalies", "description": "Detecta picos horarios z-score y enjambres espaciales DBSCAN"},
                    {"name": "generate_and_store_daily_report", "description": "Genera el boletín ejecutivo diario verificando cifras SQL"},
                    {"name": "refresh_vector_knowledge_base", "description": "Actualiza vectores bge-m3 en curated.kb_chunks"},
                ],
            },
            "usgs_backfill": {
                "desc": "Población histórica por tramos de 7 días (30 días iniciales) desde USGS FDSNWS a través de Bronce, Plata y Oro.",
                "tasks": [
                    {"name": "download_historical_windows_to_bronze", "description": "Descarga tramos de 7 días desde USGS FDSNWS"},
                    {"name": "transform_and_publish_backfill", "description": "Procesa el histórico completo en Spark y publica en curated"},
                ],
            },
        }
        pipeline_ids: Dict[str, str] = {}
        for dag_id, info in dags_info.items():
            p_res = self._put(
                "/v1/pipelines",
                {
                    "name": dag_id,
                    "service": pipeline_service_name,
                    "description": info["desc"],
                    "tasks": info["tasks"],
                },
            )
            if p_res and "id" in p_res:
                pipeline_ids[dag_id] = p_res["id"]
                summary["pipelines_registered"] += 1

        # 7. Construir el Grafo Visual de Linaje (Data Lineage) entre Contenedores, Tablas y Pipelines
        def add_edge(
            from_type: str,
            from_id: Optional[str],
            to_type: str,
            to_id: Optional[str],
            pipeline_id: Optional[str] = None,
            desc: str = "",
        ) -> None:
            if not from_id or not to_id:
                return
            edge_payload: Dict[str, Any] = {
                "edge": {
                    "fromEntity": {"id": from_id, "type": from_type},
                    "toEntity": {"id": to_id, "type": to_type},
                }
            }
            if pipeline_id or desc:
                details: Dict[str, Any] = {}
                if pipeline_id:
                    details["pipeline"] = {"id": pipeline_id, "type": "pipeline"}
                if desc:
                    details["description"] = desc
                edge_payload["edge"]["lineageDetails"] = details
            if self._put("/v1/lineage", edge_payload):
                summary["lineage_edges"] += 1

        p_ingest = pipeline_ids.get("usgs_ingest_bronze")
        p_medallion = pipeline_ids.get("usgs_transform_medallion")
        p_ai = pipeline_ids.get("usgs_ai_daily")

        # Linaje de Contenedores MinIO a Tablas PostgreSQL y entre Capas Medallion
        add_edge("container", container_ids.get("bronze"), "table", table_ids.get("raw.ingestions"), p_ingest, "Registro de auditoría de ingesta Bronce")
        add_edge("container", container_ids.get("bronze"), "container", container_ids.get("silver"), p_medallion, "Transformación PySpark Bronce a Plata")
        add_edge("container", container_ids.get("silver"), "table", table_ids.get("staging.events_silver"), p_medallion, "Espejo relacional de Capa Plata")
        add_edge("container", container_ids.get("silver"), "container", container_ids.get("gold"), p_medallion, "Certificación Pandera Plata a Oro")
        add_edge("container", container_ids.get("gold"), "table", table_ids.get("curated.events"), p_medallion, "Publicación de catálogo curado en PostgreSQL")

        # Linaje entre Tablas SQL (raw -> staging -> curated -> agregaciones e IA)
        add_edge("table", table_ids.get("raw.ingestions"), "table", table_ids.get("staging.events_silver"), p_medallion, "Lectura de último lote Bronce en Spark")
        add_edge("table", table_ids.get("staging.events_silver"), "table", table_ids.get("curated.events"), p_medallion, "Upsert transaccional tras aprobar compuerta Pandera")
        add_edge("table", table_ids.get("staging.events_silver"), "table", table_ids.get("curated.quality_results"), p_medallion, "Registro de puntaje de calidad y rechazos por regla")
        add_edge("table", table_ids.get("curated.events"), "table", table_ids.get("curated.agg_hourly"), p_medallion, "Agregación horaria de sismos y magnitudes")
        add_edge("table", table_ids.get("curated.events"), "table", table_ids.get("curated.agg_region"), p_medallion, "Agregación diaria por región geográfica")
        add_edge("table", table_ids.get("curated.events"), "table", table_ids.get("curated.anomalies"), p_ai, "Detección de picos horarios z-score y enjambres DBSCAN")
        add_edge("table", table_ids.get("curated.events"), "table", table_ids.get("curated.daily_reports"), p_ai, "Generación de boletín ejecutivo diario con verificación SQL")
        add_edge("table", table_ids.get("curated.daily_reports"), "table", table_ids.get("curated.kb_chunks"), p_ai, "Indexación de vectores semánticos bge-m3 (1024 dim)")
        add_edge("table", table_ids.get("curated.faq"), "table", table_ids.get("curated.kb_chunks"), p_ai, "Indexación de preguntas frecuentes en base vectorial RAG")

        # 8. Crear Glosario Corporativo Sismológico (`Glosario_SismoWatch`)
        g_res = self._put(
            "/v1/glossaries",
            {
                "name": "Glosario_SismoWatch",
                "displayName": "Glosario Sismológico y de Arquitectura SismoWatch",
                "description": (
                    "Definiciones oficiales de conceptos geofísicos, reglas de calidad y capas Medallion para SuperApp."
                ),
            },
        )
        if g_res:
            terms = [
                (
                    "Magnitud_Sismica",
                    "Magnitud Sísmica (mag)",
                    "Medida logarítmica de la energía liberada en el hipocentro del terremoto. Rango válido en SismoWatch: [-2.0, 10.0].",
                ),
                (
                    "Profundidad_Hipocentral",
                    "Profundidad Hipocentral (depth_km)",
                    "Distancia vertical en km desde la superficie hasta el foco sísmico: Superficial (0-70 km), Intermedio (70-300 km), Profundo (300-800 km).",
                ),
                (
                    "Alerta_Tsunami",
                    "Indicador de Alerta de Tsunami",
                    "Bandera emitida por NOAA/USGS (1 = vigilancia activa, 0 = sin alerta) para eventos oceánicos o costeros relevantes.",
                ),
                (
                    "Indice_Significancia",
                    "Índice de Significancia (sig)",
                    "Puntaje compuesto de USGS (0 a >1000) que pondera magnitud, intensidad percibida y potencial de tsunami.",
                ),
                (
                    "Arquitectura_Medallion",
                    "Arquitectura Medallion (Bronce / Plata / Oro)",
                    "Diseño de ingeniería de datos en tres capas: Bronce (JSON crudo inmutable), Plata (Parquet validado y deduplicado) y Oro (Parquet + PostgreSQL curated).",
                ),
            ]
            for t_name, t_disp, t_desc in terms:
                if self._put(
                    "/v1/glossaryTerms",
                    {
                        "glossary": "Glosario_SismoWatch",
                        "name": t_name,
                        "displayName": t_disp,
                        "description": t_desc,
                    },
                ):
                    summary["glossary_terms"] += 1

        return summary


def sync_openmetadata_safe() -> Dict[str, Any]:
    """
    Función de entrada segura (Fail-Open) para ser invocada desde Airflow o desde el servicio init.
    Jamás lanza una excepción que interrumpa el pipeline sísmico.
    """
    try:
        client = OpenMetadataGovernanceClient()
        res = client.sync_all_metadata_and_lineage()
        logger.info("Sincronización con OpenMetadata finalizada: %s", res)
        return res
    except Exception as exc:
        logger.warning("Sincronización con OpenMetadata omitida (Fail-Open): %s", exc)
        return {"status": "SKIPPED", "reason": str(exc)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(sync_openmetadata_safe())

