# Documento de Arquitectura — SismoWatch (SuperApp)

## 1. Propósito del Producto de Datos

### ¿Qué problema resuelve?
El catálogo global de sismos del Servicio Geológico de los Estados Unidos (**USGS**) publica eventos geofísicos en formato GeoJSON crudo sin garantías de deduplicación entre ventanas solapadas, con actualizaciones retroactivas (`properties.updated`), posibles valores atípicos instrumentales y textos libres susceptibles de inyección cuando se integran con modelos de lenguaje (LLMs). Consumir directamente la API pública desde aplicaciones cliente genera acoplamiento, inconsistencia en métricas agregadas, falta de trazabilidad de calidad y riesgo de indisponibilidad cuando el servicio externo presenta caídas transitorias.

### ¿Para quién está construido?
**SismoWatch** es el producto corporativo de datos de **SuperApp** diseñado para dos perfiles operativos gobernados mediante Control de Acceso Basado en Roles (**RBAC**):
1. **Analistas y Operadores de Monitoreo (`viewer`)**: Requieren visibilidad continua sobre KPIs sísmicos de las últimas 24 horas, distribución espacial en mapa interactivo, evolución horaria, reportes ejecutivos diarios redactados con IA verificada y un agente conversacional capaz de responder preguntas de negocio y geofísica sin alucinaciones.
2. **Ingenieros de Datos y Administradores de Plataforma (`admin`)**: Necesitan observabilidad profunda sobre las ejecuciones de los DAGs, auditoría de rechazos por regla de calidad, telemetría de costos/latencia de IA, capacidad de lanzar reprocesamientos históricos (*backfill*) y regeneración de reportes bajo demanda.

### ¿Qué decisiones habilita?
- Identificación inmediata de eventos sísmicos significativos (`mag >= 5.0`) y boletines de vigilancia de **tsunami**.
- Detección automatizada de picos anómalos de actividad horaria (**z-score**) y enjambres sísmicos espaciales (**DBSCAN**).
- Continuidad operativa garantizada: si USGS cae temporalmente, el sistema dispara alertas por Webhook y continúa sirviendo el último lote validado junto con su indicador exacto de frescura en minutos.

---

## 2. Declaración de Latencia y Naturaleza del "Tiempo Real"

Por decisión cerrada de arquitectura, SismoWatch implementa un patrón **Medallion Batch / Micro-Batch + Capa de Servicio**:
- **No existe streaming de ingesta** (ni Apache Kafka, ni arquitectura Lambda).
- **Latencia máxima del dato**: Está determinada por la cadencia de ingesta del DAG `usgs_ingest_bronze`, fijada en **5 minutos** (`*/5 * * * *`).
- **Tiempo real de consumo**: Una vez que el DAG `usgs_transform_medallion` publica una nueva versión (`data_version`) en PostgreSQL `curated`, el backend FastAPI empuja instantáneamente las novedades hacia los navegadores conectados mediante **Server-Sent Events (SSE)** en `/api/stream`, actualizando KPIs y marcadores sin recargar la página.

---

## 3. Diagrama de Componentes y Capas

```mermaid
flowchart LR
    subgraph Fuente["Fuente Pública"]
        USGS["USGS Earthquake API\n(all_hour.geojson & fdsnws/query)"]
    end

    subgraph Orquestacion["Orquestación & Procesamiento (Airflow + Spark local[2])"]
        DAG1["DAG 1: usgs_ingest_bronze\n(Cada 5 min)"]
        DAG4["DAG 4: usgs_backfill\n(Bajo demanda / Init 30d)"]
        DAG2["DAG 2: usgs_transform_medallion\n(Disparado por Asset Bronce)"]
        GATE["Compuerta de Calidad\n(Reglas por fila + Pandera)"]
        DAG3["DAG 3: usgs_ai_daily\n(Cada 24h / Admin)"]
    end

    subgraph DataLake["Data Lake MinIO (S3A)"]
        B[("Bucket: bronze\nJSON Crudo Inmutable")]
        S[("Bucket: silver\nParquet Tipado + Dedupe")]
        G[("Bucket: gold\nParquet: events, agg_hourly, agg_region")]
    end

    subgraph Warehouse["PostgreSQL 16 + pgvector"]
        RAW[("Schema: raw\ningestions")]
        STG[("Schema: staging\nevents_silver")]
        CUR[("Schema: curated\nevents, agg_*, quality_results,\npipeline_runs, anomalies,\ndaily_reports, ai_*, faq, kb_chunks, users")]
    end

    subgraph IA["Capa de Inteligencia Artificial"]
        GROQ["Groq Cloud API\n(GROQ_MODEL Primario)"]
        OLLAMA["Ollama Local\nqwen2.5:3b (Respaldo)\nbge-m3 (Embeddings 1024d)"]
        TPL["Plantilla Determinista\n(Fallback Python)"]
    end

    subgraph Servicio["Capa de Servicio y Consumo"]
        API["FastAPI Backend (:8000)\nJWT + RBAC + SSE + Agente 4 Niveles"]
        WEB["React + TS + Vite + Nginx (:3000)\nLeaflet + Recharts + Panel Admin"]
    end

    USGS -->|HTTPS + Backoff| DAG1
    USGS -->|Tramos 5d HTTPS| DAG4
    DAG1 -->|Temp + Rename| B
    DAG4 -->|Temp + Rename| B
    DAG1 -->|ingest_role| RAW
    DAG4 -->|ingest_role| RAW
    B ==>|Dataset s3://bronze/usgs| DAG2
    DAG2 -->|PySpark S3A| S
    S --> GATE
    GATE -->|Aprobado| G
    GATE -->|transform_role| CUR
    G -->|Upsert ACID Python| STG
    G -->|Upsert ACID Python| CUR
    CUR <-->|ai_role| DAG3
    DAG3 --> GROQ
    GROQ -.->|Fallback| OLLAMA
    OLLAMA -.->|Fallback| TPL
    CUR -->|reader_role (SELECT)| API
    API -->|api_rw_role (Cache/Logs)| CUR
    API <-->|REST + SSE /api/stream| WEB
```

---

## 4. Justificación del Stack Tecnológico

| Capa | Tecnología y Versión | Justificación Técnica |
| :--- | :--- | :--- |
| **Lenguaje** | Python 3.11 | Tipado moderno, alto rendimiento y soporte oficial en Airflow 2.10.4, PySpark 3.5.3 y FastAPI. |
| **Orquestación** | Apache Airflow `2.10.4` (`LocalExecutor`) | Permite encadenar DAGs orientados a datos con **Assets/Datasets**, manejo declarativo de reintentos con *exponential backoff*, `TaskGroups` y `on_failure_callback` sin sobrecarga de Redis/Celery. |
| **Transformación** | Apache Spark / PySpark `3.5.3` (`local[2]`) + `hadoop-aws:3.3.4` | Procesamiento distribuido unificado con escritura particionada dinámica sobre S3A (`partitionOverwriteMode=dynamic`). |
| **Data Lake** | MinIO (`RELEASE.2025-10-15T17-29-55Z`) | Almacenamiento de objetos compatible con S3 local que separa las capas Bronce, Plata y Oro como fuente de verdad inmutable. |
| **Base de Datos** | PostgreSQL 16 + `pgvector` + `pgcrypto` | Soporta transacciones ACID para el *upsert* de la capa Oro, aislamiento por schemas/roles y búsqueda vectorial HNSW de 1024 dimensiones en un único motor. |
| **Calidad** | Reglas Spark/Python + `pandera==0.20.4` | Validación dual: descarte por fila con rastro liviano y compuerta bloqueante de esquema y umbrales por lote. |
| **IA y RAG** | Groq API + Ollama (`qwen2.5:3b` y `bge-m3`) | Alta velocidad de inferencia en la nube con Groq y autonomía local sin GPU con modelos ligeros en Ollama (`OLLAMA_KEEP_ALIVE=2m`). |
| **Backend** | FastAPI `0.115.4` + Pydantic v2 + JWT + `bcrypt` | Asíncrono nativo para SSE (`/api/stream`), validación estricta de contratos y documentación OpenAPI automática en `/docs`. |
| **Frontend** | React 18 + TypeScript + Vite + Tailwind + Leaflet + Recharts + Nginx | Interfaz reactiva temática de sismología con mapa geoespacial interactivo, gráficas analíticas y huella de RAM mínima (< 50 MB en Nginx). |

---

## 5. Modelo de Datos en PostgreSQL y Roles de Mínimo Privilegio

### 5.1 Tablas, Llaves e Índices

| Schema | Tabla | Llave Primaria (PK) | Índices Clave y Relaciones | Propósito |
| :--- | :--- | :--- | :--- | :--- |
| `raw` | `ingestions` | `ingestion_id` | `idx_raw_ingestions_ts (timestamp DESC)` | Registro de archivos JSON crudos escritos en MinIO Bronce, conteo de eventos y `run_id`. |
| `staging` | `events_silver` | `id` | PK `id` | Espejo tipado de los eventos validados en la capa Plata. |
| `curated` | `events` | `id` | `idx_curated_events_time`, `idx_curated_events_mag`, `idx_curated_events_region` | Eventos sísmicos deduplicados y sanitizados listos para consumo de la plataforma. |
| `curated` | `agg_hourly` | `hora` | `idx_curated_agg_hourly_hora (hora DESC)` | Serie horaria pre-calculada en Oro (`conteo`, `mag_max`, `mag_promedio`, `tsunami`). |
| `curated` | `agg_region` | `(region, fecha)` | `idx_curated_agg_region_fecha`, `idx_curated_agg_region_region` | Actividad diaria por región geográfica derivada. |
| `curated` | `quality_results` | `run_id` | `idx_curated_quality_ts` | Puntaje del lote, conteo por regla, frescura, volumen y JSONB `rechazos_log` (`{id, regla, motivo}`). |
| `curated` | `pipeline_runs` | `id` (`UNIQUE (dag_id, run_id)`) | `idx_curated_pipeline_runs_dag_fin` | Historial de ejecuciones alimentado por callbacks y marca `data_version` para caché y SSE. |
| `curated` | `anomalies` | `id` (`UNIQUE (tipo, ventana)`) | `idx_curated_anomalies_detected` | Anomalías detectadas por **z-score** horario y clustering espacial **DBSCAN**. |
| `curated` | `daily_reports` | `fecha` | `idx_curated_daily_reports_fecha` | Reportes ejecutivos diarios con JSON validado, narrativa, proveedor, `fallback` y `version_prompt`. |
| `curated` | `ai_runs` | `id` | `idx_curated_ai_runs_created` | Telemetría de cada invocación a IA (tokens, latencia ms, costo estimado, proveedor, `fallback`). |
| `curated` | `ai_cache` | `clave_hash` | `idx_curated_ai_cache_data_version`, HNSW en `embedding vector(1024)` | Caché exacta y semántica del agente conversacional invalidada por `data_version` y `ttl`. |
| `curated` | `faq` | `id` (`UNIQUE (categoria, pregunta_canonica)`) | Secuencia ordenada por `orden` | 13 categorías documentadas con variantes de patrones para resolución en Nivel 0 sin LLM. |
| `curated` | `kb_chunks` | `id` (Hash SHA-256 determinístico) | `idx_curated_kb_chunks_fuente`, HNSW en `embedding vector(1024)` | Base vectorial RAG con chunking natural exacto (`event`, `daily_report`, `anomaly`, `faq`). |
| `curated` | `users` | `username` | PK `username` | Usuarios autenticados con hash `bcrypt` (factor 12) y rol (`viewer` o `admin`). |
| `curated` | `chat_sessions` | `id` | PK `id` | Auditoría de preguntas del chat, nivel de cascada alcanzado y latencia. |

### 5.2 Matriz de Roles de Base de Datos (Mínimo Privilegio)

- **`ingest_role`**: Escribe únicamente en `raw.ingestions` (más registro de estado en `curated.pipeline_runs` durante callbacks). Sin acceso a `staging`.
- **`transform_role`**: Lee `raw.ingestions`; escribe en `staging.events_silver` y publica el oro y métricas de calidad en `curated` (`events`, `agg_hourly`, `agg_region`, `quality_results`, `pipeline_runs`).
- **`ai_role`**: Lee `curated`; escribe exclusivamente en `curated.anomalies`, `curated.daily_reports`, `curated.ai_runs`, `curated.kb_chunks` y `curated.pipeline_runs`.
- **`reader_role`**: **Solo `SELECT` sobre `curated`**. Tiene revocado explícitamente todo privilegio sobre `raw` y `staging`. Es el rol utilizado por la API para todas las consultas y para la skill `consulta_sql_segura`.
- **`api_rw_role`**: Lectura en `curated` y escritura mínima restringida únicamente a `curated.ai_cache`, `curated.chat_sessions` y `curated.ai_runs`.

---

## 6. Trade-offs y Decisiones de Ingeniería (Defensa de Sustentación)

### 6.1 ¿Por qué Apache Airflow y no `cron`?
Un `cron` ejecuta scripts aislados a ciegas: no gestiona dependencias orientadas a datos (*Data-Aware Scheduling* con `Assets/Datasets`), no ofrece reintentos con *exponential backoff* por tarea, carece de historial de corridas auditable, no previene solapamientos (`max_active_runs=1`) y no provee una API REST estándar para disparar *backfills* parametrizados desde la aplicación web.

### 6.2 ¿Por qué Arquitectura Medallion (Bronce → Plata → Oro)?
Separa responsabilidades técnicas y garantiza reprocesabilidad:
- **Bronce** conserva el payload GeoJSON exacto e inmutable entregado por USGS. Si cambia una regla de negocio o se descubre un bug, podemos reconstruir Plata y Oro desde Bronce sin volver a consultar USGS.
- **Plata** aplica tipado fuerte, conversión de milisegundos epoch a UTC, sanitización de texto, deduplicación por `id` conservando el mayor `updated` y filtrado de filas inválidas.
- **Oro** entrega conjuntos listos para consumo analítico y de baja latencia (`events`, `agg_hourly`, `agg_region`).

### 6.3 ¿Por qué Apache Spark en modo `local[2]` y cómo escala a un clúster?
En un servidor objetivo de **16 GB de RAM y CPU Intel i5 sin GPU**, levantar un clúster Spark Standalone (Master + múltiples Workers JVM) consumiría entre 4 y 6 GB adicionales de memoria base ociosa. Al ejecutar PySpark en modo `local[2]` dentro del contenedor de Airflow con memoria acotada (`768m` driver/executor), mantenemos todo el presupuesto del sistema en **~9.2 GB**.
> **Escalabilidad sin cambios de código**: En [`session.py`](file:///home/cohorte5/IAriwi/airflow/include/spark_jobs/session.py), el maestro se lee de la variable `SPARK_MASTER` (por defecto `local[2]`). Para escalar a un clúster Spark Standalone, YARN o Kubernetes en producción, basta con cambiar `SPARK_MASTER=spark://spark-master:7077` sin modificar una sola línea de transformación en `bronze_to_silver.py` ni `silver_to_gold.py`.

### 6.4 ¿Por qué Server-Sent Events (SSE) y no WebSockets?
El flujo de eventos sísmicos desde el servidor hacia el navegador es estrictamente **unidireccional**. SSE opera sobre HTTP estándar, atraviesa proxies como Nginx sin configuraciones complejas de *upgrade* de protocolo, consume menos recursos en FastAPI y el estándar HTML5 `EventSource` incluye soporte nativo para reconexión automática y envío de la cabecera `Last-Event-ID`.

### 6.5 ¿Por qué sin tabla de cuarentena pesada?
Copiar filas inválidas completas en una tabla de cuarentena en PostgreSQL duplica almacenamiento innecesariamente y traslada payloads potencialmente corruptos o maliciosos dentro del motor relacional. En nuestra arquitectura, **el archivo original completo ya reside de forma inmutable en MinIO Bronce** (`s3://bronze/usgs/...`), mientras que en `curated.quality_results` guardamos un registro de auditoría liviano por rechazo (`{id, regla, motivo}`) que permite diagnosticar qué regla falló y localizar el evento en Bronce cuando se requiera.

### 6.6 ¿Por qué PostgreSQL sirve al dashboard y MinIO es la fuente de verdad?
MinIO (almacenamiento de objetos S3 en Parquet) es óptimo para escaneos analíticos por lotes en Spark, durabilidad e inmutabilidad a bajo costo, pero tiene alta latencia por petición para consultas puntuales concurrentes, paginación rápida, índices B-Tree compuestos y búsqueda vectorial HNSW de baja latencia. Por ello, MinIO actúa como **fuente de verdad del Data Lake** y PostgreSQL `curated` actúa como **capa de servicio (*Serving Layer*)** indexada para la API y el frontend.

### 6.7 ¿Qué se haría distinto en Producción a Escala Empresarial?
1. **Formato de Tabla Abierto en el Lago (Apache Iceberg o Delta Lake)**: Sustituir Parquet plano con sobrescritura de partición por tablas Iceberg/Delta sobre S3 para contar con *Time Travel*, *schema evolution* y `MERGE INTO` ACID directamente en el lago.
2. **dbt (Data Build Tool)**: Incorporar dbt sobre la capa Plata→Oro para gobernar linaje de datos, documentación de columnas y pruebas declarativas de esquema.
3. **Clúster de Spark Desacoplado**: Ejecutar los jobs PySpark mediante `SparkSubmitOperator` o `SparkKubernetesOperator` sobre un clúster dedicado con autoescalado dinámico.
4. **Gestión de Secretos**: Reemplazar variables de entorno por **HashiCorp Vault** o **AWS Secrets Manager** configurado como *Secrets Backend* de Airflow y FastAPI.
5. **Observabilidad Unificada**: Exportar métricas con **OpenTelemetry / StatsD** hacia Prometheus + Grafana y centralizar los logs JSON estructurados en Loki o Elasticsearch.
6. **Streaming Real (Kafka / Flink)**: Únicamente si la fuente de datos cambiara de un feed HTTP publicado cada minuto/hora a telemetría directa de estaciones sismográficas en tiempo real (sub-segundo).
