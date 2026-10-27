-- =============================================================================
-- 04_seed.sql — Semilla Inicial de Usuarios (Admin y Viewer) y FAQ Extensa (15 Categorías)
-- Incluye vocabulario profesional, coloquial/jocoso y errores ortográficos frecuentes
-- =============================================================================

-- 1. Usuarios iniciales (los hashes y usernames se actualizan en 05_apply_env.sh desde .env)
INSERT INTO curated.users (username, password_hash, rol, activo)
VALUES
    ('admin', crypt('AdminSismo2026!', gen_salt('bf', 12)), 'admin', TRUE),
    ('viewer', crypt('ViewerSismo2026!', gen_salt('bf', 12)), 'viewer', TRUE)
ON CONFLICT (username) DO NOTHING;

-- 2. Semilla de FAQ Extensa y Documentada por Categorías (Sección 11)
INSERT INTO curated.faq (categoria, pregunta_canonica, patrones, respuesta_plantilla, orden)
VALUES
(
    'que_es_sismowatch',
    '¿Qué es SismoWatch y para qué sirve?',
    '["que es sismowatch", "para que sirve sismowatch", "objetivo de la plataforma", "sobre sismowatch", "que hace este sistema", "superapp sismowatch", "ke es sismowatch", "pa que sirve esto", "que es esta app", "komo funciona sismowatch", "de que trata este programa"]'::jsonb,
    'SismoWatch es el producto corporativo de datos de SuperApp para monitoreo sísmico global casi en tiempo real.

| Componente | Función Principal |
| :--- | :--- |
| Ingesta (Airflow) | Consulta el catálogo público de USGS cada 5 minutos |
| Lago Medallion (MinIO + Spark) | Procesa capas Bronce (JSON crudo), Plata (Parquet validado) y Oro (Parquet analítico) |
| Calidad (Pandera) | Valida reglas por fila y compuerta por lote antes de publicar |
| Servicio (PostgreSQL + FastAPI + React) | Sirve datos curados, mapa en vivo SSE y asistente de IA |',
    1
),
(
    'magnitud',
    '¿Qué significa la magnitud de un sismo y cómo se interpreta?',
    '["que es la magnitud", "como se mide la magnitud", "escala de magnitud", "magnitud richter", "mw ml mb", "diferencia de magnitud", "que tan fuerte es un sismo", "komo se mide un temblor", "escala richter", "magnitud de un terremoto", "cuando un sismo es fuerte"]'::jsonb,
    'La magnitud mide la energía liberada en el hipocentro del sismo en una escala logarítmica (cada punto entero equivale a cerca de 32 veces más energía liberada).

| Rango de Magnitud | Clasificación Sísmica | Efecto Típico |
| :--- | :--- | :--- |
| Menor a 3.0 | Microsismo | Generalmente instrumental, no perceptible |
| 3.0 a 3.9 | Menor | Perceptible levemente cerca del epicentro |
| 4.0 a 4.9 | Ligero | Sacudida notable en interiores, rara vez causa daños |
| 5.0 a 5.9 | Moderado | Puede causar daños leves en estructuras vulnerables |
| 6.0 o superior | Fuerte / Mayor | Alto potencial destructivo y vigilancia oceánica |',
    2
),
(
    'profundidad',
    '¿Qué indica la profundidad (depth_km) de un evento sísmico?',
    '["que es la profundidad", "depth_km", "profundidad de un sismo", "sismo superficial o profundo", "hipocentro profundidad", "rango de profundidad", "que tan hondo fue el temblor", "profundidad del terremoto", "komo afecta la profundidad"]'::jsonb,
    'La profundidad indica la distancia vertical en kilómetros desde la superficie terrestre hasta el hipocentro donde se originó la ruptura sísmica.

| Tipo de Sismo | Rango de Profundidad | Impacto en Superficie |
| :--- | :--- | :--- |
| Superficial | 0 a 70 km | Mayor sacudida percibida en superficie |
| Intermedio | 70 a 300 km | Energía atenuada parcialmente por el manto |
| Profundo | 300 a 800 km | Perceptible en zonas amplias pero con menor daño local |',
    3
),
(
    'tsunami',
    '¿Qué significa el indicador de alerta de tsunami?',
    '["que significa tsunami", "alerta de tsunami", "indicador tsunami", "bandera tsunami", "riesgo de maremoto", "cuando hay tsunami", "ola gigante", "peligro de maremoto", "tsunami activo", "alerta maremoto"]'::jsonb,
    'El indicador de tsunami (1 = activo, 0 = sin alerta) señala que el evento sísmico fue vinculado por la NOAA y USGS con un boletín de vigilancia oceánica (usualmente sismos costeros o submarinos de magnitud relevante). Activa protocolos de monitoreo marítimo, aunque no implica necesariamente un tsunami destructivo.',
    4
),
(
    'significancia',
    '¿Qué es el índice de significancia (sig) de USGS?',
    '["que es sig", "significancia del sismo", "indice de significancia", "puntaje sig", "como se calcula sig", "importancia del sismo", "nivel sig"]'::jsonb,
    'La significancia (sig) es un índice compuesto calculado por USGS (de 0 a más de 1000) que combina la magnitud, la intensidad máxima percibida y la presencia de alerta de tsunami. Un valor superior a 600 identifica eventos de alto impacto geofísico o poblacional.',
    5
),
(
    'fuente_datos',
    '¿Cuál es la fuente oficial de los datos sísmicos?',
    '["de donde salen los datos", "fuente de datos", "quien provee los sismos", "api usgs", "catalogo usgs", "origen de la informacion", "de donde sacan los temblores", "quien manda los datos", "son reales los datos"]'::jsonb,
    'Todos los datos provienen del catálogo oficial USGS Earthquake Hazards Program (Servicio Geológico de los Estados Unidos). SismoWatch consulta el feed GeoJSON horario cada 5 minutos para la ingesta continua y el servicio FDSNWS para la carga histórica de 30 días.',
    6
),
(
    'frescura',
    '¿Con qué frecuencia se actualizan los datos (frescura)?',
    '["cada cuanto se actualiza", "frescura de los datos", "latencia de datos", "cuando corre el pipeline", "frecuencia de ingesta", "actualizacion de datos", "cada cuanto cargan sismos", "cuando entra data nueva"]'::jsonb,
    'El pipeline de ingesta consulta USGS cada 5 minutos y dispara automáticamente la transformación Medallion en Apache Spark. La compuerta de calidad verifica la frescura del lote y publica inmediatamente los cambios hacia el navegador mediante eventos en vivo (SSE).',
    7
),
(
    'calidad',
    '¿Cómo funciona el control de calidad de datos y qué pasa con los datos inválidos?',
    '["calidad de datos", "como se validan los datos", "pandera", "reglas de calidad", "tabla de cuarentena", "datos rechazados", "filas invalidas", "datos no procesados", "que pasa con los datos malos", "errores de datos"]'::jsonb,
    'SismoWatch aplica un doble control de calidad automatizado:

| Nivel de Control | Herramienta | Reglas Evaluadas |
| :--- | :--- | :--- |
| Validación por Fila (Bronce a Plata) | PySpark | Presencia de ID/tiempo/coordenadas, latitud [-90, 90], longitud [-180, 180], magnitud [-2, 10], profundidad [-5, 800 km] y deduplicación por ID |
| Compuerta por Lote (Plata a Oro) | Pandera | Esquema estricto, frescura del lote, volumen esperado y ratio máximo de rechazos (20%) |

Las filas inválidas se descartan al pasar a Plata (el archivo original permanece inmutable en MinIO Bronce) y se registra su auditoría liviana (ID, regla y motivo) en PostgreSQL.',
    8
),
(
    'uso_dashboard',
    '¿Cómo puedo usar el dashboard y filtrar los sismos?',
    '["como usar el dashboard", "como filtrar sismos", "usar el mapa", "guia del dashboard", "filtros de fecha y magnitud", "como navegar", "komo uso la pagina", "como busco en el mapa"]'::jsonb,
    'En la vista principal del Dashboard puedes filtrar por fecha inicial, fecha final, magnitud mínima y región geográfica. Al aplicar un filtro, las 6 tarjetas superiores de KPIs, el mapa interactivo Leaflet, el listado de eventos y las gráficas estadísticas se sincronizan automáticamente sobre el mismo conjunto de datos.',
    9
),
(
    'roles',
    '¿Qué diferencias hay entre el rol viewer y el rol admin?',
    '["roles de usuario", "diferencia entre viewer y admin", "permisos de admin", "que puede hacer viewer", "acceso administrador", "rbac", "permisos de usuario", "que hace el admin"]'::jsonb,
    'SismoWatch aplica control de acceso basado en roles (RBAC):

| Funcionalidad | Rol Viewer | Rol Admin |
| :--- | :--- | :--- |
| Dashboard, KPIs, Mapa en Vivo y Gráficas | Permitido | Permitido |
| Reportes Diarios y Exportación (CSV / PDF) | Permitido | Permitido |
| Asistente Analítico de IA (Lenguaje Natural) | Permitido | Permitido |
| Consultas SQL Directas y Auditoría de Rechazos | Restringido | Permitido |
| Disparo de Backfill y Regeneración de Reporte IA | Restringido | Permitido |',
    10
),
(
    'exportacion',
    '¿Cómo puedo exportar o descargar los reportes y datos?',
    '["como exportar", "descargar csv", "descargar pdf", "exportar reportes", "bajar datos", "formato de exportacion", "guardar en excel", "imprimir reporte"]'::jsonb,
    'Desde la pestaña Reportes puedes descargar tanto el informe ejecutivo diario como el catálogo de eventos sísmicos en formato CSV (compatible con hojas de cálculo) o en formato HTML imprimible a PDF.',
    11
),
(
    'tiempo_real',
    '¿Cómo funciona el modo EN VIVO (tiempo real con SSE)?',
    '["como funciona el tiempo real", "que es sse", "indicador en vivo", "server sent events", "actualizacion sin recargar", "streaming", "en vivo"]'::jsonb,
    'El sistema opera en micro-lotes de 5 minutos en la ingesta y en tiempo real de consumo hacia el navegador mediante Server-Sent Events (SSE). Cuando se publica una nueva versión de datos curados en PostgreSQL, la API notifica instantáneamente al Dashboard sin recargar la página.',
    12
),
(
    'limitaciones',
    '¿Cuáles son las limitaciones conocidas de SismoWatch?',
    '["limitaciones del sistema", "limitaciones conocidas", "que no hace el sistema", "latencia maxima", "modo local spark", "alcance del sistema"]'::jsonb,
    'Las condiciones operativas de diseño son: 1) Latencia máxima determinada por la cadencia de ingesta de 5 minutos; 2) Procesamiento Apache Spark configurado en modo local[2] para operar en un servidor de 16 GB de RAM sin GPU; 3) El backfill histórico de 30 días filtra magnitudes mayores o iguales a 2.5 para optimizar las ventanas de consulta en USGS.',
    13
),
(
    'postgres_vs_minio',
    '¿Qué guarda PostgreSQL, qué función cumple y en qué se diferencia de MinIO?',
    '["que guarda postgres", "diferencia entre postgres y minio", "para que sirve minio", "para que sirve postgres", "que hace postgresql", "minio vs postgres", "donde se guardan los datos", "que contiene minio"]'::jsonb,
    'MinIO y PostgreSQL cumplen funciones complementarias dentro de la arquitectura Medallion de SismoWatch:

| Característica | MinIO (Data Lake de Objetos S3) | PostgreSQL 16 + pgvector (Base de Datos Relacional y Vectorial) |
| :--- | :--- | :--- |
| Rol arquitectónico | Almacenamiento masivo e histórico de las capas Bronce, Plata y Oro | Capa de servicio de baja latencia, auditoría operativa e inteligencia artificial |
| Formato de datos | Archivos inmutables GeoJSON (.json) en Bronce y archivos columnares Apache Parquet (.parquet) en Plata y Oro | Tablas relacionales indexadas (B-Tree) y vectores semánticos de 1024 dimensiones (HNSW pgvector) |
| Qué almacena | Crudos originales de USGS, catálogo limpio tipado y agregaciones Parquet procesadas por Spark | Eventos curados listos para consulta rápida, KPIs, corridas de calidad, reportes diarios, caché IA, embeddings RAG y usuarios RBAC |
| Quién lo consume | Apache Airflow y los jobs de Apache Spark (nunca se expone directo al frontend) | La API FastAPI (solo lectura/escritura sobre el schema curated) para alimentar la web React en milisegundos |',
    14
),
(
    'arquitectura_medallion',
    '¿Cómo se dividen las capas Bronce, Plata y Oro en la arquitectura Medallion?',
    '["capas bronce plata y oro", "arquitectura medallion", "que es bronce plata oro", "division de las capas", "como funciona el medallion", "medallon de datos"]'::jsonb,
    'La arquitectura Medallion divide el procesamiento de datos en tres etapas progresivas de calidad:

| Capa | Ubicación Física | Formato | Contenido y Propósito |
| :--- | :--- | :--- | :--- |
| Capa Bronce | MinIO bucket bronze/ | GeoJSON crudo (.json) | Copia exacta e inmutable de la respuesta de USGS para respaldo histórico y reprocesamiento |
| Capa Plata | MinIO bucket silver/ y PostgreSQL staging | Apache Parquet (.parquet) | Datos limpios, tipados, sin duplicados y validados fila por fila con PySpark |
| Capa Oro | MinIO bucket gold/ y PostgreSQL curated | Parquet + Tablas SQL | Datos certificados por Pandera y agregaciones listas para ser leídas por la API FastAPI y el Dashboard |',
    15
)
ON CONFLICT (categoria, pregunta_canonica) DO UPDATE
SET patrones = EXCLUDED.patrones,
    respuesta_plantilla = EXCLUDED.respuesta_plantilla,
    orden = EXCLUDED.orden;
