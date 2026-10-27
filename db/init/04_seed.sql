-- =============================================================================
-- 04_seed.sql — Semilla Inicial de Usuarios (Admin y Viewer) y FAQ Extensa (13 Categorías)
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
    '["que es sismowatch", "para que sirve sismowatch", "objetivo de la plataforma", "sobre sismowatch", "que hace este sistema", "superapp sismowatch"]'::jsonb,
    'SismoWatch es el producto corporativo de datos de SuperApp para monitoreo sísmico global casi en tiempo real. Ingiere datos del catálogo público de USGS cada 5 minutos, los procesa mediante una arquitectura Medallion (Bronce → Plata → Oro) con Apache Spark y Apache Airflow, valida su calidad con Pandera y reglas por fila/lote, y los expone mediante una API segura y este dashboard interactivo con inteligencia artificial.',
    1
),
(
    'magnitud',
    '¿Qué significa la magnitud de un sismo y cómo se interpreta?',
    '["que es la magnitud", "como se mide la magnitud", "escala de magnitud", "magnitud richter", "mw ml mb", "diferencia de magnitud", "que tan fuerte es un sismo"]'::jsonb,
    'La magnitud mide la energía liberada en el hipocentro del sismo en una escala logarítmica (cada punto entero representa ~32 veces más energía). En SismoWatch aceptamos magnitudes válidas en el rango [-2.0, 10.0]: menores a 2.5 suelen ser microsismos instrumentales; entre 4.0 y 5.9 son sismos moderados; de 6.0 a 6.9 fuertes; de 7.0 a 7.9 mayores; y de 8.0 o más son grandes terremotos.',
    2
),
(
    'profundidad',
    '¿Qué indica la profundidad (depth_km) de un evento sísmico?',
    '["que es la profundidad", "depth_km", "profundidad de un sismo", "sismo superficial o profundo", "hipocentro profundidad", "rango de profundidad"]'::jsonb,
    'La profundidad (`depth_km`) indica la distancia vertical en kilómetros desde la superficie hasta el hipocentro donde se originó la ruptura. Los sismos se clasifican en superficiales (0 a 70 km, suelen causar mayor sacudida en superficie), intermedios (70 a 300 km) y profundos (300 a 800 km). Nuestra regla de calidad valida que `depth_km` esté entre -5 km (elevación topográfica) y 800 km.',
    3
),
(
    'tsunami',
    '¿Qué significa el indicador de alerta de tsunami?',
    '["que significa tsunami", "alerta de tsunami", "indicador tsunami", "bandera tsunami", "riesgo de maremoto", "cuando hay tsunami"]'::jsonb,
    'El campo `tsunami` (1 = activo, 0 = sin alerta) indica que el evento sísmico fue vinculado por la NOAA/USGS con un boletín o evaluación de potencial de tsunami (usualmente sismos oceánicos o costeros de magnitud importante). No significa necesariamente que un tsunami destructivo haya ocurrido, sino que activó vigilancia oceánica.',
    4
),
(
    'significancia',
    '¿Qué es el índice de significancia (sig) de USGS?',
    '["que es sig", "significancia del sismo", "indice de significancia", "puntaje sig", "como se calcula sig"]'::jsonb,
    'La significancia (`sig`) es un índice compuesto calculado por USGS (de 0 a >1000) que combina la magnitud, la intensidad máxima reportada (PAGER/MMI) y si existe alerta de tsunami. Un valor mayor a 600 identifica eventos de alto impacto mediático o geofísico.',
    5
),
(
    'fuente_datos',
    '¿Cuál es la fuente oficial de los datos sísmicos?',
    '["de donde salen los datos", "fuente de datos", "quien provee los sismos", "api usgs", "catalogo usgs", "origen de la informacion"]'::jsonb,
    'Todos los datos provienen del catálogo oficial **USGS Earthquake Hazards Program** (Servicio Geológico de los Estados Unidos). Utilizamos el feed GeoJSON horario (`all_hour.geojson`) cada 5 minutos para la ingesta continua y el servicio FDSNWS (`fdsnws/event/1/query`) para la carga histórica (backfill).',
    6
),
(
    'frescura',
    '¿Con qué frecuencia se actualizan los datos (frescura)?',
    '["cada cuanto se actualiza", "frescura de los datos", "latencia de datos", "cuando corre el pipeline", "frecuencia de ingesta", "actualizacion de datos"]'::jsonb,
    'El DAG `usgs_ingest_bronze` consulta USGS cada **5 minutos** y dispara automáticamente `usgs_transform_medallion` mediante Airflow Assets/Datasets. La compuerta de calidad verifica que la frescura del lote no supere el umbral `FRESHNESS_MAX_MINUTES`. Si USGS presenta caída temporal, el sistema emite una alerta y continúa sirviendo el último lote válido indicando su antigüedad exacta.',
    7
),
(
    'calidad',
    '¿Cómo funciona el control de calidad de datos y qué pasa con los datos inválidos?',
    '["calidad de datos", "como se validan los datos", "pandera", "reglas de calidad", "tabla de cuarentena", "datos rechazados", "filas invalidas"]'::jsonb,
    'Aplicamos doble control de calidad: 1) **Por fila en Spark (Bronce → Plata)**: validamos presencia de ID/tiempo/coordenadas, latitud [-90, 90], longitud [-180, 180], magnitud [-2, 10], profundidad [-5, 800 km] y deduplicación por `id` conservando el mayor `updated`. No existe tabla de cuarentena pesada: el archivo crudo permanece inmutable en MinIO Bronce y solo guardamos el rastro liviano `{id, regla, motivo}` en `curated.quality_results`. 2) **Compuerta por lote bloqueante (Pandera + umbrales)**: valida esquema, frescura, volumen y porcentaje máximo de inválidos antes de publicar en Oro.',
    8
),
(
    'uso_dashboard',
    '¿Cómo puedo usar el dashboard y filtrar los sismos?',
    '["como usar el dashboard", "como filtrar sismos", "usar el mapa", "guia del dashboard", "filtros de fecha y magnitud", "como navegar"]'::jsonb,
    'En la vista principal del Dashboard puedes: 1) Filtrar por rango de fechas, magnitud mínima y región; 2) Explorar los 6 KPIs superiores en tiempo real; 3) Hacer clic sobre cualquier marcador del mapa interactivo Leaflet para ver magnitud, profundidad, región y enlace oficial USGS; 4) Analizar las gráficas de serie horaria, distribución por magnitud y top regiones.',
    9
),
(
    'roles',
    '¿Qué diferencias hay entre el rol viewer y el rol admin?',
    '["roles de usuario", "diferencia entre viewer y admin", "permisos de admin", "que puede hacer viewer", "acceso administrador", "rbac"]'::jsonb,
    'SismoWatch implementa control de acceso basado en roles (RBAC) en backend (JWT) y frontend: el rol **viewer** puede consultar el dashboard, mapa en vivo, reportes diarios, exportar datos, usar el chat IA y ver el resumen del pipeline. El rol **admin** tiene acceso adicional al registro detallado de rechazos de calidad, detalle de corridas de DAGs, disparo manual de backfill histórico y regeneración de reportes diarios de IA.',
    10
),
(
    'exportacion',
    '¿Cómo puedo exportar o descargar los reportes y datos?',
    '["como exportar", "descargar csv", "descargar pdf", "exportar reportes", "bajar datos", "formato de exportacion"]'::jsonb,
    'Desde la pestaña **Reportes**, puedes descargar tanto los reportes ejecutivos diarios como el listado de eventos en formato **CSV** (compatible con Excel/análisis) o **HTML imprimible a PDF** usando los botones de exportación en la esquina superior derecha.',
    11
),
(
    'tiempo_real',
    '¿Cómo funciona el modo EN VIVO (tiempo real con SSE)?',
    '["como funciona el tiempo real", "que es sse", "indicador en vivo", "server sent events", "actualizacion sin recargar", "streaming"]'::jsonb,
    'Nuestra arquitectura es micro-batch en ingesta (cadencia de 5 minutos) y **tiempo real en consumo** mediante **Server-Sent Events (SSE)** en `/api/stream`. Cuando `usgs_transform_medallion` publica una nueva versión (`data_version`) en el schema `curated`, la API empuja inmediatamente los nuevos eventos al navegador sin recargar la página, con reconexión automática y heartbeat periódico.',
    12
),
(
    'limitaciones',
    '¿Cuáles son las limitaciones conocidas de SismoWatch?',
    '["limitaciones del sistema", "limitaciones conocidas", "que no hace el sistema", "latencia maxima", "modo local spark"]'::jsonb,
    'Las principales limitaciones diseñadas por alcance son: 1) Latencia ligada a la cadencia de ingesta micro-batch de 5 minutos (no es alerta temprana sísmica de segundos); 2) Apache Spark corre en modo `local[2]` optimizado para un host de 16 GB RAM sin GPU (escalable a clúster cambiando `master`); 3) En el feed histórico de backfill se filtra por defecto `minmagnitude >= 2.5` para respetar los límites por petición de la API pública de USGS.',
    13
)
ON CONFLICT (categoria, pregunta_canonica) DO UPDATE
SET patrones = EXCLUDED.patrones,
    respuesta_plantilla = EXCLUDED.respuesta_plantilla,
    orden = EXCLUDED.orden;
