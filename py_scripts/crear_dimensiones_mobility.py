from pathlib import Path
from datetime import datetime
import sqlite3

import pandas as pd


queries = [
    """
    -- Query 1: Métricas básicas de conteo y actividad diaria
    -- Primero crear la tabla
    CREATE TABLE dim_01__basic_metrics (
        h3_index TEXT PRIMARY KEY,
        devices_count INTEGER,
        avg_daily_visits REAL,
        temporal_diversity INTEGER
    );
    
    -- Luego insertar los datos
    INSERT INTO dim_01__basic_metrics
    WITH daily_counts AS (
        SELECT h3_index,
               date(fecha) as fecha_dia,
               COUNT(*) as daily_visits,
               COUNT(DISTINCT device_id) as daily_devices,
               COUNT(DISTINCT strftime('%H', fecha)) as hours_active
        FROM mobility
        GROUP BY h3_index, date(fecha)
    )
    SELECT 
        h3_index,
        COUNT(DISTINCT daily_devices) as devices_count,
        AVG(daily_visits * 1.0) as avg_daily_visits,
        MAX(hours_active) as temporal_diversity
    FROM daily_counts
    GROUP BY h3_index;
    """,

    """
    -- Query 2: Métricas de patrones temporales
    -- Primero crear la tabla
    CREATE TABLE dim_02__temporal_metrics (
        h3_index TEXT PRIMARY KEY,
        peak_hour_activity TEXT,
        night_activity_ratio REAL,
        weekend_ratio REAL
    );
    
    -- Luego insertar los datos
    INSERT INTO dim_02__temporal_metrics
    WITH hourly_counts AS (
        SELECT 
            h3_index,
            strftime('%H', fecha) as hour,
            strftime('%w', fecha) as dow,
            COUNT(*) as visits
        FROM mobility
        GROUP BY h3_index, strftime('%H', fecha), strftime('%w', fecha)
    )
    SELECT 
        h3_index,
        MAX(CASE WHEN rn = 1 THEN hour END) as peak_hour_activity,
        SUM(CASE WHEN hour >= '22' OR hour < '06' THEN visits ELSE 0 END) * 1.0 /
            NULLIF(SUM(CASE WHEN hour >= '06' AND hour < '22' THEN visits ELSE 0 END), 0) as night_activity_ratio,
        SUM(CASE WHEN dow IN ('0','6') THEN visits ELSE 0 END) * 1.0 /
            NULLIF(SUM(CASE WHEN dow NOT IN ('0','6') THEN visits ELSE 0 END), 0) as weekend_ratio
    FROM (
        SELECT 
            *,
            RANK() OVER (PARTITION BY h3_index ORDER BY visits DESC) as rn
        FROM hourly_counts
    )
    GROUP BY h3_index;
    """,

    """
    -- Query 3: Métricas de lealtad y rotación
    -- Primero crear la tabla
    CREATE TABLE dim_03__loyalty_metrics (
        h3_index TEXT PRIMARY KEY,
        device_loyalty REAL,
        device_turnover REAL
    );
    
    -- Luego insertar los datos
    INSERT INTO dim_03__loyalty_metrics
CREATE TABLE dim_03__loyalty_metrics (
    h3_index TEXT PRIMARY KEY,
    device_loyalty REAL,
    device_turnover REAL
);

INSERT INTO dim_03__loyalty_metrics
WITH device_first_visits AS (
    -- Primero obtenemos la primera visita por dispositivo y hexágono
    SELECT 
        h3_index,
        device_id,
        MIN(date(fecha)) as first_visit
    FROM mobility
    GROUP BY h3_index, device_id
),
device_history AS (
    -- Luego calculamos las métricas por dispositivo
    SELECT 
        f.h3_index,
        f.device_id,
        COUNT(DISTINCT date(m.fecha)) as days_visited,
        COUNT(DISTINCT CASE 
            WHEN date(m.fecha) > f.first_visit THEN date(m.fecha)
            ELSE NULL
        END) as return_days
    FROM device_first_visits f
    JOIN mobility m ON f.device_id = m.device_id 
        AND f.h3_index = m.h3_index
    GROUP BY f.h3_index, f.device_id
)
SELECT 
    h3_index,
    AVG(days_visited * 1.0) as device_loyalty,
    AVG(CASE WHEN return_days > 0 THEN 1 ELSE 0 END) as device_turnover
FROM device_history
GROUP BY h3_index;
    """,
    """
    -- Query 4: Fact Table
    -- Primero crear la tabla
    CREATE TABLE dim_00__fact_table (
        h3_index TEXT PRIMARY KEY
    );
    INSERT INTO dim_00__fact_table
    select distinct h3_index from dim_01__basic_metrics;
    """, # Query 4: Métricas de permanencia temporal
    """
    CREATE TABLE IF NOT EXISTS dim_04__duration_metrics (
        h3_index TEXT PRIMARY KEY,
        avg_visit_duration REAL,
        long_visits_ratio REAL,
        repeat_visits_same_day REAL
    );
    
    INSERT INTO dim_04__duration_metrics
    WITH visit_durations AS (
        SELECT 
            h3_index,
            device_id,
            date(fecha) as visit_date,
            (MAX(timestamp) - MIN(timestamp))/60.0 as duration_minutes,
            COUNT(*) as visits_per_day
        FROM mobility
        GROUP BY h3_index, device_id, date(fecha)
    )
    SELECT 
        h3_index,
        AVG(duration_minutes) as avg_visit_duration,
        AVG(CASE WHEN duration_minutes > 60 THEN 1.0 ELSE 0.0 END) as long_visits_ratio,
        AVG(CASE WHEN visits_per_day > 1 THEN visits_per_day ELSE 0 END) as repeat_visits_same_day
    FROM visit_durations
    GROUP BY h3_index;
    """,

    # Query 5: Métricas de regularidad
    """
    CREATE TABLE IF NOT EXISTS dim_05__regularity_metrics (
        h3_index TEXT PRIMARY KEY,
        morning_ratio REAL,
        afternoon_ratio REAL,
        evening_ratio REAL,
        weekly_routine_score REAL
    );
    
INSERT OR REPLACE INTO dim_05__regularity_metrics 
WITH hourly_patterns AS (
    SELECT 
        h3_index,
        strftime('%H', fecha) as hora,
        strftime('%w', fecha) as day_of_week,
        COUNT(*) as visits,
        SUM(COUNT(*)) OVER (PARTITION BY h3_index) as total_visits
    FROM mobility
    GROUP BY h3_index, strftime('%H', fecha), strftime('%w', fecha)
),
day_counts AS (
    SELECT 
        h3_index,
        strftime('%w', fecha) as day_of_week,
        COUNT(*) as day_count
    FROM mobility
    GROUP BY h3_index, strftime('%w', fecha)
),
weekly_stats AS (
    SELECT
        h3_index,
        AVG(CAST(day_count AS FLOAT)) as avg_count,
        SQRT(
            AVG(CAST(day_count * day_count AS FLOAT)) - 
            AVG(CAST(day_count AS FLOAT)) * AVG(CAST(day_count AS FLOAT))
        ) as std_count
    FROM day_counts
    GROUP BY h3_index
)
SELECT 
    h.h3_index,
    SUM(CASE WHEN CAST(hora as INTEGER) BETWEEN 6 AND 11 THEN visits ELSE 0 END) * 1.0 / total_visits as morning_ratio,
    SUM(CASE WHEN CAST(hora as INTEGER) BETWEEN 12 AND 17 THEN visits ELSE 0 END) * 1.0 / total_visits as afternoon_ratio,
    SUM(CASE WHEN CAST(hora as INTEGER) BETWEEN 18 AND 21 THEN visits ELSE 0 END) * 1.0 / total_visits as evening_ratio,
    1 - (MAX(w.std_count) / NULLIF(MAX(w.avg_count), 0)) as weekly_routine_score
FROM hourly_patterns h
LEFT JOIN weekly_stats w ON h.h3_index = w.h3_index
GROUP BY h.h3_index;
    """,

#     # Query 6: Métricas de conectividad
#     """
#     CREATE TABLE IF NOT EXISTS dim_06__connectivity_metrics (
#         h3_index TEXT PRIMARY KEY,
#         unique_next_hexs INTEGER,
#         return_probability REAL,
#         avg_connected_hexs REAL
#     );

# INSERT OR REPLACE INTO dim_06__connectivity_metrics
# WITH device_daily_locations AS (
#     -- Primero agrupamos por día y dispositivo para reducir el volumen de datos
#     SELECT 
#         device_id,
#         h3_index,
#         date(fecha) as visit_date,
#         MIN(timestamp) as first_timestamp
#     FROM mobility
#     GROUP BY device_id, h3_index, date(fecha)
# ),
# device_movements AS (
#     -- Ahora calculamos movimientos usando datos pre-agregados
#     SELECT 
#         m1.h3_index as origin_hex,
#         m2.h3_index as next_hex,
#         m1.device_id,
#         COUNT(*) as transitions,
#         MIN((m2.first_timestamp - m1.first_timestamp)/3600.0) as hours_between
#     FROM device_daily_locations m1
#     JOIN device_daily_locations m2 ON 
#         m1.device_id = m2.device_id 
#         AND m2.first_timestamp > m1.first_timestamp
#         AND m2.h3_index != m1.h3_index
#         AND m2.first_timestamp - m1.first_timestamp <= 86400  -- 24 horas en segundos
#         AND m1.visit_date = m2.visit_date  -- Restricción adicional para reducir joins
#     GROUP BY m1.h3_index, m2.h3_index, m1.device_id
# ),
# daily_hex_counts AS (
#     -- Pre-calculamos conteos diarios una vez
#     SELECT 
#         device_id,
#         visit_date,
#         COUNT(DISTINCT h3_index) as hexs_visited
#     FROM device_daily_locations
#     GROUP BY device_id, visit_date
# ),
# hex_averages AS (
#     -- Pre-calculamos promedios por hexágono
#     SELECT 
#         m.h3_index,
#         AVG(CAST(dhc.hexs_visited as FLOAT)) as avg_hexs
#     FROM mobility m
#     JOIN daily_hex_counts dhc ON dhc.device_id = m.device_id
#     GROUP BY m.h3_index
# )
# SELECT 
#     dm.origin_hex as h3_index,
#     COUNT(DISTINCT dm.next_hex) as unique_next_hexs,
#     COUNT(DISTINCT CASE WHEN dm.hours_between <= 24 THEN dm.device_id END) * 1.0 / 
#         NULLIF(COUNT(DISTINCT dm.device_id), 0) as return_probability,
#     ha.avg_hexs as avg_connected_hexs
# FROM device_movements dm
# LEFT JOIN hex_averages ha ON dm.origin_hex = ha.h3_index
# GROUP BY dm.origin_hex;
#    """,
    """
    CREATE TABLE mobility_dimensions AS
    SELECT 
       *
    FROM dim_00__fact_table f
    LEFT JOIN dim_01__basic_metrics USING (h3_index)
    LEFT JOIN dim_02__temporal_metrics USING (h3_index)
    LEFT JOIN dim_03__loyalty_metrics USING (h3_index)
    LEFT JOIN dim_04__duration_metrics USING (h3_index)
    LEFT JOIN dim_05__regularity_metrics USING (h3_index)
    """
]


# def execute_queries(db_path, queries):
#     """
#     Ejecuta una lista de queries SQL para crear tablas de métricas.

#     Args:
#         db_path (str): Ruta al archivo de la base de datos SQLite
#         queries (list): Lista de strings conteniendo los queries SQL
#     """
#     with sqlite3.connect(db_path) as conn:
#         for query in queries:
#             print('\n\n')
#             print(query)
#             try:
#                 conn.executescript(query)
#                 conn.commit()
#             except sqlite3.Error as e:
#                 print(f"Error ejecutando query: {str(e)}")
#                 continue
def execute_queries(db_path, queries):
    """
    Ejecuta una lista de queries SQL para crear tablas de métricas.

    Args:
        db_path (str): Ruta al archivo de la base de datos SQLite
        queries (list): Lista de strings conteniendo los queries SQL
    """
    # Crear archivo de progress
    progress_file = Path(db_path).parent / 'dim_progress.txt'
    
    # Tiempo inicial
    start_time = datetime.now()
    last_time = start_time
    
    def log_message(message):
        """Helper function para logging tanto a consola como a archivo"""
        current_time = datetime.now()
        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
        formatted_message = f"[{timestamp}] {message}"
        
        # Print a consola
        print(formatted_message)
        
        # Escribir a archivo
        with open(progress_file, 'a') as f:
            f.write(formatted_message + '\n')
    
    with sqlite3.connect(db_path) as conn:
        for i, query in enumerate(queries, 1):
            # Calcular tiempo transcurrido
            current_time = datetime.now()
            elapsed_since_start = (current_time - start_time).total_seconds() / 60
            elapsed_since_last = (current_time - last_time).total_seconds() / 60
            
            # Logging del inicio del query
            log_message(f"\n\nEjecutando Query {i}/{len(queries)}")
            log_message(f"Tiempo transcurrido desde inicio: {elapsed_since_start:.2f} minutos")
            log_message(f"Tiempo desde último query: {elapsed_since_last:.2f} minutos")
            log_message("Query a ejecutar:")
            log_message(query)
            
            try:
                # Ejecutar query
                conn.executescript(query)
                conn.commit()
                
                # Logging del éxito
                log_message(f"Query {i} ejecutado exitosamente")
                
            except sqlite3.Error as e:
                # Logging del error
                log_message(f"Error ejecutando query {i}: {str(e)}")
                
            # Actualizar tiempo del último query
            last_time = datetime.now()


if __name__ == "__main__":
    execute_queries('db/mobility.db', queries)

    db_path = "db/mobility.db"
    query = "SELECT * FROM mobility_dimensions"

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn)
        df.to_csv('db/mobility_dimensions.csv.gz', index=False, compression='gzip')
