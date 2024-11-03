import sqlite3


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
    WITH device_history AS (
        SELECT 
            h3_index,
            device_id,
            COUNT(DISTINCT date(fecha)) as days_visited,
            MIN(date(fecha)) as first_visit,
            COUNT(DISTINCT CASE 
                WHEN fecha >= date(MIN(fecha), '+1 day') 
                THEN date(fecha) 
            END) as return_days
        FROM mobility
        GROUP BY h3_index, device_id
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
    select distinct hex_3_index from dim_01__basic_metrics;
    """

]


def execute_queries(db_path, queries):
    """
    Ejecuta una lista de queries SQL para crear tablas de métricas.

    Args:
        db_path (str): Ruta al archivo de la base de datos SQLite
        queries (list): Lista de strings conteniendo los queries SQL
    """
    with sqlite3.connect(db_path) as conn:
        for query in queries:
            print('\n\n')
            print(query)
            try:
                conn.executescript(query)
                conn.commit()
            except sqlite3.Error as e:
                print(f"Error ejecutando query: {str(e)}")
                continue


if __name__ == "__main__":
    execute_queries('db/mobility.db', queries)
