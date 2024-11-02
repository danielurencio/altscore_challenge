import gc
import sqlite3
from pathlib import Path
from typing import Union

import h3
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm



def configurar_conexion() -> sqlite3.Connection:
    """Configura SQLite para máxima velocidad de escritura"""
    conn = sqlite3.connect('db/mobility.db')
    # Configuraciones optimizadas para escritura masiva
    pragmas = {
        'journal_mode': 'OFF',      # Deshabilita journaling para máxima velocidad
        'synchronous': 'OFF',       # Deshabilita sincronización con disco
        'cache_size': -2000000,     # Cache de 2GB
        'temp_store': 'MEMORY'      # Temporales en memoria
    }
    
    for setting, value in pragmas.items():
        conn.execute(f'PRAGMA {setting} = {value}')
    
    # Esquema simple sin índices
    conn.execute('''
        CREATE TABLE IF NOT EXISTS mobility (
            device_id TEXT,
            timestamp INTEGER,
            lat REAL,
            lon REAL,
            h3_index TEXT
        )
    ''')
    return conn



def procesar_coordenadas(df: pd.DataFrame, resolution: int = 9) -> pd.DataFrame:
    """Agrega índices H3 a las coordenadas"""
    df['h3_index'] = [
        h3.latlng_to_cell(lat, lon, resolution)
        for lat, lon in zip(df['lat'], df['lon'])
    ]
    return df



def cargar_datos(
    ruta_parquet: Union[str, Path],
    batch_size: int = 300000,
    chunk_size: int = 10000
) -> None:
    """Carga datos de Parquet a SQLite optimizada para escritura"""
    conn = configurar_conexion()
    pf = pq.ParquetFile(ruta_parquet)
    
    with tqdm(total=pf.metadata.num_rows, desc="Cargando datos") as pbar:
        for batch in pf.iter_batches(batch_size=batch_size):
            try:
                df_batch = procesar_coordenadas(batch.to_pandas())
                df_batch.to_sql(
                    'mobility',
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=chunk_size
                )
                pbar.update(len(df_batch))
            except Exception as e:
                print(e)
            finally:
                del df_batch
                gc.collect()
    
    conn.close()



def configurar_para_indices(db_path: Union[str, Path]) -> None:
    """
    Configura SQLite para creación óptima de índices después de carga masiva.
    Cambia de configuración ultra-rápida a configuración balanceada con WAL.
    """
    conn = sqlite3.connect(db_path)
    
    # Configuración optimizada para indexación
    pragmas = {
        'journal_mode': 'WAL',      # Permite concurrencia
        'synchronous': 'NORMAL',    # Balance velocidad/seguridad
        'cache_size': -2000000,     # 2GB cache
        'mmap_size': 30000000000,   # 30GB mapeo en memoria
        'page_size': 4096,          # Tamaño página óptimo
        'temp_store': 'MEMORY'      # Temporales en memoria
    }

    pragmas = {
    'journal_mode': 'OFF',          # Máxima velocidad, sin journaling
    'synchronous': 'OFF',           # Sin espera de confirmación de disco
    'cache_size': -2000000,         # 2GB cache (mantener)
    'temp_store': 'MEMORY',         # Mantener en memoria
    'page_size': 4096,              # Mantener
    'mmap_size': 30000000000,       # Mantener
    'busy_timeout': 60000,          # Timeout para bloqueos
    'locking_mode': 'EXCLUSIVE'     # Bloqueo exclusivo para mejor rendimiento
    }
    
    try:
        for setting, value in pragmas.items():
            conn.execute(f'PRAGMA {setting} = {value}')
        conn.commit()
        print("Base de datos configurada para indexación")
    finally:
        conn.close()



def creacion_de_indices(db_path: Union[str, Path]) -> None:
    """
    Ejecuta script de optimización sobre la base de datos.
    Solo ejecuta los comandos, no realiza configuraciones.
    """
    script = """
    -- Índices básicos
    CREATE INDEX IF NOT EXISTS idx_mobility_device ON mobility(device_id);
    CREATE INDEX IF NOT EXISTS idx_mobility_h3 ON mobility(h3_index);
    CREATE INDEX IF NOT EXISTS idx_mobility_device_h3 ON mobility(device_id, h3_index);
    CREATE INDEX IF NOT EXISTS idx_mobility_time ON mobility(timestamp);

    -- Configuración espacial
    SELECT load_extension('mod_spatialite');
    SELECT InitSpatialMetaData(1);

    -- Columna espacial
    ALTER TABLE mobility ADD COLUMN IF NOT EXISTS geom POINT;
    UPDATE mobility SET geom = MakePoint(lon, lat, 4326);
    SELECT CreateSpatialIndex('mobility', 'geom');
    SELECT UpdateLayerStatistics('mobility', 'geom');

    -- Columna temporal
    ALTER TABLE mobility ADD COLUMN IF NOT EXISTS fecha DATETIME;
    UPDATE mobility SET fecha = datetime(timestamp, 'unixepoch');
    """
    
    conn = sqlite3.connect(db_path)
    try:
        conn.enable_load_extension(True)
        for command in script.strip().split(';'):
            if command.strip():
                try:
                    conn.execute(command)
                    conn.commit()
                    print(f"Ejecutado: {command.strip().split()[0]}")
                except sqlite3.OperationalError as e:
                    print(f"Error en comando {command.strip().split()[0]}: {e}")
    finally:
        conn.close()



if __name__ == '__main__':
    cargar_datos('altscore_data/mobility_data.parquet')
    configurar_para_indices('db/mobility.db')
    creacion_de_indices('db/mobility.db')