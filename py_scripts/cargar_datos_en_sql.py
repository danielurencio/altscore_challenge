import gc
import sqlite3
from pathlib import Path
from typing import Union
from datetime import datetime

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
        'cache_size': -8000000,     # Cache de 4GB
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
        # Configuraciones de escritura a disco
        'journal_mode': 'OFF',          # Máxima velocidad, sin journaling
        'synchronous': 'OFF',           # Sin espera de confirmación de disco
        'locking_mode': 'EXCLUSIVE',    # Bloqueo exclusivo para mejor rendimiento
        
        # Configuraciones de memoria (optimizadas para 64GB RAM)
        'cache_size': -45000000,        # ~45GB de cache (70% de RAM disponible)
        'temp_store': 'MEMORY',         # Usar memoria para temporales
        'mmap_size': 51539607552,       # ~48GB para memory mapping
        
        # Configuraciones de página y rendimiento
        'page_size': 4096,              # Tamaño óptimo para NVMe
        'auto_vacuum': 'NONE',          # Desactivar auto vacuum para mejor rendimiento
        'busy_timeout': 300000,         # Timeout más largo (5 minutos) para operaciones grandes
        
        # Optimizaciones adicionales
        'threads': 16,                  # Usar todos los cores disponibles
        'analysis_limit': 1000,         # Límite de análisis para optimizador
        'secure_delete': 'OFF',         # Desactivar borrado seguro para mejor rendimiento
        'read_uncommitted': 1,          # Permitir lecturas sin confirmar para mejor rendimiento
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
    start_time = datetime.now()
    progress_file = Path(db_path).parent / 'index_progress.txt'
    
    index_commands = [
        "CREATE INDEX IF NOT EXISTS idx_mobility_device ON mobility(device_id)",
        "CREATE INDEX IF NOT EXISTS idx_mobility_h3 ON mobility(h3_index)",
        "CREATE INDEX IF NOT EXISTS idx_mobility_device_h3 ON mobility(device_id, h3_index)",
        "CREATE INDEX IF NOT EXISTS idx_mobility_time ON mobility(timestamp)"
    ]
    
    spatial_setup = [
        "SELECT load_extension('mod_spatialite')",
        "SELECT InitSpatialMetaData(1)"
    ]
    
    spatial_columns = [
        "SELECT AddGeometryColumn('mobility', 'geom', 4326, 'POINT', 'XY')",
        "UPDATE mobility SET geom = MakePoint(lon, lat, 4326)",
        "SELECT CreateSpatialIndex('mobility', 'geom')",
        "SELECT UpdateLayerStatistics('mobility', 'geom')"
    ]
    
    temporal_columns = [
        "SELECT CASE WHEN COUNT(*) = 0 THEN 'ALTER TABLE mobility ADD COLUMN fecha DATETIME' ELSE 'SELECT 1' END FROM pragma_table_info('mobility') WHERE name = 'fecha'",
        "UPDATE mobility SET fecha = datetime(timestamp, 'unixepoch')"
    ]
    
    conn = sqlite3.connect(db_path)
    try:
        conn.enable_load_extension(True)
        
        def execute_command(command: str) -> None:
            command_start = datetime.now()
            try:
                conn.execute(command)
                conn.commit()
                command_time = datetime.now() - command_start
                with open(progress_file, 'a') as f:
                    status_msg = f"Completado: \n{command}\n - Tiempo: {command_time}\n"
                    f.write(status_msg)
                    print(status_msg)
            except sqlite3.OperationalError as e:
                err_msg = f"Error en {command}: {e}\n"
                with open(progress_file, 'a') as f:
                    f.write(err_msg)
                    print(err_msg)

        def execute_command_group(progress_file: str, commands: list, group_name: str) -> None:
            """
            Ejecuta un grupo de comandos y registra su progreso.
        
            Args:
                progress_file: Ruta al archivo de progreso
                commands: Lista de comandos a ejecutar
                group_name: Nombre del grupo de comandos para el mensaje
            """
            message = f"\n{group_name}..."
            with open(progress_file, 'a') as f:
                f.write(message)
                print(message)
            for cmd in commands:
                execute_command(cmd)

        command_groups = [
            (index_commands, "Creando índices básicos"),
            #(spatial_setup, "Configurando extensión espacial"),
            #(spatial_columns, "Creando columnas espaciales"),
            (temporal_columns, "Creando columnas temporales")
        ]
        
        for commands, group_name in command_groups:
            execute_command_group(progress_file, commands, group_name)
            
    finally:
        conn.close()
    
    total_time = datetime.now() - start_time
    print(f"\nTiempo total de ejecución: {total_time}")




if __name__ == '__main__':
    cargar_datos('altscore_data/mobility_data.parquet')
    configurar_para_indices('db/mobility.db')
    creacion_de_indices('db/mobility.db')
