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

if __name__ == '__main__':
    cargar_datos('altscore_data/mobility_data.parquet')