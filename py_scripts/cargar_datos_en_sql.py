import gc
import sqlite3
from datetime import datetime

import h3
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

def crear_base_datos():
    conn = sqlite3.connect('mobility.db')
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA synchronous = NORMAL')
    conn.execute('PRAGMA cache_size = -2000000')  # 2GB cache
    conn.execute('PRAGMA mmap_size = 2000000000')
    conn.execute('PRAGMA temp_store = MEMORY')
    conn.execute('PRAGMA journal_size_limit = 67110000')
    c = conn.cursor()
    
    # Crear tabla con índices
    c.execute('''
        CREATE TABLE IF NOT EXISTS mobility (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            timestamp DATETIME,
            lat REAL,
            lon REAL
        );
    ''')
    conn.commit()
    return conn


def get_h3_for_coords(df, lat_col, lng_col, resolution=9):
    '''
    Asigna un h-index a cada par de coordenadas
    '''
    df = df.copy()

    # Para cada fila, obtenemos el h-index
    df.loc[:, 'h3_index'] = df.apply(
        lambda row: h3.latlng_to_cell(row[lat_col], row[lng_col], resolution), 
        axis=1
    )

    return df



def cargar_datos_por_chunks(parquet_file,
                            batch_size=100000,
                            chunk_size=10000):
    conn = crear_base_datos()
    pf = pq.ParquetFile(parquet_file)
    total_rows = pf.metadata.num_rows
    
    with tqdm(total=total_rows, desc="Cargando datos") as pbar:
        for batch in pf.iter_batches(batch_size=batch_size):
            df_batch = get_h3_for_coords(batch.to_pandas(),
                                         'lat', 'lon')
            # Insertar batch
            df_batch.to_sql('mobility', 
                           conn, 
                           if_exists='append', 
                           index=False,
                           method='multi',
                           chunksize=chunk_size)
            
            pbar.update(len(df_batch))
            del df_batch
            gc.collect()

    conn.close()


if __name__ == '__main__':
    # Tanaño de batch
    batch_size = 300000
    # Usamos pyarrow para generar una especie de generador / iterador
    parquet_file = pq.ParquetFile('altscore_data/mobility_data.parquet')
    cargar_datos_por_chunks(parquet_file, batch_size=batch_size)