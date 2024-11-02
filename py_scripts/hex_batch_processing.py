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
    df['h3_index'] = df.apply(
        lambda row: h3.latlng_to_cell(row[lat_col], row[lng_col], resolution), 
        axis=1
    )
    
    # Cuáles son los hexágonos únicos?
    hexagons = df.h3_index.unique().tolist()
    
    # Liberar memoria
    del df
    gc.collect()

    return hexagons


# Tanaño de batch
batch_size = 300000

# Usamos pyarrow para generar una especie de generador / iterador
parquet_file = pq.ParquetFile('altscore_data/mobility_data.parquet')

# Lista para almacenar los hexágonos
hexagons = set()  # Un set es mejor para encontrar registros únicos

# Cuántas filas en total tenemos? Saber esto nos ayuda a conocer el progreso
total_rows = parquet_file.metadata.num_rows
start_time = datetime.now()

with tqdm(total=total_rows, desc="Processing records") as pbar:
    records_processed = 0
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        batch_size = len(df_batch)
        records_processed += batch_size

        hexagons_in_batch = get_h3_for_coords(df_batch, 'lat', 'lon', resolution=9)
        
        # Tenemos nuevos hexágonos?
        new_hexagons = set(hexagons_in_batch) - hexagons
        if new_hexagons:
            hexagons.update(new_hexagons)
            print(f"\nNew hexagons found: {len(new_hexagons)}")

        # Barra de progreso
        pbar.update(batch_size)
        pbar.set_postfix({
            'Unique Hexagons': len(hexagons),
            'Records': f"{records_processed:,}/{total_rows:,}",
            'Progress': f"{(records_processed/total_rows)*100:.1f}%"
        })

        del df_batch
        gc.collect()

total_time = datetime.now() - start_time
print(f"\nTotal processing time: {total_time}")
print(f"Total unique hexagons found: {len(hexagons)}")
