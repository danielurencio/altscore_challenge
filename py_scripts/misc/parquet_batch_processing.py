import gc
from datetime import datetime

import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
from shapely.geometry import Point
from tqdm import tqdm


def get_country_for_coords(df, lat_col, lng_col):
    '''
    Convirte coordenadas a GeoDataFrame, hace union con "world map" y
    añade información de país
    '''
    df = df.copy()
    points = gpd.GeoDataFrame(
        df,
        geometry=[Point(xy) for xy in zip(df[lng_col], df[lat_col])],
        crs="EPSG:4326"
    )

    result = gpd.sjoin(points, world, how='left', predicate='within')
    df.loc[:, 'country'] = result['name'].fillna('NA')  # Los nulos son "NAs"
    
    # Obtengamos solamente los valores únicos
    paises = df.country.unique().tolist()
    # Liberemos memoria
    del df
    gc.collect()

    return paises


# Tamaño de cada batch para procesar el archivo parquet en chunks
batch_size = 10000

# Cargar el archivo parquet que contiene datos de movilidad
parquet_file = pq.ParquetFile('altscore_data/mobility_data.parquet')

# Cargar el GeoJSON con la geometría de todos los países del mundo
world = gpd.read_file('https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json')
world = world.to_crs(crs="EPSG:4326")

# Lista para almacenar países únicos encontrados
paises = list()

# Variable para rastrear cambios en el tamaño de la lista de países
arr_size = len(paises)

total_rows = parquet_file.metadata.num_rows
start_time = datetime.now()

with tqdm(total=total_rows, desc="Procesando registros") as pbar:
    records_processed = 0
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        batch_size = len(df_batch)
        records_processed += batch_size

        paises_en_df = get_country_for_coords(df_batch, 'lat', 'lon')

        for pais in paises_en_df:
            if pais not in paises:
                paises.append(pais)
                print(f"\nNuevo país encontrado: {pais}")

        # Actualizar barra y mostrar estadísticas
        pbar.update(batch_size)
        pbar.set_postfix({
            'Países': len(paises),
            'Registros': f"{records_processed:,}/{total_rows:,}",
            'Progreso': f"{(records_processed/total_rows)*100:.1f}%"
        })

        del df_batch
        gc.collect()

tiempo_total = datetime.now() - start_time
print(f"\nTiempo total de procesamiento: {tiempo_total}")
