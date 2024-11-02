import gc
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
from shapely.geometry import Point


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

# Lista para almacenar países únicos encontrados
paises = list()

# Variable para rastrear cambios en el tamaño de la lista de países
arr_size = len(paises)

# Iterar sobre el archivo parquet en batches para manejo eficiente de memoria
for batch in parquet_file.iter_batches(batch_size=batch_size):
   # Convertir el batch actual a DataFrame de pandas
   df_batch = batch.to_pandas()

   # Obtener los países correspondientes a las coordenadas en el batch actual
   # usando las columnas 'lat' y 'lon' del DataFrame
   paises_en_df = get_country_for_coords(df_batch, 'lat', 'lon')

   # Agregar nuevos países encontrados a la lista si no están ya presentes
   for pais in paises_en_df:
       if pais not in paises:
           paises.append(pais)

   # Si se encontraron nuevos países, imprimir la lista actualizada
   if len(paises) != arr_size:
       print(paises)
       arr_size = len(paises)

   # Liberar memoria eliminando el DataFrame del batch actual
   del df_batch

   # Forzar la recolección de basura para liberar memoria
   gc.collect()
