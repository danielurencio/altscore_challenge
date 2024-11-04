import os
import zipfile
import time
from urllib.request import urlretrieve

import fiona
import geopandas as gpd
import pandas as pd
import h3
from shapely.geometry import Polygon
from tqdm import tqdm
os.makedirs('resultados', exist_ok=True)

# 1. Primero leemos tu archivo de hexágonos
hex = pd.read_csv('hex.csv.gzip', compression='gzip')

# 2. Convertimos los hexágonos a GeoDataFrame
def h3_to_polygon_(h3_address):
    coords = h3.cell_to_boundary(h3_address)
    return Polygon(coords)

def h3_to_polygon(h3_address):
    # Get the coordinates (returns [lat, lng] pairs)
    coords = h3.cell_to_boundary(h3_address)
    # Flip coordinates to [lng, lat] pairs for Shapely
    coords_flipped = [(lng, lat) for lat, lng in coords]
    return Polygon(coords_flipped)


print("Convirtiendo hexágonos a geometrías...")
geometries = [h3_to_polygon(h3_id) for h3_id in tqdm(hex['hex_id'])]
hex_gdf = gpd.GeoDataFrame(hex, geometry=geometries, crs="EPSG:4326")

# 3. Leemos las capas de la geodatabase
file_path = 'GEODATABASE_NACIONAL_2021/GEODATABASE_NACIONAL_2021.gdb'
layers = fiona.listlayers(file_path)

# Definimos qué tipo de capa es cada una
point_layers = ['loc_p', 'viv_p']
line_layers = ['ejes_l', 'ingresos_l']
polygon_layers = ['aream_a', 'ca04_a', 'man_a', 'sec_a', 'zon_a']

results = {}

# 4. Procesamos cada tipo de capa
for layer_name in point_layers:
    print(f"\nProcesando capa de puntos: {layer_name}")
    gdf = gpd.read_file(file_path, layer=layer_name).to_crs('EPSG:4326')
    results[layer_name] = gpd.sjoin(hex_gdf, gdf, how='left', predicate='contains')
    print(f"Resultado: {len(results[layer_name])} registros")

for layer_name in line_layers:
    print(f"\nProcesando capa de líneas: {layer_name}")
    gdf = gpd.read_file(file_path, layer=layer_name).to_crs('EPSG:4326')
    results[layer_name] = gpd.sjoin(hex_gdf, gdf, how='left', predicate='intersects')
    print(f"Resultado: {len(results[layer_name])} registros")

for layer_name in polygon_layers:
    print(f"\nProcesando capa de polígonos: {layer_name}")
    gdf = gpd.read_file(file_path, layer=layer_name).to_crs('EPSG:4326')
    results[layer_name] = gpd.sjoin(hex_gdf, gdf, how='left', predicate='intersects')
    print(f"Resultado: {len(results[layer_name])} registros")

# 5. Opcional: Guardar resultados
for layer_name, result_gdf in results.items():
    output_file = f"resultados/{layer_name}_hex_join.gpkg"
    result_gdf.to_file(output_file, driver="GPKG")
