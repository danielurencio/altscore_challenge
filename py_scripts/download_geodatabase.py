import os
import zipfile
import time
from urllib.request import urlretrieve

import fiona
import geopandas as gpd


def format_time(seconds):
    """Convierte segundos en un formato más legible.."""
    minutes = int(seconds // 60)
    seconds = seconds % 60
    if minutes > 0:
        return f"{minutes}m {seconds:.2f}s"
    return f"{seconds:.2f}s"


url = "https://www.ecuadorencifras.gob.ec/documentos/web-inec/Geografia_Estadistica/Documentos/GEODATABASE_NACIONAL_2021.zip"
file_name = url.split('/')[-1]
file_path = 'GEODATABASE_NACIONAL_2021/GEODATABASE_NACIONAL_2021.gdb'

if file_name not in os.listdir():
    print('Descargando archivo...')
    urlretrieve(url, file_name)

if 'GEODATABASE_NACIONAL_2021' not in os.listdir():
    print('Descomprimiendo archiivo')
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall('./')


layers = fiona.listlayers(file_path)

gdfs = list()
for i, layer in enumerate(layers, 1):
    print(f"\nLeyendo capa {i}/{len(layers)}: {layer}")
    
    start_time = time.time()
    gdf = gpd.read_file(file_path, layer=layer)
    elapsed_time = time.time() - start_time
    
    print(f"Registros: {len(gdf)}")
    print(f"Tiempo de carga: {format_time(elapsed_time)}")