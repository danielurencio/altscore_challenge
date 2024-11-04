import os
from tqdm import tqdm
from urllib.request import urlretrieve
from typing import Dict, List, Literal

import h3
import fiona
import pandas as pd
import pandasql as psql
import geopandas as gpd
from shapely.geometry import Polygon


def h3_to_polygon(h3_address):
    '''
    Regresa un polígono dado un H3_index
    '''
    coords = h3.cell_to_boundary(h3_address)
    # Revertir el orden de las coordenadas
    coords_flipped = [(lng, lat) for lat, lng in coords]
    return Polygon(coords_flipped)


def process_layer(
    hex_gdf: gpd.GeoDataFrame,
    file_path: str,
    layer_name: str,
    geometry_type: Literal['point', 'line', 'polygon'],
    target_crs: str = 'EPSG:4326'
) -> gpd.GeoDataFrame:
    '''
    Realiza operaciones tipo "spatial join" según el tipo
    de geometría
    '''
    # Definir predicado según tipo de geometría
    predicates = {
        'point': 'contains',
        'line': 'intersects',
        'polygon': 'intersects'
    }
    
    print(f"\nProcesando capa de {geometry_type}s: {layer_name}")
    
    # Leer y reproyectar capa
    gdf = gpd.read_file(file_path, layer=layer_name).to_crs(target_crs)
    # Realizar spatial join
    result = gpd.sjoin(hex_gdf, gdf, how='left', predicate=predicates[geometry_type])
    result.drop('geometry', axis=1, inplace=True)
    if 'index_right' in result.columns:
        result = result.drop('index_right', axis=1).drop_duplicates()
    print(f"Resultado: {len(result)} registros")
    return result



def sql_query(query, local_vars):
    """
    Ejecuta query SQL con las variables locales proporcionadas
    """
    return psql.sqldf(query, local_vars)



'''
Inizialización de variables globales
'''
# creamos un directorio para guardar resultados
os.makedirs('resultados', exist_ok=True)
# Leemos un archivo con el conjunto de hexágonos de interés
hex = pd.read_csv('hex.csv.gzip', compression='gzip')

# Obtenemos los polígonos de los hexágonos
print("Convirtiendo hexágonos a geometrías...")
geometries = [h3_to_polygon(h3_id) for h3_id in tqdm(hex['hex_id'])]
hex_gdf = gpd.GeoDataFrame(hex, geometry=geometries, crs="EPSG:4326")

#Leemos las capas de la geodatabase
file_path = 'GEODATABASE_NACIONAL_2021/GEODATABASE_NACIONAL_2021.gdb'
layers = fiona.listlayers(file_path)

# Definimos qué tipo de capa es cada una
point_layers = ['loc_p', 'viv_p']
line_layers = ['ejes_l', 'ingresos_l']
polygon_layers = ['aream_a', 'ca04_a', 'man_a', 'sec_a', 'zon_a']
layers_to_process = [
    (point_layers, 'point'),
    (line_layers, 'line'),
    (polygon_layers, 'polygon')
]

results = {}

# Definición de queries
q_man = '''
with base as (

    select distinct
      hex_id
    , man
    , tot_edif
    , tot_viv
    , Shape_Length
    , Shape_Area
    from man_a

) , stats_edificaciones as (

  select
    hex_id
  , sum(tot_edif)         total_edificaciones
  , sum(tot_viv)          total_viviendas
  , avg(tot_edif)         avg_edificaciones
  , avg(tot_viv)          avg_viviendas
  , count(distinct man)   n_manzanas
  , sum(Shape_Area)       area_total
  from base
  group by 1

)
select
  hex_id
, total_edificaciones
, total_viviendas
, avg_edificaciones
, avg_viviendas
, n_manzanas
, area_total
, total_edificaciones / nullif(area_total, 0) densidad_edificaciones
, total_viviendas / nullif(area_total, 0) densidad_viviendas
from stats_edificaciones
'''


q_aream = '''
with base as (

    select distinct
      hex_id
    , tipo_aream
    from aream_a

)
select
  hex_id
, case when tipo_aream = 'CAPITAL PROVINCIAL' then 1 else 0 end is_aream_capital_provincial
, case when tipo_aream = 'CABECERA PARROQUAL' then 1 else 0 end is_aream_cabecera_parroquial
, case when tipo_aream = 'LOCALIDAD AMANZANADA' then 1 else 0 end is_aream_localidad_amanzanada
, case when tipo_aream = 'CABECERA CANTONAL' then 1 else 0 end is_aream_cabecera_cantonal
, case when tipo_aream is null then 1 else 0 end is_aream_null
from base
'''


q_ejes = """
WITH base AS (

    select distinct
      hex_id
    , tipo_eje
    , nom_eje
    from ejes_l

) , grouped as (

    SELECT
      hex_id
    , count(distinct nom_eje) ejes_unicos
    , count(distinct case when tipo_eje = 'CALLE' then  nom_eje else null end) eje_calles
    , count(distinct case when tipo_eje = 'PASAJE' then  nom_eje else null end) eje_pasajes
    , count(distinct case when tipo_eje = 'AVENIDA' then  nom_eje else null end) eje_avenidas
    , count(distinct case when tipo_eje = 'CALLEJON' then  nom_eje else null end) eje_callejones
    , count(distinct case when tipo_eje = 'PEATONAL' then  nom_eje else null end) eje_peatonales
    , count(distinct case when tipo_eje = 'ESCALINATA' then  nom_eje else null end) eje_escalinatas
    , count(distinct case when tipo_eje = 'PASEO' then  nom_eje else null end) eje_paseos
    , count(distinct case when tipo_eje = 'SENDERO' then  nom_eje else null end) eje_senderos
    , count(distinct case when tipo_eje not in
      ('CALLE', 'PASAJE', 'AVENIDA', 'CALLEJON', 'PEATONAL', 'ESCALINATA', 'PASEO', 'SENDERO')
      then nom_eje else null end) eje_otros
    FROM base
    GROUP BY hex_id
)
select
  *
, CAST(eje_calles AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_calles
, CAST(eje_pasajes AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_pasajes
, CAST(eje_avenidas AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_avenidas
, CAST(eje_callejones AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_callejones
, CAST(eje_peatonales AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_peatonales
, CAST(eje_escalinatas AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_escalinatas
, CAST(eje_paseos AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_paseos
, CAST(eje_senderos AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_senderos
, CAST(eje_otros AS FLOAT) / nullif(ejes_unicos, 0) fraccion_eje_otros
from grouped
"""

q_viv = '''
with base as (

    select distinct
      hex_id
    , man
    , n_edif
    , nom_edif
    , edif_censo
    , cod_otros
    from viv_p

) , grouped as (

    select
    hex_id
    , count(distinct edif_censo)                                                                        edificios_totales
    , count(distinct case when cod_otros = ' ' then  edif_censo else null end)                          edificios_default
    , count(distinct case when cod_otros = 'CAMPO DEPORTIVO' then  edif_censo else null end)            edificios_campos_deportivos
    , count(distinct case when cod_otros is null then edif_censo else null end)                         edificios_null
    , count(distinct case when cod_otros = 'CASA COMUNAL' then  edif_censo else null end)               edificios_casas_comunales
    , count(distinct case when cod_otros = 'CEMENTERIO' then  edif_censo else null end)                 edificios_cementerios
    , count(distinct case when cod_otros = 'EDIFICIO DE REFERENCIA' then  edif_censo else null end)     edificios_de_referencia
    , count(distinct case when cod_otros = 'EDIFICIO EDUCACIONAL' then  edif_censo else null end)       edificios_educacionales
    , count(distinct case when cod_otros = 'EDIFICIO IMPORTANTE' then  edif_censo else null end)        edificios_importantes
    , count(distinct case when cod_otros = 'ESTABLECIMIENTO DE SALUD' then  edif_censo else null end)   edificios_de_salud
    , count(distinct case when cod_otros = 'GASOLINERA' then  edif_censo else null end)                 edificios_gasolineras
    , count(distinct case when cod_otros = 'PARQUE' then  edif_censo else null end)                     edificios_parques
    , count(distinct case when cod_otros = 'PLAZA' then  edif_censo else null end)                      edificios_plazas
    , count(distinct case when cod_otros = 'TEMPLO RELIGIOSO' then  edif_censo else null end)           edificios_templos_religiosos
    from base
    group by 1

)
select
  *
, CAST(edificios_default AS FLOAT) / nullif(edificios_totales, 0)              fraccion_edificios_default
, CAST(edificios_campos_deportivos AS FLOAT) / nullif(edificios_totales, 0)    fraccion_edificios_campos_deportivos
, CAST(edificios_null AS FLOAT) / nullif(edificios_totales, 0)                 fraccion_edificios_null
, CAST(edificios_casas_comunales AS FLOAT) / nullif(edificios_totales, 0)      fraccion_edificios_casas_comunales
, CAST(edificios_cementerios AS FLOAT) / nullif(edificios_totales, 0)          fraccion_edificios_cementerios
, CAST(edificios_de_referencia AS FLOAT) / nullif(edificios_totales, 0)        fraccion_edificios_de_referencia
, CAST(edificios_educacionales AS FLOAT) / nullif(edificios_totales, 0)        fraccion_edificios_educacionales
, CAST(edificios_importantes AS FLOAT) / nullif(edificios_totales, 0)          fraccion_edificios_importantes
, CAST(edificios_de_salud AS FLOAT) / nullif(edificios_totales, 0)             fraccion_edificios_de_salud
, CAST(edificios_gasolineras AS FLOAT) / nullif(edificios_totales, 0)          fraccion_edificios_gasolineras
, CAST(edificios_parques AS FLOAT) / nullif(edificios_totales, 0)              fraccion_edificios_parques
, CAST(edificios_plazas AS FLOAT) / nullif(edificios_totales, 0)               fraccion_edificios_plazas
, CAST(edificios_templos_religiosos AS FLOAT) / nullif(edificios_totales, 0)   fraccion_edificios_templos_religiosos
from grouped
'''

if __name__ == '__main__':
    # El primer paso es procesar todas las capas según su tipo
    # Nuestra base son los hexágonos, por lo que necesitamos hacer
    # spatial joins con diferentes predicados: contains o intersects )
    for layers, geometry_type in layers_to_process:
        for layer_name in tqdm(layers, desc=f"Procesando capas de {geometry_type}s"):
            results[layer_name] = process_layer(
                hex_gdf=hex_gdf,
                file_path=file_path,
                layer_name=layer_name,
                geometry_type=geometry_type
            )

    # Una vez que tenemos info geoespacial para cada hexágono
    # podemos hacer ingeniería de variables a niven hex_id
    print('Computando dimensiones ...')
    results['dim_man'] = sql_query(q_man, results)
    results['dim_aream'] = sql_query(q_aream, results)
    results['dim_ejes'] = sql_query(q_ejes, results)
    results['dim_viv'] = sql_query(q_viv, results)

