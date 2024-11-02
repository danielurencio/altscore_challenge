import h3
import pandas as pd
import numpy as np

import geopandas as gpd
from shapely.geometry import Point
from multiprocessing import Pool


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
    return df




# Read mobility data
data = pd.read_parquet('altscore_data/mobility_data.parquet')
world = gpd.read_file('https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json')
step_size = 5000

arr = []
arr_size = len(arr)
for d in range(data.index.min(), data.index.max(), step_size):
    x = data.loc[d:(d + step_size), :]
    x = get_country_for_coords(x, 'lat', 'lon')
    country = x.country.unique()[0]

    if country not in arr:
        arr.append(country)

    if len(arr) != arr_size:
        print(arr)
        arr_size = len(arr)