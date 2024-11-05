import os

import h3
import json
import pandas as pd
import duckdb

path = 'ecuador_listings'
files = os.listdir(path)

def procesar_coordenadas(df: pd.DataFrame, resolution: int = 9) -> pd.DataFrame:
    """Agrega índices H3 a las coordenadas"""
    df['h3_index'] = [
        h3.latlng_to_cell(lat, lon, resolution)
        for lat, lon in zip(df['lat'], df['lon'])
    ]
    return df

def read_json(x):
    y = f"{path}/{x}"
    json_file = json.loads(open(y, encoding='utf-8').read())
    return json_file


def parse_posting(file):
    splits = file.split('.')[0].split('_')
    hex_id = splits[0]
    property_type = splits[1]
    operation_type = splits[2]
    json_file = read_json(file)

    arr = list()
    try:
        for posting in json_file['mapPostings']:
            dictionary = dict(hex_id=hex_id,
                            property_type=property_type,
                            operation_type=operation_type)
            
            dictionary['lat'] = posting['geolocation']['geolocation']['latitude']
            dictionary['lon'] = posting['geolocation']['geolocation']['longitude']
            dictionary['is_premier'] = posting['premier']
            dictionary['low_price_percentage'] = posting['price']['lowPricePercentage']
            dictionary['operation_type'] = posting['price']['operationType']['name']
            dictionary['operation_type'] = posting['price']['operationType']['operationTypeId']
            dictionary['posting_id'] = posting['postingId']

            for i, p in enumerate(posting['price']['prices']):
                dictionary['price_ix'] = i
                dictionary['price'] = p['amount']
                dictionary['currency'] = p['currency']
                dictionary['formated_amount'] = p['formattedAmount']
                dictionary['iso_code'] = p['isoCode']

            arr.append(dictionary)
    except:
        pass

    return arr

parsed_postings = list()
for file in files:
    parsed_posting = parse_posting(file)
    parsed_postings += parsed_posting
    print(file)

df = pd.DataFrame(parsed_postings)
df = procesar_coordenadas(df, 9)
df.to_csv('parsed_plusvalia_postings.csv.gz',
          compression='gzip', index=False)


q = '''
with base as (

    select distinct
      hex_id
    , posting_id
    , property_type
    , operation_type
    , is_premier
    , price
    from df
    where true
    and property_type in (1, 2, 3, 4, 5)
    and operation_type in (1, 2)

)
select
  hex_id
, count(distinct posting_id) plusv_anuncios_inmobiliarios
, count(distinct case when property_type = 1 and operation_type = 1 then posting_id else null end) plusv_anuncios_casas_venta
, count(distinct case when property_type = 1 and operation_type = 2 then posting_id else null end) plusv_anuncios_casas_renta
, avg(case when property_type = 1 and operation_type = 1 then price else null end) plusv_anuncios_casas_venta_promedio
, avg(case when property_type = 1 and operation_type = 2 then price else null end) plusv_anuncios_casas_renta_promedio
, median(case when property_type = 1 and operation_type = 1 then price else null end) plusv_anuncios_casas_venta_mediana
, median(case when property_type = 1 and operation_type = 2 then price else null end) plusv_anuncios_casas_renta_mediana
, stddev(case when property_type = 1 and operation_type = 1 then price else null end) plusv_anuncios_casas_venta_stddev
, stddev(case when property_type = 1 and operation_type = 2 then price else null end) plusv_anuncios_casas_renta_stddev
, quantile(case when property_type = 1 and operation_type = 1 then price else null end, 0.25) plusv_anuncios_casas_venta_p25
, quantile(case when property_type = 1 and operation_type = 2 then price else null end, 0.25) plusv_anuncios_casas_renta_p25
, quantile(case when property_type = 1 and operation_type = 1 then price else null end, 0.75) plusv_anuncios_casas_venta_p75
, quantile(case when property_type = 1 and operation_type = 2 then price else null end, 0.75) plusv_anuncios_casas_renta_p75

, count(distinct case when property_type = 2 and operation_type = 1 then posting_id else null end) plusv_anuncios_deptos_venta
, count(distinct case when property_type = 2 and operation_type = 2 then posting_id else null end) plusv_anuncios_deptos_renta
, avg(case when property_type = 2 and operation_type = 1 then price else null end) plusv_anuncios_deptos_venta_promedio
, avg(case when property_type = 2 and operation_type = 2 then price else null end) plusv_anuncios_deptos_renta_promedio
, median(case when property_type = 2 and operation_type = 1 then price else null end) plusv_anuncios_deptos_venta_mediana
, median(case when property_type = 2 and operation_type = 2 then price else null end) plusv_anuncios_deptos_renta_mediana
, stddev(case when property_type = 2 and operation_type = 1 then price else null end) plusv_anuncios_deptos_venta_stddev
, stddev(case when property_type = 2 and operation_type = 2 then price else null end) plusv_anuncios_deptos_renta_stddev
, quantile(case when property_type = 2 and operation_type = 1 then price else null end, 0.25) plusv_anuncios_deptos_venta_p25
, quantile(case when property_type = 2 and operation_type = 2 then price else null end, 0.25) plusv_anuncios_deptos_renta_p25
, quantile(case when property_type = 2 and operation_type = 1 then price else null end, 0.75) plusv_anuncios_deptos_venta_p75
, quantile(case when property_type = 2 and operation_type = 2 then price else null end, 0.75) plusv_anuncios_deptos_renta_p75

, count(distinct case when property_type = 3 and operation_type = 1 then posting_id else null end) plusv_anuncios_terrenos_venta
, count(distinct case when property_type = 3 and operation_type = 2 then posting_id else null end) plusv_anuncios_terrenos_renta
, avg(case when property_type = 3 and operation_type = 1 then price else null end) plusv_anuncios_terrenos_venta_promedio
, avg(case when property_type = 3 and operation_type = 2 then price else null end) plusv_anuncios_terrenos_renta_promedio
, median(case when property_type = 3 and operation_type = 1 then price else null end) plusv_anuncios_terrenos_venta_mediana
, median(case when property_type = 3 and operation_type = 2 then price else null end) plusv_anuncios_terrenos_renta_mediana
, stddev(case when property_type = 3 and operation_type = 1 then price else null end) plusv_anuncios_terrenos_venta_stddev
, stddev(case when property_type = 3 and operation_type = 2 then price else null end) plusv_anuncios_terrenos_renta_stddev
, quantile(case when property_type = 3 and operation_type = 1 then price else null end, 0.25) plusv_anuncios_terrenos_venta_p25
, quantile(case when property_type = 3 and operation_type = 2 then price else null end, 0.25) plusv_anuncios_terrenos_renta_p25
, quantile(case when property_type = 3 and operation_type = 1 then price else null end, 0.75) plusv_anuncios_terrenos_venta_p75
, quantile(case when property_type = 3 and operation_type = 2 then price else null end, 0.75) plusv_anuncios_terrenos_renta_p75

, count(distinct case when property_type = 4 and operation_type = 1 then posting_id else null end) plusv_anuncios_oficinas_venta
, count(distinct case when property_type = 4 and operation_type = 2 then posting_id else null end) plusv_anuncios_oficinas_renta
, avg(case when property_type = 4 and operation_type = 1 then price else null end) plusv_anuncios_oficinas_venta_promedio
, avg(case when property_type = 4 and operation_type = 2 then price else null end) plusv_anuncios_oficinas_renta_promedio
, median(case when property_type = 4 and operation_type = 1 then price else null end) plusv_anuncios_oficinas_venta_mediana
, median(case when property_type = 4 and operation_type = 2 then price else null end) plusv_anuncios_oficinas_renta_mediana
, stddev(case when property_type = 4 and operation_type = 1 then price else null end) plusv_anuncios_oficinas_venta_stddev
, stddev(case when property_type = 4 and operation_type = 2 then price else null end) plusv_anuncios_oficinas_renta_stddev
, quantile(case when property_type = 4 and operation_type = 1 then price else null end, 0.25) plusv_anuncios_oficinas_venta_p25
, quantile(case when property_type = 4 and operation_type = 2 then price else null end, 0.25) plusv_anuncios_oficinas_renta_p25
, quantile(case when property_type = 4 and operation_type = 1 then price else null end, 0.75) plusv_anuncios_oficinas_venta_p75
, quantile(case when property_type = 4 and operation_type = 2 then price else null end, 0.75) plusv_anuncios_oficinas_renta_p75

, count(distinct case when property_type = 5 and operation_type = 1 then posting_id else null end) plusv_anuncios_locales_venta
, count(distinct case when property_type = 5 and operation_type = 2 then posting_id else null end) plusv_anuncios_locales_renta
, avg(case when property_type = 5 and operation_type = 1 then price else null end) plusv_anuncios_locales_venta_promedio
, avg(case when property_type = 5 and operation_type = 2 then price else null end) plusv_anuncios_locales_renta_promedio
, median(case when property_type = 5 and operation_type = 1 then price else null end) plusv_anuncios_locales_venta_mediana
, median(case when property_type = 5 and operation_type = 2 then price else null end) plusv_anuncios_locales_renta_mediana
, stddev(case when property_type = 5 and operation_type = 1 then price else null end) plusv_anuncios_locales_venta_stddev
, stddev(case when property_type = 5 and operation_type = 2 then price else null end) plusv_anuncios_locales_renta_stddev
, quantile(case when property_type = 5 and operation_type = 1 then price else null end, 0.25) plusv_anuncios_locales_venta_p25
, quantile(case when property_type = 5 and operation_type = 2 then price else null end, 0.25) plusv_anuncios_locales_renta_p25
, quantile(case when property_type = 5 and operation_type = 1 then price else null end, 0.75) plusv_anuncios_locales_venta_p75
, quantile(case when property_type = 5 and operation_type = 2 then price else null end, 0.75) plusv_anuncios_locales_renta_p75
from base
group by 1
'''

conn = duckdb.connect()
conn.register('df', df)

r = conn.execute(q).df()
conn.close()
r.to_csv('dim_plusv_info_inmobiliaria.csv.gz', index=False, compression='gzip')