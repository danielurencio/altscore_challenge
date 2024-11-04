import os

import json
import pandas as pd
import h3

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