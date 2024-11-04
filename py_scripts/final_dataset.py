import pandas as pd
import pandasql as psql

dimensions = [
    ('infraestructura', 'dim_infra.csv.gz'),
    ('anuncios_inmobiliarios', 'dim_plusv_info_inmobiliaria.csv.gz'),
    ('movilidad', 'mobility_dimensions.csv.gz')
]

renames = dict(h3_index='hex_id')
dims = {
    k: pd.read_csv(v, compression='gzip').rename(columns=renames)
    for k, v in dimensions
}

q = '''
select
  *
from infraestructura
left join anuncios_inmobiliarios using(hex_id)
left join movilidad using(hex_id)
'''
dataset = psql.sqldf(q, dims)
dataset.to_csv('final_datset.csv.gz', index=False, compression='gzip')