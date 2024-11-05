import os
import time
import json
import random
import requests

import h3
import pandas as pd

property_types = [
    {"label": "Departamento", "min": "2"},
    {"label": "Casa", "min": "1"},
    {"label": "Terreno / Lote", "min": "3"},
    {"label": "Local comercial", "min": "5"},
    {"label": "Oficina comercial", "min": "4"},
    {"label": "Suite", "min": "9"},
    {"label": "Bodega-Galpón", "min": "8"},
    {"label": "Consultorio", "min": "10"},
    {"label": "Edificio-Hotel-Fabrica", "min": "7"},
    {"label": "Habitación", "min": "25"},
    {"label": "Hacienda-Quinta", "min": "6"},
    {"label": "Parqueadero", "min": "32"},
    {"label": "Proyecto horizontal", "min": "33"},
    {"label": "Proyecto vertical", "min": "34"},
    {"label": "Quinta vacacional", "min": "11"}
]


operation_types = [
    {"label": "Alquilar", "min": "2"},
    {"label": "Comprar", "min": "1"},
    {"label": "Temporal/Vacacional", "min": "4"},
    {"label": "Proyectos", "min": "desarrollosURL"},
    {"label": "Traspaso", "min": "3"}
]



# Create a directory to save each response as a JSON file
os.makedirs('ecuador_listings', exist_ok=True)

# Define the base URL and headers for the requests
url = 'https://www.plusvalia.com/rplis-api/map/postings'
headers = {
    'accept': '*/*',
    'accept-language': 'es-ES,es;q=0.9',
    'content-type': 'application/json',
    'cookie': '__cf_bm=gkfmUPkTjB_YYmgvA9GbTjakq976fB8F.x4EjjCWckI-1730427118-1.0.1.1-bopipMogopIO_UP9WwUwqD9lGu2qkhqgRaxd3Qs4Gb6MldffzKdNgst7l4sQ0dxtA_A9MKwmQ5olZ9yugeKFRWHD5Uheo1cehgCLv7TzlqA; _cfuvid=A67Fkgm.f_ikrlcyE.sXEhg.4.aVE6djqS2mg6pLxz8-1730427123160-0.0.1.1-604800000; _gcl_au=1.1.435884812.1730427124; _fbp=fb.1.1730427124464.519737899197694416; sessionId=038a0108-944c-4b24-9aad-1a6cde9fddbb; crto_is_user_optout=false; crto_mapped_user_id=RlfTJ3YnBUOJAhlgCt7hArA-W6jtxlh4; _hjSession_212844=eyJpZCI6IjljM2FlZWZhLTQ4NzAtNDlkYS05M2U0LTQwZWE1MmFhYjg4MyIsImMiOjE3MzA0MjcxMjgwNDUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=; _hjSessionUser_212844=eyJpZCI6IjE5YzExYTU4LWFkOWMtNTk0ZS1iNTk1LWZkNzMzODgwNDY3MiIsImNyZWF0ZWQiOjE3MzA0MjcxMjgwNDQsImV4aXN0aW5nIjp0cnVlfQ==; CF_AppSession=n1ec6751380e166ab; _gid=GA1.2.1833227426.1730427284; _ga=GA1.1.901771201.1730427125; __gads=ID=75230bae9bf2ba12:T=1730427125:RT=1730427439:S=ALNI_MbEKDaDwMjQalMnso20qdehidGqWg; __gpi=UID=00000f536e889f44:T=1730427125:RT=1730427439:S=ALNI_MaNWkzprFQ7eWIy3hlpfvvRk1B59A; __eoi=ID=255d0fdb11bc26fd:T=1730427125:RT=1730427439:S=AA-AfjbLrs2oVdFXU2YZc8__gCdY; idUltimoAvisoVisto=66927317; 62558441=visited; cf_clearance=Gh3Qq_Yh62efXBWMv33y.ncphecqRbgzSq1X.XbXKSQ-1730428206-1.2.1.1-tONU3EG_R14wYcwSzdAubL3wDp9s2NiGg41.Xd6BwUL7qggxyBW3jxQIvT.wp5GOfAZwQon7Er.CgSfkYPse8Se_bEjh1gnluKds7bTzSaa8OG7o0w12PCuYJfolQQRwSEhX3wD9E28A5.Es5A5t5EeBS4XYYiKWapjzLUZps6NL6sqCDdYOdXuar3.a0_T.yK6x3u.vKyF2JVFGv5skzG_zbsqv1Yh4eZHfY2UlmF4nnzQ1yr9zPOVMNj3_PTkreHyGe_0dclTdpAqYCG6lLlZDJu2XF8S2rVzO1NX_frYBfwSe1XNcwyBs_enW2PD8270GNZ9SVb3QGTxTD18dn_XH5MtMb8i7Zf4_cQnKZozfiT1l9M2TLSjofyMnpI.aeVyzL4QUnGi5ypfPc3ZzB903MnsCttjFhT_.6Fzdqllbf.neHJPu8ObSw6WI32Ok; JSESSIONID=49B1016462537AF84ED450E5A879DD3B; 144883810=visited; _ga_EFS2CCCL6S=GS1.1.1730427125.1.1.1730428356.43.0.0',
    'origin': 'https://www.plusvalia.com',
    'priority': 'u=1, i',
    'referer': 'https://www.plusvalia.com/venta/casas?listado=map',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"130.0.6723.92"',
    'sec-ch-ua-full-version-list': '"Chromium";v="130.0.6723.92", "Google Chrome";v="130.0.6723.92", "Not?A_Brand";v="99.0.0.0"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

# Define Ecuador's bounding box coordinates (southwest and northeast corners)
ecuador_sw = {'lat': -4.959128, 'lng': -81.078738}  # Southwest corner
ecuador_ne = {'lat': 1.483404, 'lng': -75.192466}   # Northeast corner

# Grid settings
lat_step = 0.5
lng_step = 0.5


def get_hexagon_bbox(h3_index: str) -> tuple:
    """
    Obtiene las coordenadas del bounding box (min_lat, min_lng, max_lat, max_lng)
    que rodea a un hexágono H3.
    
    Args:
        h3_index: Índice H3 del hexágono
    
    Returns:
        tuple: (min_lat, min_lng, max_lat, max_lng)
    """
    # Obtener los vértices del hexágono
    boundaries = h3.cell_to_boundary(h3_index)
    
    # Extraer listas separadas de latitudes y longitudes
    lats = [vertex[0] for vertex in boundaries]
    lngs = [vertex[1] for vertex in boundaries]
    
    # Encontrar los valores mínimos y máximos
    min_lat = min(lats)
    max_lat = max(lats)
    min_lng = min(lngs)
    max_lng = max(lngs)
    
    return (min_lat, min_lng, max_lat, max_lng)


def get_bbox_corners(bbox: tuple) -> dict:
    min_lat, min_lng, max_lat, max_lng = bbox
    return {
        'sw_lat': min_lat,
        'sw_lng': min_lng,
        'ne_lat': max_lat,
        'ne_lng': max_lng
    }

def fetch_listings_by_type(property_key, operation_key):
    property_type = [d['min'] for d in property_types if d['label'] == property_key][0]
    operation_type = [d['min'] for d in operation_types if d['label'] == operation_key][0]
    def fetch_listings(bbox_coords: dict) -> dict:
        payload = {
            "coordenates": f"swLat:{bbox_coords['sw_lat']},swLng:{bbox_coords['sw_lng']},neLat:{bbox_coords['ne_lat']},neLng:{bbox_coords['ne_lng']}",
            "tipoDePropiedad": f"{property_type}",  # Casa
            "tipoDeOperacion": f"{operation_type}",  # Venta
            "sort": "relevance",
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            json_response = response.json()
            return json_response
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}")
            return {}
        
    return fetch_listings


if __name__ == '__main__':
    h = pd.read_csv('hex.csv.gzip', compression='gzip')
    h.loc[:, 'bbox'] = h.loc[:, 'hex_id'].map(get_hexagon_bbox)
    h.loc[:, 'bbox_corners'] = h.loc[:, 'bbox'].map(get_bbox_corners)

    for i, row in h.iterrows():

        for property in property_types:
            property_type = property['label']

            for operation in operation_types:
                operation_type = operation['label']
                filename = f"{row['hex_id']}_{property['min']}_{operation['min']}"

                if f"{filename}.json" not in os.listdir('ecuador_listings'):
                    resp = fetch_listings_by_type(property_type, operation_type)(row['bbox_corners'])
                    resp['hex_id'] = filename
                    resp['bbox_corners'] = row['bbox_corners']

                    sleeping_secs = random.randint(1, 4)
                    time.sleep(sleeping_secs)

                    complete_filename = f'ecuador_listings/{filename}.json'
                    with open(complete_filename, 'w', encoding='utf-8') as f:
                        json.dump(resp, f, indent=4, ensure_ascii=False)
                        print(filename)
                else:
                    print(f'{filename} exists')