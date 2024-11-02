import requests
import time
import json
import os
import random

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

# Function to fetch listings in a specific bounding box
def fetch_listings(sw_lat, sw_lng, ne_lat, ne_lng):
    payload = {
        "coordenates": f"swLat:{sw_lat},swLng:{sw_lng},neLat:{ne_lat},neLng:{ne_lng}",
        "tipoDePropiedad": "1",  # House (you can adjust as needed)
        "tipoDeOperacion": "1",  # Buy (you can adjust as needed)
        "sort": "relevance",
    }
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
    return {}

# Iterate over the grid and fetch listings
lat = ecuador_sw['lat']
while lat < ecuador_ne['lat']:
    lng = ecuador_sw['lng']
    while lng < ecuador_ne['lng']:
        # Define bounding box for the current grid cell
        sw_lat, sw_lng = lat, lng
        ne_lat, ne_lng = lat + lat_step, lng + lng_step

        # Fetch listings in the current grid cell
        data = fetch_listings(sw_lat, sw_lng, ne_lat, ne_lng)
        
        # Save each response to a unique JSON file
        file_name = f"ecuador_listings/listings_{sw_lat}_{sw_lng}_{ne_lat}_{ne_lng}.json"
        with open(file_name, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Fetched and saved data for area SW({sw_lat}, {sw_lng}) NE({ne_lat}, {ne_lng})")
        
        # Move to the next cell in the longitude direction
        lng += lng_step

        # Respectful pause to avoid overwhelming the server
        secs = random.randint(1, 3)
        time.sleep(2)  # Adjust as needed
    
    # Move to the next row in the latitude direction
    lat += lat_step

print("Data fetching completed.")
