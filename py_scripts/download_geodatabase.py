import os
from urllib.request import urlretrieve

url = "https://www.ecuadorencifras.gob.ec/documentos/web-inec/Geografia_Estadistica/Documentos/GEODATABASE_NACIONAL_2021.zip"
file_name = url.split('/')[-1]

if file_name not in os.listdir():
    urlretrieve(url, file_name)

