# Altscore Challenge

Para poder generar el ambiente vía el _Dockerfile_:
```console
sudo docker build -t altscore_env .
```

Y, después:
```console
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    # Windows-specific
    export HOST_PATH=$(pwd -W)
else
    # Unix/Linux-specific
    export HOST_PATH=$(pwd)
fi

sudo docker rm -f altscore_challenge || true

sudo docker run -d \
  --name altscore_challenge \
  --memory="60g" \
  --cpus=16 \
  -v "$HOST_PATH"/db:/app/db \
  -v "$HOST_PATH"/notebooks:/app/notebooks \
  -p 8888:8888 \
  altscore_env 
```

Una vez levantado el contenedor, es posible hacer "`ssh`" mediante:
```console
sudo docker exec -it altscore_challenge bash
```

El contenedor genera una instancia de _jupyter notebook_ el cual se puede acceder desde:
```
http://localhost:8888
```

Para ver el código de entrenamiento, consúltese el archivo `notebooks/Model.ipynb`.

#### Importante:
* Para poder construir la imagen de docker es necesario tener en el mismo directorio un archivo `kaggle.json` con las credenciales para autenticarse y descargar los datos de la competencia.
