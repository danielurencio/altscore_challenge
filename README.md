# Altscore Challenge

Para poder generar el ambiente vía el _Dockerfile_:
```console
sudo docker build -t altscore_env .
```

Y, después:
```console
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    # Windows-specific
    export HOST_PATH=$(pwd -W)/db
else
    # Unix/Linux-specific
    export HOST_PATH=$(pwd)/db
fi

sudo docker rm -f altscore_challenge || true

sudo docker run -d \
  --name altscore_challenge1 \
  --memory="4g" \
  --cpus=2 \
  -v "$HOST_PATH":/app/db \
  altscore_env 
```

Una vez levantado el contenedor, es posible hacer "`ssh`" mediante:
```console
sudo docker exec -it altscore_challenge bash
```

Dentro del contenedor debe de existir una estructura como esta:
```
/app/
  ├── altscore_data/
  ├── py_scripts/
  │   ├── script1.py
  │   └── script2.py
  └── shell_scripts/
      ├── script1.sh
      └── script2.sh
```
