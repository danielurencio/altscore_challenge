# altscore_challenge

Para poder generar el ambiente vía el _Dockerfile_:
```console
sudo docker build -t altscore_env .
```

Y, después:
```console
sudo docker rm -f altscore_challenge || true

sudo docker run -d \
  --name altscore_challenge \
  --memory="4g" \
  --cpus=2 \
  altscore_env 
```

Una vez levantado el contenedor, es posible hacer "`ssh`" mediante:
```console
sudo docker exec -it altscore_challenge bash
```
