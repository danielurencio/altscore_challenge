FROM python:3.11-slim

WORKDIR /app

# Actualiza pip
RUN pip install --no-cache-dir --upgrade pip

# Copia solo el requirements.txt primero
# Esto es una buena práctica porque aprovecha el cache de Docker
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de tu código
COPY ./py_script .
COPY ./shell_scripts .

CMD ["tail", "-f", "/dev/null"]

