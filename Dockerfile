FROM python:3.11-slim

# Evitar interacciones durante la instalación
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app
RUN mkdir -p /app/db

# Instalar dependencias del sistema incluyendo unzip
RUN apt-get update && apt-get install -y \
    unzip \
    curl \
    vim \
    sqlite3 \
    libsqlite3-mod-spatialite \
    && rm -rf /var/lib/apt/lists/*

# Crear .sqliterc con configuraciones por defecto
RUN echo ".mode box\n\
.headers on\n\
.timer on" > /root/.sqliterc

# Actualiza pip
RUN pip install --no-cache-dir --upgrade pip

# Copia solo el requirements.txt primero
# Esto es una buena práctica porque aprovecha el cache de Docker
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Configura el directorio para las credenciales de Kaggle
RUN mkdir -p /root/.kaggle

# Copia el archivo kaggle.json
COPY kaggle.json /root/.kaggle/

# Establece los permisos correctos para kaggle.json
RUN chmod 600 /root/.kaggle/kaggle.json

# Descarga los datos de la competencia
RUN kaggle competitions download -c alt-score-data-science-competition \
    && mkdir -p altscore_data \
    && unzip alt-score-data-science-competition.zip -d altscore_data \
    && rm alt-score-data-science-competition.zip  # Limpiar archivo zip

# Crea y copia a directorios específicos
COPY ./py_scripts ./py_scripts/
COPY ./shell_scripts ./shell_scripts/

# Mantén el ambiente corriendo
CMD ["tail", "-f", "/dev/null"]
