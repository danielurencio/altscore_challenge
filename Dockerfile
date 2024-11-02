FROM python:3.11-slim

WORKDIR /app

# Actualiza pip
RUN pip install --no-cache-dir --upgrade pip

# Copia solo el requirements.txt primero
# Esto es una buena práctica porque aprovecha el cache de Docker
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Instala kaggle
RUN pip install --no-cache-dir kaggle

# Configura el directorio para las credenciales de Kaggle
RUN mkdir -p /root/.kaggle

# Copia el archivo kaggle.json
COPY kaggle.json /root/.kaggle/

# Establece los permisos correctos para kaggle.json
RUN chmod 600 /root/.kaggle/kaggle.json

# Descarga los datos de la competencia
RUN kaggle competitions download -c alt-score-data-science-competition
RUN mkdir -p altscore_data
RUN unzip alt-score-data-science-competition.zip -d altscore_data
# Copia los scripts
COPY ./py_scripts .
COPY ./shell_scripts .

CMD ["tail", "-f", "/dev/null"]
