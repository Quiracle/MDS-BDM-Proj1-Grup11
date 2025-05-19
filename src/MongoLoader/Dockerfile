FROM openjdk:11-slim

# Instala Python, pip, y utilidades necesarias
RUN apt-get update && \
    apt-get install -y python3 python3-pip procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Symlink para compatibilidad con PySpark
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app
COPY mongo_loader.py .

# Instala librerías
RUN pip install --no-cache-dir pyspark==3.4.4 delta-spark==2.4.0 pymongo

VOLUME ["/data"]
CMD ["python", "mongo_loader.py"]
