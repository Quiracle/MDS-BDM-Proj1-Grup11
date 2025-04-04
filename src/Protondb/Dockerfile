FROM python:3.11-slim

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app/src/Protondb

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark delta-spark requests

COPY . .

COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh

CMD ["./wait-for-it.sh", "proton:8080", "--", "python", "-u", "ProtondbAPI.py"]
