FROM python:3.11-slim

WORKDIR /app

COPY influx_loader.py .

RUN pip install influxdb-client pyspark==3.4.4 delta-spark==2.4.0

VOLUME ["/data"]

CMD ["python", "influx_loader.py"]