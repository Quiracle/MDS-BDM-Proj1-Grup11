FROM python:3.11-slim

WORKDIR /app

COPY duckdb_loader.py .

RUN pip install duckdb

VOLUME ["/data"]

CMD ["python", "duckdb_loader.py"]