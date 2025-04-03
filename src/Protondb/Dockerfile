# Dockerfile

FROM python:3.11-slim

WORKDIR /app/src/Protondb

#COPY requirements.txt ./
#RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir requests

COPY . .

CMD ["python", "ProtondbAPI.py"]
