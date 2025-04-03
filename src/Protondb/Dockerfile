# Dockerfile

FROM python:3.11-slim

WORKDIR /app/src/Protondb

#COPY requirements.txt ./
#RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir requests

COPY . .


COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh

CMD ["./wait-for-it.sh", "proton:8080", "--", "python", "-u", "ProtondbAPI.py"]
