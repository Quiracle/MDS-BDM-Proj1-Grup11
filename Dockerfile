# Dockerfile
FROM golang:1.20

WORKDIR /app

COPY . .

RUN go mod download

RUN go build -o proton-api

CMD ["./proton-api"]
