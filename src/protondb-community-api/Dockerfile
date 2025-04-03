# Stage 1: Build the Go binary
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Now copy the full source
COPY . .

# Build the Go binary
RUN go build -o proton-app main.go

# Stage 2: Create a slim runtime image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/proton-app .

COPY .env .

# Expose the port the app runs on
EXPOSE 3000

# Run the binary
CMD ["./proton-app"]
