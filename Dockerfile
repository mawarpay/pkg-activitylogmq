# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm

WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Default: run the full unit-test suite. Compose overrides env for RabbitMQ.
CMD ["go", "test", "./...", "-count=1"]
