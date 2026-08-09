# pkg-activitylogmq

[![Go Reference](https://pkg.go.dev/badge/github.com/mawarpay/pkg-activitylogmq.svg)](https://pkg.go.dev/github.com/mawarpay/pkg-activitylogmq)

Shared Go library that carries IlonaPay **activity-log (audit) events** from any microservice to `activity-log-service`.

Services enqueue a JSON payload with one call; [Watermill](https://watermill.io/) publishes it to a message broker; a consumer process forwards it over HTTP to `POST /activity-logs`.

```text
EnqueueActivityLogCreate → activity-log.create → handleActivityLogCreate → POST /activity-logs
```

This module has no HTTP server and no database. Persistence and query APIs live in `activity-log-service`.

## Overview

pkg-activitylogmq provides a small, dependency-light producer and consumer for application audit events. Producers call a single convenience function to enqueue an audit event; the library handles broker selection and publishing using Watermill. A consumer (also provided) reads messages and forwards them to the central `activity-log-service` over HTTP.

The library is designed to be embedded by any microservice that needs to emit activity logs without coupling that service to the log storage or HTTP latency.

## Goals

- Provide a single-call, non-blocking API for services to publish audit events.
- Be broker-agnostic: support RabbitMQ / Amazon MQ (AMQPS), Google Cloud Pub/Sub, and Kafka.
- Fail open for missing broker configuration so services keep running when audit delivery is unavailable.
- Keep the library small and focused — no servers, no DBs, no schema ownership.
- Ensure producers and consumers share a common payload type so the log service and producers remain aligned.

## Tech stack

- Language: Go (1.26+)
- Messaging: Watermill (publisher/subscriber abstraction)
- Brokers supported: RabbitMQ, Amazon MQ (RabbitMQ engine / AMQPS), Google Cloud Pub/Sub, Kafka
- Testing: go test, httptest for HTTP fakes, integration tests with RabbitMQ and Kafka (CI)
- Build / local infra: Docker Compose (for local RabbitMQ/Kafka during integration)

## Features

- One-call publish via `messaging.EnqueueActivityLogCreate`
- Broker-agnostic: RabbitMQ, Amazon MQ (RabbitMQ/AMQPS), Google Cloud Pub/Sub, and Kafka
- Safe degradation when broker env is missing (host service keeps running)
- Non-blocking audits — publish does not wait on `activity-log-service`
- Shared `clients.CreateBody` payload across services

## Requirements

- Go **1.26+**

## Installation

```bash
go get github.com/mawarpay/pkg-activitylogmq@latest
```

```go
import (
    "github.com/mawarpay/pkg-activitylogmq/clients"
    "github.com/mawarpay/pkg-activitylogmq/messaging"
)
```

## Quick start

```go
package main

import (
    "context"
    "log"

    "github.com/mawarpay/pkg-activitylogmq/clients"
    "github.com/mawarpay/pkg-activitylogmq/messaging"
)

func main() {
    ctx := context.Background()
    if err := messaging.Init(ctx); err != nil {
        log.Fatal(err) // only broker wiring failures; missing config returns nil
    }
    defer messaging.Close()

    err := messaging.EnqueueActivityLogCreate(ctx, clients.CreateBody{
        LogName:     "auth",
        Description: "User logged in",
        SubjectType: "User",
        Event:       "login",
        SubjectID:   42,
        CauserType:  "User",
        CauserID:    42,
    })
    if err != nil {
        // typically log and continue — do not fail the business request
        log.Printf("activity log enqueue: %v", err)
    }
}
```

Low-level broker factory (without the messaging package):

```go
import activitylogmq "github.com/mawarpay/pkg-activitylogmq"

cfg := activitylogmq.LoadConfig()
if cfg.Enabled() {
    pub, err := activitylogmq.NewPublisher(cfg, nil)
    // ...
    sub, err := activitylogmq.NewSubscriber(cfg, nil)
    // ...
}
```

## Package layout

```text
.
├── activitylogmq          # config + NewPublisher / NewSubscriber
├── clients/               # CreateBody + HTTP client for activity-log-service
├── messaging/             # Init, EnqueueActivityLogCreate, consumer handler
├── LICENSE
├── README.md
├── PRD.md                 # product requirements and known risks
└── PLAN.md                # gRPC + mTLS migration roadmap
```

| Package | Import path | Role |
|---------|-------------|------|
| `activitylogmq` | `github.com/mawarpay/pkg-activitylogmq` | Config and Watermill broker factory |
| `clients` | `.../clients` | `CreateBody` and HTTP `Create` |
| `messaging` | `.../messaging` | Process-wide Init / enqueue / consumer |

API reference: [pkg.go.dev/github.com/mawarpay/pkg-activitylogmq](https://pkg.go.dev/github.com/mawarpay/pkg-activitylogmq).

## Configuration

All settings are environment variables. No broker configured → queue disabled (warn only).

### Broker selection

| Variable | Description |
|----------|-------------|
| `MESSAGE_BROKER` | `rabbitmq` (`amqp`/`rabbit`), `amazonmq` (`amazon_mq`/`amq`), `pubsub` (`google`/`gcp`), or `kafka`. Auto-detected if unset. |

Auto-detect order when `MESSAGE_BROKER` is unset: RabbitMQ URI present → `PUBSUB_PROJECT_ID` → `KAFKA_BROKERS` → Amazon MQ URI/host.

### Topic / queue

| Variable | Description |
|----------|-------------|
| `ACTIVITY_LOG_QUEUE` | Topic / queue name (preferred) |
| `ACTIVITY_LOG_TOPIC` | Alias for queue name |
| `PUBSUB_TOPIC` | Pub/Sub fallback |
| `KAFKA_TOPIC` | Kafka fallback |

Default when none are set: `activity-log.create`.

### RabbitMQ

| Variable | Description |
|----------|-------------|
| `RABBITMQ_URL` | Full AMQP URI (overrides host-based config) |
| `RABBITMQ_HOST` | Host (required for host-based URI) |
| `RABBITMQ_PORT` | Default `5672` |
| `RABBITMQ_USER` | Default `guest` |
| `RABBITMQ_PASSWORD` | Default `guest` |
| `RABBITMQ_VHOST` | Default `/` |

### Amazon MQ (RabbitMQ engine)

Uses the same Watermill AMQP adapter over **AMQPS** (TLS). ActiveMQ is not supported.

| Variable | Description |
|----------|-------------|
| `AMAZONMQ_URL` | Full `amqps://` URI (overrides host-based config). Alias: `AMAZON_MQ_URL`. |
| `AMAZONMQ_HOST` | Broker endpoint hostname. Alias: `AMAZON_MQ_HOST`. |
| `AMAZONMQ_PORT` | Default `5671` (TLS) or `5672` when TLS is off. Alias: `AMAZON_MQ_PORT`. |
| `AMAZONMQ_USER` | Broker username. Alias: `AMAZON_MQ_USER`. |
| `AMAZONMQ_PASSWORD` | Broker password. Alias: `AMAZON_MQ_PASSWORD`. |
| `AMAZONMQ_VHOST` | Default `/`. Alias: `AMAZON_MQ_VHOST`. |
| `AMAZONMQ_TLS` | Default `true` (`amqps`). Set `false` for plain `amqp` (local/dev only). Alias: `AMAZON_MQ_TLS`. |

```yaml
environment:
  MESSAGE_BROKER: amazonmq
  AMAZONMQ_HOST: b-xxxxx.mq.us-east-1.amazonaws.com
  AMAZONMQ_USER: mquser
  AMAZONMQ_PASSWORD: secret
  ACTIVITY_LOG_QUEUE: activity-log.create
```

### Google Cloud Pub/Sub

| Variable | Description |
|----------|-------------|
| `PUBSUB_PROJECT_ID` | GCP project ID |

Auth uses standard `GOOGLE_APPLICATION_CREDENTIALS`.

### Kafka

| Variable | Description |
|----------|-------------|
| `KAFKA_BROKERS` | Comma-separated broker addresses |
| `KAFKA_CONSUMER_GROUP` | Default `activity-log-consumer` |

### HTTP forward (consumer)

| Variable | Description |
|----------|-------------|
| `ACTIVITY_LOG_SERVICE_URL` | Base URL for `POST /activity-logs` (e.g. `http://activity-log-service:8080`) |

Read once at package init into `clients.ActivityLog`. Consumers without this URL acknowledge and drop messages they receive.

> `messaging.Init` starts a publisher **and** a consumer. Every service that calls `Init` binds the same durable queue `activity-log.create` and they compete for deliveries. See [PRD.md](./PRD.md) and [PLAN.md](./PLAN.md) for migration and design notes.

### Typical Compose snippet

```yaml
environment:
  MESSAGE_BROKER: rabbitmq
  RABBITMQ_HOST: rabbitmq
  RABBITMQ_USER: guest
  RABBITMQ_PASSWORD: guest
  ACTIVITY_LOG_QUEUE: activity-log.create
  ACTIVITY_LOG_SERVICE_URL: http://activity-log-service:8080
```

## Design philosophy

- Prefer async enqueue over synchronous HTTP on the request path.
- Fail open for configuration gaps; fail closed only on explicit wiring errors from Init.
- Keep this module a library — no routes, servers, or schema ownership.
- Share one payload type (`CreateBody`) so producers and the log service stay aligned.

## Development

```bash
make help          # list targets
make check         # fmt-check + vet + build + test (race/cover) — matches CI unit steps
make test          # go test ./... -race -cover
make test-integration  # RabbitMQ + Kafka publish/subscribe (needs INTEGRATION=1)

make docker-up     # start RabbitMQ (:5672, UI :15672 guest/guest)
make docker-test   # build image and run go test ./... against Compose RabbitMQ
make docker-down   # stop Compose services
```

Unit tests use fakes and `httptest`; a live broker is not required for `make test`.
CI starts RabbitMQ and Kafka service containers and runs `make test-integration`.

## Docs

- [PRD.md](./PRD.md) — requirements, config surface, known risks
- [PLAN.md](./PLAN.md) — mTLS + gRPC migration plan

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup, scope, API stability, and PR expectations.

## License

[MIT](./LICENSE) © 2026 mawarpay
