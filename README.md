# github.com/writdev-alt/pkg-activitylogmq

## Overview

Shared Go library that carries IlonaPay **activity-log (audit) events** from any microservice to `activity-log-service`.

Services enqueue JSON payloads with one call; [Watermill](https://watermill.io/) publishes them to a message broker; a consumer process forwards them over HTTP to `POST /activity-logs`. Supports **RabbitMQ**, **Google Cloud Pub/Sub**, and **Kafka** behind a single config surface.

```
EnqueueActivityLogCreate → activity-log.create → handleActivityLogCreate → POST /activity-logs
```

This package has no HTTP server and no database. Persistence and query APIs live in `admin/activity-log-service`.

| Package | Responsibility |
|---------|----------------|
| `activitylogmq` | Config + broker factory (`NewPublisher` / `NewSubscriber`) |
| `messaging` | `Init`, `EnqueueActivityLogCreate`, consumer handler |
| `clients` | `CreateBody` + HTTP client for `activity-log-service` |

## Goals

| Goal | Notes |
|------|-------|
| One-call publish | `messaging.EnqueueActivityLogCreate` — no broker knowledge required |
| Broker-agnostic | RabbitMQ, Pub/Sub, Kafka via `MESSAGE_BROKER` or auto-detect |
| Safe degradation | Missing broker config warns and disables the queue; host service keeps running |
| Non-blocking audits | Publish is async; business requests are not tied to log-service latency |
| Shared payload shape | Single `clients.CreateBody` across all IlonaPay services |

Non-goals: reading or querying logs, owning the `activity_log` table, exposing routes, or replacing the broker with gRPC streaming.

Roadmap (gRPC + mTLS delivery): [PLAN.md](./PLAN.md). Requirements and known risks: [PRD.md](./PRD.md).

## Installation

Requires **Go 1.26+**.

```bash
go get github.com/writdev-alt/pkg-activitylogmq@latest
```

In an IlonaPay service `go.mod`:

```go
require github.com/writdev-alt/pkg-activitylogmq vX.Y.Z
```

Wire at process startup:

```go
import (
    "context"

    "github.com/writdev-alt/pkg-activitylogmq/clients"
    "github.com/writdev-alt/pkg-activitylogmq/messaging"
)

func main() {
    ctx := context.Background()
    if err := messaging.Init(ctx); err != nil {
        // fatal only if broker wiring itself failed; missing config is not an error
        log.Fatal(err)
    }
    defer messaging.Close()

    // elsewhere in a service method:
    _ = messaging.EnqueueActivityLogCreate(ctx, clients.CreateBody{
        LogName:     "auth",
        Description: "User logged in",
        SubjectType: "User",
        Event:       "login",
        SubjectID:   42,
        CauserType:  "User",
        CauserID:    42,
    })
}
```

Low-level broker access (without the messaging package):

```go
import activitylogmq "github.com/writdev-alt/pkg-activitylogmq"

cfg := activitylogmq.LoadConfig()
if cfg.Enabled() {
    pub, _ := activitylogmq.NewPublisher(cfg, logger)
    sub, _ := activitylogmq.NewSubscriber(cfg, logger)
}
```

| Function | Description |
|----------|-------------|
| `LoadConfig()` | Read broker + topic from environment |
| `Config.Enabled()` | `true` when broker credentials are present |
| `NewPublisher(cfg, logger)` | Watermill `message.Publisher` |
| `NewSubscriber(cfg, logger)` | Watermill `message.Subscriber` |
| `messaging.Init(ctx)` | Publisher + consumer + router (`sync.Once`) |
| `messaging.EnqueueActivityLogCreate(ctx, body)` | Publish a create payload |
| `messaging.Close()` | Shut down router, subscriber, publisher |

Verify locally:

```bash
go build ./... && go vet ./... && go test ./...
```

## Configuration

All settings are environment variables. No broker configured → queue disabled (warn only).

### Broker selection

| Variable | Description |
|----------|-------------|
| `MESSAGE_BROKER` | `rabbitmq` (`amqp`/`rabbit`), `pubsub` (`google`/`gcp`), or `kafka`. Auto-detected if unset. |

Auto-detect order when `MESSAGE_BROKER` is unset: RabbitMQ URI present → `PUBSUB_PROJECT_ID` → `KAFKA_BROKERS`.

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

Read once at package init into `clients.ActivityLog`. Consumers without this URL ACK and drop messages they receive.

> `messaging.Init` starts a publisher **and** a consumer. Every service that calls `Init` binds the same durable queue `activity-log.create` and they compete for deliveries. See [PRD.md](./PRD.md) §6.

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

## Consumers in the monorepo

Services importing `messaging` and calling `Init`:

`auth-service`, `admin/admin-service`, `admin/admin-user-service`, `admin/admin-bank-service`, `admin/admin-merchant-service`, `admin/admin-wallet-service`, `admin/setting-service`, `admin/notification-service`, `admin/transaction-export-service`, `admin/release-service`, `user/user-service`, `user/merchant-service`, `user/wallet-service`.

## Docs

- [PRD.md](./PRD.md) — requirements, config surface, known risks
- [PLAN.md](./PLAN.md) — mTLS + gRPC migration plan
