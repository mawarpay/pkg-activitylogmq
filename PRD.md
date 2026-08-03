# PRD.md — pkg-activitylogmq

Product requirements for `github.com/writdev-alt/pkg-activitylogmq`, the shared Go library that carries IlonaPay **activity-log (audit) events** from any microservice to `activity-log-service`.

Scope: this library only. Persistence, query APIs, and retention live in `admin/activity-log-service`.

Related docs: [README.md](./README.md) (usage) · [PLAN.md](./PLAN.md) (mTLS + gRPC roadmap).

---

## 1. Problem

Every IlonaPay service needs to record who did what (logins, bank edits, wallet changes, releases). Writing those rows synchronously over HTTP from each service would couple request latency to `activity-log-service` availability, and would duplicate broker/client wiring in ~12 services.

This package exists so a service can emit an audit event with **one function call**, without knowing the broker, the transport, or the log service's address.

## 2. Goals

| # | Goal | Status |
|---|------|--------|
| G1 | One-call publish API for audit events | Met — `messaging.EnqueueActivityLogCreate` |
| G2 | Broker-agnostic: RabbitMQ, Pub/Sub, Kafka behind one config surface | Met — `broker.go` |
| G3 | Zero-config degradation: no broker configured must not crash the host service | Met — `Init` warns and returns `nil` |
| G4 | Audit writes never block or fail the business request | Met — publish is async, errors returned but callers log-and-continue |
| G5 | Single source of the audit payload shape across all services | Met — `clients.CreateBody` |
| G6 | Delivery is at-least-once and survives log-service downtime | **Not met** — see R1/R2 |
| G7 | Transport between consumer and log service is authenticated | **Not met** — plain HTTP, see [PLAN.md](./PLAN.md) |

### Non-goals

- Reading, querying, or aggregating activity logs
- Owning the `activity_log` table or its migrations
- Exposing any HTTP route (this library has no server)
- Replacing the broker with gRPC streaming for all audit events

## 3. Users

| User | Need |
|------|------|
| **Service developer** (Go) | Emit an audit event from a service method without broker knowledge |
| **Platform/ops** | Configure broker + log-service URL per environment via env vars only |
| **Compliance/admin** | Trust that admin and user actions land in `activity_log` |

## 4. Current architecture (verified against code)

```
Producer service                  RabbitMQ                      Consumer process             activity-log-service
────────────────                  ────────                      ────────────────             ────────────────────
EnqueueActivityLogCreate  ──────► activity-log.create  ───────► handleActivityLogCreate ───► POST /activity-logs
(JSON clients.CreateBody)         (durable queue)               (Watermill router)           (plain HTTP, no auth)
```

`messaging.Init` starts **both** a publisher and a subscriber in the same process, so most services are simultaneously producer and consumer of the shared queue.

### Package layout

| Path | Responsibility |
|------|----------------|
| `config.go` | `LoadConfig()` — read `MESSAGE_BROKER` or auto-detect; build RabbitMQ URI; `Config.Enabled()` |
| `broker.go` | `NewPublisher` / `NewSubscriber` — Watermill adapters for AMQP, Pub/Sub, Kafka |
| `messaging/activity_log.go` | `Init`, `Close`, `EnqueueActivityLogCreate`, `handleActivityLogCreate` |
| `clients/activity_log_client.go` | `ActivityLogClient.Create` — `POST {ACTIVITY_LOG_SERVICE_URL}/activity-logs` |

### Public API contract

```go
cfg := activitylogmq.LoadConfig()          // env → Config
cfg.Enabled()                              // broker + required settings present

activitylogmq.NewPublisher(cfg, logger)    // message.Publisher
activitylogmq.NewSubscriber(cfg, logger)   // message.Subscriber

messaging.Init(ctx)                        // publisher + consumer + router; idempotent (sync.Once)
messaging.EnqueueActivityLogCreate(ctx, clients.CreateBody{...})
messaging.Close()
```

`CreateBody` fields: `logName`, `description`, `subjectType`, `event`, `subjectId`, `causerType`, `causerId`, `properties`.

### Behavioral requirements

| ID | Requirement | Where enforced |
|----|-------------|----------------|
| B1 | `Init` runs at most once per process | `sync.Once` in `Init` |
| B2 | No broker configured → warn, return `nil`, publisher stays `nil` | `initMessaging` |
| B3 | Publish with no publisher → return `ErrNotConfigured`, do not panic | `EnqueueActivityLogCreate` |
| B4 | Malformed message payload → log and **ACK** (no infinite redelivery) | `handleActivityLogCreate` |
| B5 | `ACTIVITY_LOG_SERVICE_URL` unset in consumer → log and ACK | `handleActivityLogCreate` |
| B6 | Non-2xx from log service → return error so Watermill **NACKs** and retries | `handleActivityLogCreate` |
| B7 | Partial `Init` failure closes already-opened publisher/subscriber | `initMessaging` |
| B8 | RabbitMQ credentials are URL-escaped when building the AMQP URI | `rabbitMQURI` |

### Configuration surface

| Variable | Default | Notes |
|----------|---------|-------|
| `MESSAGE_BROKER` | auto-detect | `rabbitmq`\|`amqp`\|`rabbit`, `pubsub`\|`google`\|`gcp`, `kafka` |
| `ACTIVITY_LOG_QUEUE` | `activity-log.create` | Then `ACTIVITY_LOG_TOPIC`, `PUBSUB_TOPIC`, `KAFKA_TOPIC` |
| `RABBITMQ_URL` | — | Wins over host-based vars |
| `RABBITMQ_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_VHOST` | — / `5672` / `guest` / `guest` / `/` | URI built only when host is set |
| `PUBSUB_PROJECT_ID` | — | Auth via `GOOGLE_APPLICATION_CREDENTIALS` |
| `KAFKA_BROKERS` | — | Comma-separated |
| `KAFKA_CONSUMER_GROUP` | `activity-log-consumer` | |
| `ACTIVITY_LOG_SERVICE_URL` | — | Consumer-side HTTP target; **read once at package init** |

Auto-detection order when `MESSAGE_BROKER` is unset: RabbitMQ URI → `PUBSUB_PROJECT_ID` → `KAFKA_BROKERS`.

## 5. Consumers in the monorepo

Services importing `pkg-activitylogmq/messaging` and calling `Init` (each therefore also runs a consumer):

`auth-service`, `admin/admin-service`, `admin/admin-user-service`, `admin/admin-bank-service`, `admin/admin-merchant-service`, `admin/admin-wallet-service`, `admin/setting-service`, `admin/notification-service`, `admin/transaction-export-service`, `admin/release-service`, `user/user-service`, `user/merchant-service`, `user/wallet-service`.

Compose services that set `ACTIVITY_LOG_SERVICE_URL` (i.e. can actually forward to the log service): `admin-service`, `admin-user-service`, `admin-bank-service`, `admin-merchant-service`, `admin-wallet-service`, `admin-setting-service`, `admin-notification-service`, `release-service`, `notification-service`.

> `README.md` currently lists only auth-service and release-service as consumers. That is stale.

## 6. Known risks

### R1 — Competing consumers can silently drop events (highest severity)

Watermill's `amqp.NewDurableQueueConfig` uses `GenerateQueueNameTopicName`, so the queue name equals the topic. Every service that calls `Init` binds to the **same durable queue** `activity-log.create` and they compete for messages.

Services that call `Init` but have **no** `ACTIVITY_LOG_SERVICE_URL` — including `auth-service`, `user-service`, `merchant-service`, `wallet-service`, and `transaction-export-service` — will, when they win a delivery, hit B5: log a warning and **ACK the message**. The audit event is lost, and which events are lost is nondeterministic (round-robin across whichever consumers are up).

Mitigations, in order of preference:

1. Split publish-only from consume: add an opt-out (e.g. `ACTIVITY_LOG_CONSUMER_ENABLED`) so only a dedicated worker consumes.
2. Set `ACTIVITY_LOG_SERVICE_URL` on every service that calls `Init`.
3. NACK instead of ACK when the client is unconfigured, so another consumer can take the message.

### R2 — Other reliability gaps

| ID | Risk | Impact |
|----|------|--------|
| R2 | No dead-letter queue; B6 NACKs requeue forever while the log service is down | Poison messages loop |
| R3 | No idempotency key; redelivery duplicates rows | Duplicate audit entries |
| R4 | `clients.ActivityLog` is a package-level `var` read from env at init | Env set after init is ignored; hard to inject in tests |
| R5 | HTTP hop is unauthenticated (network placement only) | Any pod on the network can write audit rows |
| R6 | 5s timeout, no retry/backoff in the client | Transient blips become NACK churn |
| R7 | No metrics or tracing on publish/consume | Silent loss is invisible |

R5 is addressed by the gRPC + mTLS migration in [PLAN.md](./PLAN.md).

## 7. Success metrics

| Metric | Target |
|--------|--------|
| Audit events published vs rows persisted | ≥ 99.9% (currently unmeasured — R7) |
| Added latency on the producer's request path | < 5 ms (publish is fire-and-forget) |
| Service startup failures caused by this package | 0 (G3) |
| Services needing custom broker wiring | 0 |

## 8. Roadmap

Near-term hardening, in priority order:

1. **R1** — stop unconfigured consumers from dropping events
2. **R2/R3** — dead-letter queue and idempotency key
3. **R7** — publish/consume metrics and OTEL spans
4. **R4** — constructor-injected client instead of a package-level `var`
5. **R5** — gRPC + mTLS transport ([PLAN.md](./PLAN.md) phases 1–6)

## 9. Quality bar

- Go 1.26; the only required deps are Watermill adapters plus `github.com/turahe/pkg`
- `go build ./... && go vet ./... && go test ./...` must pass before any commit
- Unit tests cover config parsing, broker selection, publish paths, and handler branches with fakes and `httptest` — no live broker required
- This library must remain server-free and route-free
