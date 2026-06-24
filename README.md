# github.com/writdev-alt/pkg-activitylogmq

Shared [Watermill](https://watermill.io/) broker factory for IlonaPay activity-log messages.

Supports **RabbitMQ**, **Google Cloud Pub/Sub**, and **Kafka** with a single config surface.

## API

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

## Environment

| Variable | Description |
|----------|-------------|
| `MESSAGE_BROKER` | `rabbitmq`, `pubsub`, or `kafka` (auto-detected if unset) |
| `ACTIVITY_LOG_QUEUE` | Topic / queue name (default: `activity-log.create`) |
| `ACTIVITY_LOG_TOPIC` | Alias for queue name |
| `RABBITMQ_URL` | Full AMQP URI (overrides host-based config) |
| `RABBITMQ_HOST` | RabbitMQ host |
| `RABBITMQ_PORT` | Default `5672` |
| `RABBITMQ_USER` | Default `guest` |
| `RABBITMQ_PASSWORD` | Default `guest` |
| `RABBITMQ_VHOST` | Default `/` |
| `PUBSUB_PROJECT_ID` | GCP project ID |
| `KAFKA_BROKERS` | Comma-separated broker addresses |
| `KAFKA_CONSUMER_GROUP` | Default `activity-log-consumer` |

Pub/Sub auth uses standard `GOOGLE_APPLICATION_CREDENTIALS`.

## Consumers in this repo

- **auth-service** — publisher (user login events)
- **release-service** — publisher + consumer (HTTP forward to `activity-log-service`)

## Roadmap

See [PLAN.md](./PLAN.md) for the mTLS + gRPC migration plan (replace HTTP `POST /activity-logs` with an internal gRPC API).

