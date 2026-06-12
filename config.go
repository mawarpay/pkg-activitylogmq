package activitylogmq

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const defaultTopic = "activity-log.create"

// Broker identifies the message transport.
type Broker string

const (
	BrokerRabbitMQ Broker = "rabbitmq"
	BrokerPubSub   Broker = "pubsub"
	BrokerKafka    Broker = "kafka"
)

// Config holds broker connection settings loaded from the environment.
type Config struct {
	Broker             Broker
	Topic              string
	RabbitMQURI        string
	PubSubProjectID    string
	KafkaBrokers       []string
	KafkaConsumerGroup string
}

// LoadConfig reads MESSAGE_BROKER (or auto-detect) and broker-specific env vars.
func LoadConfig() Config {
	cfg := Config{Topic: topicFromEnv()}
	cfg.Broker = brokerFromEnv(cfg)
	switch cfg.Broker {
	case BrokerRabbitMQ:
		cfg.RabbitMQURI = rabbitMQURI()
	case BrokerPubSub:
		cfg.PubSubProjectID = strings.TrimSpace(os.Getenv("PUBSUB_PROJECT_ID"))
	case BrokerKafka:
		cfg.KafkaBrokers = splitCSV(os.Getenv("KAFKA_BROKERS"))
		cfg.KafkaConsumerGroup = envOr("KAFKA_CONSUMER_GROUP", "activity-log-consumer")
	}
	return cfg
}

// Enabled reports whether a broker and required settings are present.
func (c Config) Enabled() bool {
	switch c.Broker {
	case BrokerRabbitMQ:
		return c.RabbitMQURI != ""
	case BrokerPubSub:
		return c.PubSubProjectID != ""
	case BrokerKafka:
		return len(c.KafkaBrokers) > 0
	default:
		return false
	}
}

func topicFromEnv() string {
	if t := strings.TrimSpace(os.Getenv("ACTIVITY_LOG_QUEUE")); t != "" {
		return t
	}
	if t := strings.TrimSpace(os.Getenv("ACTIVITY_LOG_TOPIC")); t != "" {
		return t
	}
	if t := strings.TrimSpace(os.Getenv("PUBSUB_TOPIC")); t != "" {
		return t
	}
	if t := strings.TrimSpace(os.Getenv("KAFKA_TOPIC")); t != "" {
		return t
	}
	return defaultTopic
}

func brokerFromEnv(cfg Config) Broker {
	if b := normalizeBroker(os.Getenv("MESSAGE_BROKER")); b != "" {
		return b
	}
	if rabbitMQURI() != "" {
		return BrokerRabbitMQ
	}
	if strings.TrimSpace(os.Getenv("PUBSUB_PROJECT_ID")) != "" {
		return BrokerPubSub
	}
	if strings.TrimSpace(os.Getenv("KAFKA_BROKERS")) != "" {
		return BrokerKafka
	}
	return ""
}

func normalizeBroker(raw string) Broker {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "rabbitmq", "amqp", "rabbit":
		return BrokerRabbitMQ
	case "pubsub", "google", "gcp", "google_pubsub":
		return BrokerPubSub
	case "kafka":
		return BrokerKafka
	default:
		return ""
	}
}

func rabbitMQURI() string {
	if u := strings.TrimSpace(os.Getenv("RABBITMQ_URL")); u != "" {
		return u
	}
	host := strings.TrimSpace(os.Getenv("RABBITMQ_HOST"))
	if host == "" {
		return ""
	}
	user := envOr("RABBITMQ_USER", "guest")
	pass := envOr("RABBITMQ_PASSWORD", "guest")
	port := envOr("RABBITMQ_PORT", "5672")
	vhost := strings.TrimSpace(os.Getenv("RABBITMQ_VHOST"))
	if vhost == "" || vhost == "/" {
		return fmt.Sprintf("amqp://%s:%s@%s:%s/",
			url.QueryEscape(user), url.QueryEscape(pass), host, port)
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%s/%s",
		url.QueryEscape(user), url.QueryEscape(pass), host, port, url.PathEscape(strings.TrimPrefix(vhost, "/")))
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
