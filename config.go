package activitylogmq

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const defaultTopic = "activity-log.create"

// Broker identifies the message transport selected by [LoadConfig].
type Broker string

const (
	// BrokerRabbitMQ selects RabbitMQ / AMQP. Aliases for MESSAGE_BROKER:
	// "rabbitmq", "amqp", "rabbit".
	BrokerRabbitMQ Broker = "rabbitmq"

	// BrokerAmazonMQ selects Amazon MQ for RabbitMQ over AMQPS (TLS).
	// It reuses the Watermill AMQP adapter with an amqps:// URI.
	// Aliases for MESSAGE_BROKER: "amazonmq", "amazon_mq", "amazon-mq", "amq".
	BrokerAmazonMQ Broker = "amazonmq"

	// BrokerPubSub selects Google Cloud Pub/Sub. Aliases for MESSAGE_BROKER:
	// "pubsub", "google", "gcp", "google_pubsub".
	BrokerPubSub Broker = "pubsub"

	// BrokerKafka selects Apache Kafka.
	BrokerKafka Broker = "kafka"
)

// Config holds broker connection settings loaded from the environment by
// [LoadConfig]. Pass an Enabled config to [NewPublisher] or [NewSubscriber].
type Config struct {
	// Broker is the selected transport. Empty when no broker could be detected.
	Broker Broker

	// Topic is the queue or topic name used for publish and subscribe.
	// Defaults to "activity-log.create" when no topic env var is set.
	Topic string

	// RabbitMQURI is the AMQP/AMQPS connection URI when Broker is
	// BrokerRabbitMQ or BrokerAmazonMQ.
	RabbitMQURI string

	// PubSubProjectID is the GCP project ID when Broker is BrokerPubSub.
	PubSubProjectID string

	// KafkaBrokers is the list of Kafka bootstrap addresses when Broker is
	// BrokerKafka.
	KafkaBrokers []string

	// KafkaConsumerGroup is the consumer group for Kafka subscribers.
	// Defaults to "activity-log-consumer".
	KafkaConsumerGroup string
}

// LoadConfig reads MESSAGE_BROKER (or auto-detects the broker) and
// broker-specific environment variables into a [Config].
//
// Topic resolution order: ACTIVITY_LOG_QUEUE, ACTIVITY_LOG_TOPIC,
// PUBSUB_TOPIC, KAFKA_TOPIC, then "activity-log.create".
//
// Broker auto-detect order when MESSAGE_BROKER is unset: RabbitMQ URI present,
// then PUBSUB_PROJECT_ID, then KAFKA_BROKERS, then Amazon MQ URI/host.
//
// LoadConfig never returns an error; call [Config.Enabled] to determine whether
// publishing and subscribing can proceed.
func LoadConfig() Config {
	cfg := Config{Topic: topicFromEnv()}
	cfg.Broker = brokerFromEnv(cfg)
	switch cfg.Broker {
	case BrokerRabbitMQ:
		cfg.RabbitMQURI = rabbitMQURI()
	case BrokerAmazonMQ:
		cfg.RabbitMQURI = amazonMQURI()
	case BrokerPubSub:
		cfg.PubSubProjectID = strings.TrimSpace(os.Getenv("PUBSUB_PROJECT_ID"))
	case BrokerKafka:
		cfg.KafkaBrokers = splitCSV(os.Getenv("KAFKA_BROKERS"))
		cfg.KafkaConsumerGroup = envOr("KAFKA_CONSUMER_GROUP", "activity-log-consumer")
	}
	return cfg
}

// Enabled reports whether Broker is set and the corresponding connection
// settings are present (AMQP URI, Pub/Sub project ID, or Kafka brokers).
// A disabled config must not be passed to [NewPublisher] or [NewSubscriber].
func (c Config) Enabled() bool {
	switch c.Broker {
	case BrokerRabbitMQ, BrokerAmazonMQ:
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
	if amazonMQURI() != "" {
		return BrokerAmazonMQ
	}
	return ""
}

func normalizeBroker(raw string) Broker {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "rabbitmq", "amqp", "rabbit":
		return BrokerRabbitMQ
	case "amazonmq", "amazon_mq", "amazon-mq", "amq":
		return BrokerAmazonMQ
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
	return buildAMQPURI("amqp", user, pass, host, port, vhost)
}

// amazonMQURI builds an AMQPS (TLS) connection URI for Amazon MQ for RabbitMQ.
//
// Precedence: AMAZONMQ_URL (or AMAZON_MQ_URL), else host-based vars.
// TLS is on by default (amqps://, port 5671). Set AMAZONMQ_TLS=false to use
// plain amqp:// (local/dev only).
func amazonMQURI() string {
	if u := firstNonEmptyEnv("AMAZONMQ_URL", "AMAZON_MQ_URL"); u != "" {
		return u
	}
	host := firstNonEmptyEnv("AMAZONMQ_HOST", "AMAZON_MQ_HOST")
	if host == "" {
		return ""
	}
	user := firstNonEmptyEnv("AMAZONMQ_USER", "AMAZON_MQ_USER")
	if user == "" {
		user = "guest"
	}
	pass := firstNonEmptyEnv("AMAZONMQ_PASSWORD", "AMAZON_MQ_PASSWORD")
	if pass == "" {
		pass = "guest"
	}
	scheme := "amqps"
	defaultPort := "5671"
	if !envBoolDefaultTrue(firstNonEmptyEnv("AMAZONMQ_TLS", "AMAZON_MQ_TLS")) {
		scheme = "amqp"
		defaultPort = "5672"
	}
	port := firstNonEmptyEnv("AMAZONMQ_PORT", "AMAZON_MQ_PORT")
	if port == "" {
		port = defaultPort
	}
	vhost := firstNonEmptyEnv("AMAZONMQ_VHOST", "AMAZON_MQ_VHOST")
	return buildAMQPURI(scheme, user, pass, host, port, vhost)
}

func buildAMQPURI(scheme, user, pass, host, port, vhost string) string {
	if vhost == "" || vhost == "/" {
		return fmt.Sprintf("%s://%s:%s@%s:%s/",
			scheme, url.QueryEscape(user), url.QueryEscape(pass), host, port)
	}
	return fmt.Sprintf("%s://%s:%s@%s:%s/%s",
		scheme, url.QueryEscape(user), url.QueryEscape(pass), host, port,
		url.PathEscape(strings.TrimPrefix(vhost, "/")))
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

// envBoolDefaultTrue parses an env value as a boolean. Empty or unrecognized
// values default to true (safe for Amazon MQ TLS).
func envBoolDefaultTrue(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
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
