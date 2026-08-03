package activitylogmq_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	activitylogmq "github.com/mawarpay/pkg-activitylogmq"
)

// integrationEnabled is set in CI (and optionally locally) when RabbitMQ/Kafka
// service containers are available.
func integrationEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("set INTEGRATION=1 to run broker integration tests")
	}
}

func TestIntegration_RabbitMQ_PublishSubscribe(t *testing.T) {
	integrationEnabled(t)

	cfg := activitylogmq.Config{
		Broker:      activitylogmq.BrokerRabbitMQ,
		Topic:       "activity-log.create.integration.rabbit",
		RabbitMQURI: rabbitMQURIFromEnv(),
	}
	if !cfg.Enabled() {
		t.Fatal("RabbitMQ config is not enabled; set RABBITMQ_HOST or RABBITMQ_URL")
	}

	pubSubRoundTrip(t, cfg)
}

func TestIntegration_Kafka_PublishSubscribe(t *testing.T) {
	integrationEnabled(t)

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	group := os.Getenv("KAFKA_CONSUMER_GROUP")
	if group == "" {
		group = "activity-log-integration"
	}

	cfg := activitylogmq.Config{
		Broker:             activitylogmq.BrokerKafka,
		Topic:              "activity-log.create.integration.kafka",
		KafkaBrokers:       splitCSV(brokers),
		KafkaConsumerGroup: group,
	}
	if !cfg.Enabled() {
		t.Fatal("Kafka config is not enabled; set KAFKA_BROKERS")
	}

	pubSubRoundTrip(t, cfg)
}

func pubSubRoundTrip(t *testing.T, cfg activitylogmq.Config) {
	t.Helper()

	logger := watermill.NewStdLogger(false, false)

	pub, err := activitylogmq.NewPublisher(cfg, logger)
	if err != nil {
		t.Fatalf("NewPublisher(%s): %v", cfg.Broker, err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	sub, err := activitylogmq.NewSubscriber(cfg, logger)
	if err != nil {
		t.Fatalf("NewSubscriber(%s): %v", cfg.Broker, err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	msgs, err := sub.Subscribe(ctx, cfg.Topic)
	if err != nil {
		t.Fatalf("Subscribe(%s): %v", cfg.Broker, err)
	}

	// Allow Kafka consumer-group join / AMQP consumer startup before publish.
	time.Sleep(5 * time.Second)

	want := []byte(`{"event":"integration_test","logName":"ci"}`)
	msg := message.NewMessage(watermill.NewUUID(), want)
	if err := pub.Publish(cfg.Topic, msg); err != nil {
		t.Fatalf("Publish(%s): %v", cfg.Broker, err)
	}

	select {
	case got := <-msgs:
		if got == nil {
			t.Fatalf("Subscribe(%s): nil message", cfg.Broker)
		}
		got.Ack()
		if string(got.Payload) != string(want) {
			t.Fatalf("payload = %q, want %q", got.Payload, want)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for message on %s topic %q", cfg.Broker, cfg.Topic)
	}
}

func rabbitMQURIFromEnv() string {
	if u := strings.TrimSpace(os.Getenv("RABBITMQ_URL")); u != "" {
		return u
	}
	host := strings.TrimSpace(os.Getenv("RABBITMQ_HOST"))
	if host == "" {
		host = "localhost"
	}
	user := envOr("RABBITMQ_USER", "guest")
	pass := envOr("RABBITMQ_PASSWORD", "guest")
	port := envOr("RABBITMQ_PORT", "5672")
	return "amqp://" + user + ":" + pass + "@" + host + ":" + port + "/"
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
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
