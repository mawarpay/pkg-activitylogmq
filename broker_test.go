package activitylogmq

import (
	"strings"
	"testing"
)

func TestNewPublisher_Disabled(t *testing.T) {
	_, err := NewPublisher(Config{Broker: BrokerRabbitMQ}, nil)
	if err == nil {
		t.Fatal("expected error for disabled config")
	}
	if !strings.Contains(err.Error(), `broker "rabbitmq" is not configured`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewSubscriber_Disabled(t *testing.T) {
	_, err := NewSubscriber(Config{Broker: BrokerPubSub}, nil)
	if err == nil {
		t.Fatal("expected error for disabled config")
	}
	if !strings.Contains(err.Error(), `broker "pubsub" is not configured`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewPublisher_UnknownBrokerNotEnabled(t *testing.T) {
	_, err := NewPublisher(Config{
		Broker:      Broker("redis"),
		RabbitMQURI: "amqp://guest:guest@localhost:5672/",
	}, nil)
	if err == nil {
		t.Fatal("expected error for unknown broker")
	}
	if !strings.Contains(err.Error(), `broker "redis" is not configured`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
