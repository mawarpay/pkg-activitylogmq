package activitylogmq

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// NewPublisher creates a Watermill publisher for the configured broker.
func NewPublisher(cfg Config, logger watermill.LoggerAdapter) (message.Publisher, error) {
	if !cfg.Enabled() {
		return nil, fmt.Errorf("activitylogmq: broker %q is not configured", cfg.Broker)
	}
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	switch cfg.Broker {
	case BrokerRabbitMQ:
		return amqp.NewPublisher(amqp.NewDurableQueueConfig(cfg.RabbitMQURI), logger)
	case BrokerPubSub:
		return googlecloud.NewPublisher(googlecloud.PublisherConfig{
			ProjectID: cfg.PubSubProjectID,
		}, logger)
	case BrokerKafka:
		return kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:   cfg.KafkaBrokers,
			Marshaler: kafka.DefaultMarshaler{},
		}, logger)
	default:
		return nil, fmt.Errorf("activitylogmq: unsupported broker %q", cfg.Broker)
	}
}

// NewSubscriber creates a Watermill subscriber for the configured broker.
func NewSubscriber(cfg Config, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	if !cfg.Enabled() {
		return nil, fmt.Errorf("activitylogmq: broker %q is not configured", cfg.Broker)
	}
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	switch cfg.Broker {
	case BrokerRabbitMQ:
		return amqp.NewSubscriber(amqp.NewDurableQueueConfig(cfg.RabbitMQURI), logger)
	case BrokerPubSub:
		return googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
			ProjectID:                cfg.PubSubProjectID,
			GenerateSubscriptionName: googlecloud.TopicSubscriptionName,
		}, logger)
	case BrokerKafka:
		return kafka.NewSubscriber(kafka.SubscriberConfig{
			Brokers:       cfg.KafkaBrokers,
			Unmarshaler:   kafka.DefaultMarshaler{},
			ConsumerGroup: cfg.KafkaConsumerGroup,
		}, logger)
	default:
		return nil, fmt.Errorf("activitylogmq: unsupported broker %q", cfg.Broker)
	}
}
