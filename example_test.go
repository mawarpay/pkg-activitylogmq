package activitylogmq_test

import (
	"fmt"

	activitylogmq "github.com/mawarpay/pkg-activitylogmq"
)

func ExampleConfig_Enabled() {
	enabled := activitylogmq.Config{
		Broker:      activitylogmq.BrokerRabbitMQ,
		RabbitMQURI: "amqp://guest:guest@localhost:5672/",
	}
	disabled := activitylogmq.Config{
		Broker: activitylogmq.BrokerRabbitMQ,
	}

	fmt.Println(enabled.Enabled())
	fmt.Println(disabled.Enabled())
	// Output:
	// true
	// false
}

func ExampleNewPublisher() {
	_, err := activitylogmq.NewPublisher(activitylogmq.Config{
		Broker: activitylogmq.BrokerRabbitMQ,
	}, nil)
	fmt.Println(err != nil)
	// Output: true
}

func ExampleLoadConfig() {
	// LoadConfig reads MESSAGE_BROKER and broker-specific env vars.
	// Check Enabled before constructing publishers or subscribers.
	cfg := activitylogmq.LoadConfig()
	if !cfg.Enabled() {
		fmt.Println("broker disabled")
		return
	}

	pub, err := activitylogmq.NewPublisher(cfg, nil)
	if err != nil {
		fmt.Println("publisher:", err)
		return
	}
	defer pub.Close()

	sub, err := activitylogmq.NewSubscriber(cfg, nil)
	if err != nil {
		fmt.Println("subscriber:", err)
		return
	}
	defer sub.Close()
}
