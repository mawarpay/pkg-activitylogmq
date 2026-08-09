// Package activitylogmq provides a Watermill broker factory for publishing and
// consuming IlonaPay activity-log (audit) messages.
//
// Most application code should use the higher-level [github.com/mawarpay/pkg-activitylogmq/messaging]
// package, which wires publisher, subscriber, and the HTTP forwarder to
// activity-log-service in one Init call. This root package is for services that
// need direct control over Watermill publishers and subscribers.
//
// # Supported brokers
//
//   - RabbitMQ (AMQP)
//   - Amazon MQ for RabbitMQ (AMQPS)
//   - Google Cloud Pub/Sub
//   - Apache Kafka
//
// Broker selection is driven by environment variables. Call [LoadConfig] then
// check [Config.Enabled] before constructing clients:
//
//	cfg := activitylogmq.LoadConfig()
//	if !cfg.Enabled() {
//	    // queue disabled; service continues without audit publishing
//	    return
//	}
//	pub, err := activitylogmq.NewPublisher(cfg, logger)
//	sub, err := activitylogmq.NewSubscriber(cfg, logger)
//
// # Configuration
//
// See the module README for the full environment-variable reference. Key vars:
//
//   - MESSAGE_BROKER — rabbitmq | amazonmq | pubsub | kafka (auto-detected when unset)
//   - ACTIVITY_LOG_QUEUE — topic / queue name (default activity-log.create)
//   - RABBITMQ_URL or RABBITMQ_HOST — RabbitMQ connection
//   - AMAZONMQ_URL or AMAZONMQ_HOST — Amazon MQ for RabbitMQ (AMQPS, port 5671)
//   - PUBSUB_PROJECT_ID — Google Cloud project
//   - KAFKA_BROKERS — comma-separated Kafka addresses
//
// # Caveats
//
// RabbitMQ and Amazon MQ use Watermill's durable queue config, which names the
// queue after the topic. Multiple subscribers on the same topic compete for
// deliveries. NewPublisher and NewSubscriber dial the broker; callers must
// Close the returned clients when finished.
package activitylogmq
