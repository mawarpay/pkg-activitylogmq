// Package messaging wires activity-log publishing and consumption for
// IlonaPay microservices.
//
// Call [Init] once at process startup. It loads broker config from the
// environment, starts a Watermill publisher and subscriber, and runs a
// consumer that POSTs each message to activity-log-service via
// [github.com/mawarpay/pkg-activitylogmq/clients]. Emit audit events with
// [EnqueueActivityLogCreate]; call [Close] on shutdown.
//
// # Typical usage
//
//	if err := messaging.Init(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer messaging.Close()
//
//	err := messaging.EnqueueActivityLogCreate(ctx, clients.CreateBody{
//	    LogName: "auth",
//	    Event:   "login",
//	    // ...
//	})
//
// # Behavior
//
//   - Missing broker config: Init logs a warning and returns nil; publishing
//     later returns [ErrNotConfigured].
//   - Init is idempotent (sync.Once); a second call is a no-op.
//   - Invalid JSON payloads are logged and acknowledged (not retried).
//   - When ACTIVITY_LOG_SERVICE_URL is unset, delivered messages are
//     acknowledged and dropped.
//   - Non-2xx or transport errors from the HTTP client cause a NACK so
//     Watermill can retry.
//
// # Competing consumers
//
// Init always starts both publisher and consumer. With RabbitMQ durable queues,
// every process that calls Init binds the same queue name (the topic) and
// consumers compete. Ensure every such process has ACTIVITY_LOG_SERVICE_URL
// set, or events won by an unconfigured consumer are lost.
package messaging
