package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/turahe/pkg/logger"
	activitylogmq "github.com/writdev-alt/pkg-activitylogmq"
	"github.com/writdev-alt/pkg-activitylogmq/clients"
)

var ErrNotConfigured = errors.New("activity log queue not configured")

var (
	brokerCfg  activitylogmq.Config
	publisher  message.Publisher
	subscriber message.Subscriber
	router     *message.Router
	initOnce   sync.Once
)

// Init connects to the message broker, starts the activity-log HTTP consumer, and enables publishing.
func Init(ctx context.Context) error {
	var initErr error
	initOnce.Do(func() {
		initErr = initMessaging(ctx)
	})
	return initErr
}

func initMessaging(ctx context.Context) error {
	brokerCfg = activitylogmq.LoadConfig()
	if !brokerCfg.Enabled() {
		logger.Warnf("messaging: no message broker configured (set MESSAGE_BROKER or broker env vars); activity log queue disabled")
		return nil
	}

	amqpLogger := watermill.NewStdLogger(false, false)

	pub, err := activitylogmq.NewPublisher(brokerCfg, amqpLogger)
	if err != nil {
		return fmt.Errorf("activity log publisher: %w", err)
	}

	sub, err := activitylogmq.NewSubscriber(brokerCfg, amqpLogger)
	if err != nil {
		_ = pub.Close()
		return fmt.Errorf("activity log subscriber: %w", err)
	}

	r, err := message.NewRouter(message.RouterConfig{}, amqpLogger)
	if err != nil {
		_ = pub.Close()
		_ = sub.Close()
		return fmt.Errorf("watermill router: %w", err)
	}

	r.AddHandler(
		"activity_log_http_create",
		brokerCfg.Topic,
		sub,
		"",
		nil,
		handleActivityLogCreate,
	)

	go func() {
		if err := r.Run(ctx); err != nil {
			logger.Errorf("messaging: activity log consumer stopped: %v", err)
		}
	}()

	publisher = pub
	subscriber = sub
	router = r
	logger.Infof("messaging: activity log queue ready broker=%s topic=%s", brokerCfg.Broker, brokerCfg.Topic)
	return nil
}

// Close shuts down the Watermill router, subscriber, and publisher.
func Close() error {
	var err error
	if router != nil {
		if closeErr := router.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if subscriber != nil {
		if closeErr := subscriber.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if publisher != nil {
		if closeErr := publisher.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// EnqueueActivityLogCreate publishes an activity-log create payload for async HTTP delivery.
func EnqueueActivityLogCreate(ctx context.Context, body clients.CreateBody) error {
	log := logger.WithContext(ctx)
	if publisher == nil {
		log.Warnf("messaging: activity log queue not configured; dropping message")
		return ErrNotConfigured
	}
	payload, err := json.Marshal(body)
	if err != nil {
		log.Errorf("messaging: marshal activity log message: %v", err)
		return fmt.Errorf("marshal activity log message: %w", err)
	}
	msg := message.NewMessage(watermill.NewUUID(), payload)
	if err := publisher.Publish(brokerCfg.Topic, msg); err != nil {
		log.Errorf("messaging: publish activity log message: %v", err)
		return fmt.Errorf("publish activity log message: %w", err)
	}
	return nil
}

func handleActivityLogCreate(msg *message.Message) ([]*message.Message, error) {
	var body clients.CreateBody
	if err := json.Unmarshal(msg.Payload, &body); err != nil {
		logger.Errorf("messaging: invalid activity log payload: %v", err)
		return nil, nil
	}

	if err := clients.ActivityLog.Create(context.Background(), body); err != nil {
		if errors.Is(err, clients.ErrActivityLogNotConfigured) {
			logger.Warnf("messaging: ACTIVITY_LOG_SERVICE_URL not set; dropping message")
			return nil, nil
		}
		return nil, err
	}
	return nil, nil
}
