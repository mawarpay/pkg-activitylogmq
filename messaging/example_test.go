package messaging_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/mawarpay/pkg-activitylogmq/clients"
	"github.com/mawarpay/pkg-activitylogmq/messaging"
)

func ExampleEnqueueActivityLogCreate() {
	ctx := context.Background()

	// Call Init once at process start when a broker is configured.
	// If Init was skipped or the broker is disabled, Enqueue returns
	// ErrNotConfigured — log and continue so the business request is not blocked.
	err := messaging.EnqueueActivityLogCreate(ctx, clients.CreateBody{
		LogName: "auth",
		Event:   "login",
	})
	if errors.Is(err, messaging.ErrNotConfigured) {
		fmt.Println("queue not configured")
		return
	}
	if err != nil {
		fmt.Println("enqueue:", err)
	}
}

func ExampleInit() {
	ctx := context.Background()

	// Init is idempotent. Missing broker config warns and returns nil.
	if err := messaging.Init(ctx); err != nil {
		fmt.Println("init:", err)
		return
	}
	defer messaging.Close()

	_ = messaging.EnqueueActivityLogCreate(ctx, clients.CreateBody{
		LogName:     "bank",
		Description: "Bank created",
		SubjectType: "bank",
		Event:       "bank_created",
		SubjectID:   1,
		CauserType:  "Admin",
		CauserID:    2,
	})
}
