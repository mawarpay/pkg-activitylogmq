package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	activitylogmq "github.com/writdev-alt/pkg-activitylogmq"
	"github.com/writdev-alt/pkg-activitylogmq/clients"
)

func resetMessagingTestState(t *testing.T) {
	t.Helper()
	publisher = nil
	subscriber = nil
	router = nil
	brokerCfg = activitylogmq.Config{}
	initOnce = sync.Once{}
}

func clearBrokerEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"MESSAGE_BROKER",
		"ACTIVITY_LOG_QUEUE",
		"ACTIVITY_LOG_TOPIC",
		"RABBITMQ_URL",
		"RABBITMQ_HOST",
		"PUBSUB_PROJECT_ID",
		"KAFKA_BROKERS",
	} {
		t.Setenv(key, "")
	}
}

type fakePublisher struct {
	topic   string
	payload []byte
	err     error
	closed  bool
}

func (f *fakePublisher) Publish(topic string, messages ...*message.Message) error {
	f.topic = topic
	if len(messages) > 0 && messages[0] != nil {
		f.payload = append([]byte(nil), messages[0].Payload...)
	}
	return f.err
}

func (f *fakePublisher) Close() error {
	f.closed = true
	return nil
}

func TestInit_NoBrokerConfigured(t *testing.T) {
	resetMessagingTestState(t)
	clearBrokerEnv(t)

	if err := Init(context.Background()); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if publisher != nil {
		t.Fatal("expected publisher to remain nil when broker is disabled")
	}
}

func TestEnqueueActivityLogCreate_NotConfigured(t *testing.T) {
	resetMessagingTestState(t)

	err := EnqueueActivityLogCreate(context.Background(), clients.CreateBody{Event: "test"})
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("EnqueueActivityLogCreate() error = %v, want %v", err, ErrNotConfigured)
	}
}

func TestEnqueueActivityLogCreate_Publishes(t *testing.T) {
	resetMessagingTestState(t)

	pub := &fakePublisher{}
	publisher = pub
	brokerCfg = activitylogmq.Config{Topic: "activity-log.create"}

	body := clients.CreateBody{
		LogName:     "bank",
		Description: "Bank created",
		SubjectType: "bank",
		Event:       "bank_created",
		SubjectID:   1,
		CauserType:  "Admin",
		CauserID:    2,
	}

	if err := EnqueueActivityLogCreate(context.Background(), body); err != nil {
		t.Fatalf("EnqueueActivityLogCreate() error = %v", err)
	}
	if pub.topic != "activity-log.create" {
		t.Fatalf("published topic = %q, want activity-log.create", pub.topic)
	}

	var got clients.CreateBody
	if err := json.Unmarshal(pub.payload, &got); err != nil {
		t.Fatalf("unmarshal published payload: %v", err)
	}
	if !reflect.DeepEqual(got, body) {
		t.Fatalf("published body = %+v, want %+v", got, body)
	}
}

func TestEnqueueActivityLogCreate_PublishError(t *testing.T) {
	resetMessagingTestState(t)

	pub := &fakePublisher{err: errors.New("publish failed")}
	publisher = pub
	brokerCfg = activitylogmq.Config{Topic: "activity-log.create"}

	err := EnqueueActivityLogCreate(context.Background(), clients.CreateBody{Event: "test"})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if !errors.Is(err, pub.err) && err.Error() != "publish activity log message: publish failed" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHandleActivityLogCreate_InvalidJSON(t *testing.T) {
	out, err := handleActivityLogCreate(message.NewMessage("id", []byte("{invalid")))
	if err != nil {
		t.Fatalf("handleActivityLogCreate() error = %v, want nil", err)
	}
	if out != nil {
		t.Fatalf("handleActivityLogCreate() = %#v, want nil", out)
	}
}

func TestHandleActivityLogCreate_NotConfigured(t *testing.T) {
	old := clients.ActivityLog
	t.Cleanup(func() { clients.ActivityLog = old })
	clients.ActivityLog = clients.NewActivityLogClient("", nil)

	body, err := json.Marshal(clients.CreateBody{Event: "bank_created"})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	out, err := handleActivityLogCreate(message.NewMessage("id", body))
	if err != nil {
		t.Fatalf("handleActivityLogCreate() error = %v, want nil", err)
	}
	if out != nil {
		t.Fatalf("handleActivityLogCreate() = %#v, want nil", out)
	}
}

func TestHandleActivityLogCreate_Success(t *testing.T) {
	want := clients.CreateBody{
		LogName:     "bank",
		Description: "Bank created",
		SubjectType: "bank",
		Event:       "bank_created",
		SubjectID:   99,
		CauserType:  "Admin",
		CauserID:    5,
	}

	var posted clients.CreateBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	old := clients.ActivityLog
	t.Cleanup(func() { clients.ActivityLog = old })
	clients.ActivityLog = clients.NewActivityLogClient(srv.URL, srv.Client())

	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	out, err := handleActivityLogCreate(message.NewMessage("id", payload))
	if err != nil {
		t.Fatalf("handleActivityLogCreate() error = %v", err)
	}
	if out != nil {
		t.Fatalf("handleActivityLogCreate() = %#v, want nil", out)
	}
	if !reflect.DeepEqual(posted, want) {
		t.Fatalf("posted body = %+v, want %+v", posted, want)
	}
}

func TestClose_NoOpWhenUninitialized(t *testing.T) {
	resetMessagingTestState(t)
	if err := Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestClose_ClosesPublisher(t *testing.T) {
	resetMessagingTestState(t)

	pub := &fakePublisher{}
	publisher = pub

	if err := Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !pub.closed {
		t.Fatal("expected publisher to be closed")
	}
}
