package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/turahe/pkg/logger"
)

// ErrActivityLogNotConfigured is returned by [ActivityLogClient.Create] when
// the client's base URL is empty (ACTIVITY_LOG_SERVICE_URL unset or an empty
// string passed to [NewActivityLogClient]).
var ErrActivityLogNotConfigured = errors.New("activity log service not configured")

// CreateBody is the JSON body for POST /activity-logs on activity-log-service.
// It is also the payload shape published on the activity-log message topic.
type CreateBody struct {
	// LogName groups related events (for example "auth" or "bank").
	LogName string `json:"logName"`

	// Description is a human-readable summary of the action.
	Description string `json:"description"`

	// SubjectType is the type of entity acted upon (for example "User").
	SubjectType string `json:"subjectType"`

	// Event is a machine-readable event name (for example "login").
	Event string `json:"event"`

	// SubjectID is the primary key of the subject entity.
	SubjectID uint64 `json:"subjectId"`

	// CauserType is the type of actor that caused the event (for example "Admin").
	CauserType string `json:"causerType"`

	// CauserID is the primary key of the causer entity.
	CauserID uint64 `json:"causerId"`

	// Properties holds optional structured metadata. Avoid logging secrets or
	// unnecessary PII.
	Properties map[string]interface{} `json:"properties"`
}

// ActivityLogClient posts audit entries to activity-log-service over HTTP.
//
// Instances are safe for concurrent use when the underlying [*http.Client] is.
// The default client created by [NewActivityLogClient] uses a 5s timeout and
// performs no retries; callers may supply their own client for custom transport
// or timeout behavior.
type ActivityLogClient struct {
	baseURL string
	client  *http.Client
}

// NewActivityLogClient builds a client that posts to baseURL + "/activity-logs".
// A trailing slash on baseURL is stripped. If httpClient is nil, a client with
// a 5-second timeout is used.
//
// An empty baseURL produces a usable client that returns
// [ErrActivityLogNotConfigured] from [ActivityLogClient.Create].
func NewActivityLogClient(baseURL string, httpClient *http.Client) *ActivityLogClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &ActivityLogClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  httpClient,
	}
}

// ActivityLog is the process-wide client initialized from
// ACTIVITY_LOG_SERVICE_URL at package load time. Reassign it in tests; prefer
// [NewActivityLogClient] when constructing clients with an explicit URL.
var ActivityLog = NewActivityLogClient(os.Getenv("ACTIVITY_LOG_SERVICE_URL"), nil)

func (c *ActivityLogClient) enabled() bool {
	return c.baseURL != ""
}

// Create posts body as JSON to {baseURL}/activity-logs.
//
// It returns [ErrActivityLogNotConfigured] when the base URL is empty.
// Marshal failures, request construction failures, transport errors, and
// non-2xx responses are returned as wrapped errors. On non-2xx, up to 512 bytes
// of the response body are logged at warn level.
//
// The request respects ctx for cancellation and deadlines.
func (c *ActivityLogClient) Create(ctx context.Context, body CreateBody) error {
	log := logger.WithContext(ctx)
	if !c.enabled() {
		return ErrActivityLogNotConfigured
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal activity log: %w", err)
	}

	url := c.baseURL + "/activity-logs"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build activity log request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("post activity log: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		log.Warnf("ActivityLogClient.Create: bad_status status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		return fmt.Errorf("activity log service returned status %d", resp.StatusCode)
	}
	return nil
}
