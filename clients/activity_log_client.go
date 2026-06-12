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

var ErrActivityLogNotConfigured = errors.New("activity log service not configured")

// CreateBody is the JSON body for POST /activity-logs.
type CreateBody struct {
	LogName     string                 `json:"logName"`
	Description string                 `json:"description"`
	SubjectType string                 `json:"subjectType"`
	Event       string                 `json:"event"`
	SubjectID   uint64                 `json:"subjectId"`
	CauserType  string                 `json:"causerType"`
	CauserID    uint64                 `json:"causerId"`
	Properties  map[string]interface{} `json:"properties"`
}

// ActivityLogClient posts audit entries to activity-log-service (internal).
type ActivityLogClient struct {
	baseURL string
	client  *http.Client
}

// NewActivityLogClient builds a client for the given base URL and HTTP client.
func NewActivityLogClient(baseURL string, httpClient *http.Client) *ActivityLogClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &ActivityLogClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  httpClient,
	}
}

var ActivityLog = NewActivityLogClient(os.Getenv("ACTIVITY_LOG_SERVICE_URL"), nil)

func (c *ActivityLogClient) enabled() bool {
	return c.baseURL != ""
}

// Create posts a new activity log row.
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
