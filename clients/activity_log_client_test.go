package clients

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestActivityLogClient_enabled(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    bool
	}{
		{name: "empty", baseURL: "", want: false},
		{name: "set", baseURL: "http://activity-log:8080", want: true},
		{name: "trailing slash trimmed", baseURL: "http://activity-log:8080/", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewActivityLogClient(tt.baseURL, nil)
			if got := c.enabled(); got != tt.want {
				t.Fatalf("enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActivityLogClient_Create_NotConfigured(t *testing.T) {
	c := NewActivityLogClient("", nil)
	err := c.Create(context.Background(), CreateBody{Event: "test"})
	if !errors.Is(err, ErrActivityLogNotConfigured) {
		t.Fatalf("Create() error = %v, want %v", err, ErrActivityLogNotConfigured)
	}
}

func TestActivityLogClient_Create_Success(t *testing.T) {
	want := CreateBody{
		LogName:     "bank",
		Description: "Bank created",
		SubjectType: "bank",
		Event:       "bank_created",
		SubjectID:   42,
		CauserType:  "Admin",
		CauserID:    7,
		Properties:  map[string]interface{}{"bank_code": "BCA"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/activity-logs" {
			t.Errorf("path = %q, want /activity-logs", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var got CreateBody
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("unmarshal body: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("body = %+v, want %+v", got, want)
		}

		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := NewActivityLogClient(srv.URL, srv.Client())
	if err := c.Create(context.Background(), want); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
}

func TestActivityLogClient_Create_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	c := NewActivityLogClient(srv.URL, srv.Client())
	err := c.Create(context.Background(), CreateBody{Event: "test"})
	if err == nil {
		t.Fatal("expected error for non-2xx response")
	}
	if !strings.Contains(err.Error(), "activity log service returned status 500") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestActivityLogClient_Create_RequestError(t *testing.T) {
	c := NewActivityLogClient("http://127.0.0.1:1", &http.Client{Timeout: 1})
	err := c.Create(context.Background(), CreateBody{Event: "test"})
	if err == nil {
		t.Fatal("expected error when request fails")
	}
	if !strings.Contains(err.Error(), "post activity log:") {
		t.Fatalf("unexpected error: %v", err)
	}
}
