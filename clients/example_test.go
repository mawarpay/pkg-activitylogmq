package clients_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/mawarpay/pkg-activitylogmq/clients"
)

func ExampleActivityLogClient_Create() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body clients.CreateBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := clients.NewActivityLogClient(srv.URL, srv.Client())
	err := c.Create(context.Background(), clients.CreateBody{
		LogName:     "auth",
		Description: "User logged in",
		SubjectType: "User",
		Event:       "login",
		SubjectID:   42,
		CauserType:  "User",
		CauserID:    42,
	})
	fmt.Println(err == nil)
	// Output: true
}

func ExampleActivityLogClient_Create_notConfigured() {
	c := clients.NewActivityLogClient("", nil)
	err := c.Create(context.Background(), clients.CreateBody{Event: "login"})
	fmt.Println(errors.Is(err, clients.ErrActivityLogNotConfigured))
	// Output: true
}

func ExampleNewActivityLogClient() {
	c := clients.NewActivityLogClient("http://activity-log-service:8080/", nil)
	// Trailing slashes are trimmed; Create posts to baseURL + "/activity-logs".
	_ = c
}
