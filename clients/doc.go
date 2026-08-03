// Package clients provides an HTTP client for creating activity-log rows in
// activity-log-service.
//
// The messaging package uses this client from its Watermill consumer to forward
// queued CreateBody payloads to POST /activity-logs. Services may also call
// the client synchronously when a broker is not needed.
//
// # Configuration
//
// The package-level [ActivityLog] client is constructed from
// ACTIVITY_LOG_SERVICE_URL at init time. Prefer [NewActivityLogClient] in tests
// or when the URL is supplied at runtime.
//
// # Example
//
//	c := clients.NewActivityLogClient("http://activity-log-service:8080", nil)
//	err := c.Create(ctx, clients.CreateBody{
//	    LogName:     "auth",
//	    Description: "User logged in",
//	    SubjectType: "User",
//	    Event:       "login",
//	    SubjectID:   42,
//	    CauserType:  "User",
//	    CauserID:    42,
//	})
//
// Create returns [ErrActivityLogNotConfigured] when the base URL is empty.
// Non-2xx responses and transport failures are returned as wrapped errors.
package clients
