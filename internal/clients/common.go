package clients

import (
	"errors"
	"net/http"
	"time"
)

// Common sentinel errors returned by the clients.
var (
	// ErrConflict is returned when a downstream reports an idempotency
	// conflict (duplicate key).
	ErrConflict = errors.New("clients: conflict")
	// ErrUnavailable is returned when the downstream is unreachable.
	ErrUnavailable = errors.New("clients: unavailable")
	// ErrBadRequest is returned when the downstream rejects the payload.
	ErrBadRequest = errors.New("clients: bad request")
)

// timeoutClient wraps net/http.Client with a default timeout.
type timeoutClient struct {
	http *http.Client
}

func newTimeoutClient(timeout time.Duration) *timeoutClient {
	return &timeoutClient{http: &http.Client{Timeout: timeout}}
}