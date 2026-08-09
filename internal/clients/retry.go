package clients

import (
	"context"
	"errors"
	"sync"
	"time"
)

// RetryOptions controls the retry behavior of ResilientClient.
type RetryOptions struct {
	MaxAttempts int           // total attempts (1 = no retry)
	Backoff     time.Duration // initial backoff between attempts
}

// DefaultRetry returns sensible defaults: 3 attempts, 100ms backoff.
func DefaultRetry() RetryOptions {
	return RetryOptions{MaxAttempts: 3, Backoff: 100 * time.Millisecond}
}

// CircuitBreaker is a simple circuit breaker: after `failThreshold`
// consecutive failures the circuit opens for `resetTimeout`; during the
// open window calls fail fast with ErrCircuitOpen. After the timeout a
// single probe call is allowed (half-open); on success the circuit
// closes, on failure it re-opens.
type CircuitBreaker struct {
	mu             sync.Mutex
	failures       int
	failThreshold  int
	resetTimeout   time.Duration
	openedAt       time.Time
	open           bool
	halfOpen       bool
}

// ErrCircuitOpen is returned when the circuit is open.
var ErrCircuitOpen = errors.New("clients: circuit open")

// NewCircuitBreaker returns a circuit breaker that opens after
// failThreshold consecutive failures and resets after resetTimeout.
func NewCircuitBreaker(failThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{failThreshold: failThreshold, resetTimeout: resetTimeout}
}

// Allow reports whether a call is permitted.
func (c *CircuitBreaker) Allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.open {
		return true
	}
	// Open: check if reset timeout has elapsed.
	if time.Since(c.openedAt) >= c.resetTimeout {
		c.halfOpen = true
		return true
	}
	return false
}

// RecordSuccess resets the failure count and closes the circuit.
func (c *CircuitBreaker) RecordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.open = false
	c.halfOpen = false
}

// RecordFailure increments the failure count and opens the circuit if
// the threshold is reached.
func (c *CircuitBreaker) RecordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.halfOpen {
		// Half-open failure: re-open immediately.
		c.open = true
		c.openedAt = time.Now()
		c.halfOpen = false
		return
	}
	c.failures++
	if c.failures >= c.failThreshold {
		c.open = true
		c.openedAt = time.Now()
	}
}

// IsOpen reports whether the circuit is currently open (test helper).
func (c *CircuitBreaker) IsOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.open && time.Since(c.openedAt) < c.resetTimeout
}

// Do executes fn with retry and circuit-breaker semantics. fn must return
// a typed error; ErrUnavailable is retried, ErrConflict/ErrBadRequest are
// not (they are deterministic client errors).
func Do(ctx context.Context, opts RetryOptions, cb *CircuitBreaker, fn func(context.Context) error) error {
	if cb != nil && !cb.Allow() {
		return ErrCircuitOpen
	}
	attempts := opts.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		err := fn(ctx)
		if err == nil {
			if cb != nil {
				cb.RecordSuccess()
			}
			return nil
		}
		lastErr = err
		// Deterministic client errors: do not retry.
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrBadRequest) {
			if cb != nil {
				cb.RecordFailure()
			}
			return err
		}
		// Circuit-open: stop.
		if cb != nil && errors.Is(err, ErrCircuitOpen) {
			return err
		}
		// Retryable: backoff then retry.
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opts.Backoff):
			}
		}
	}
	if cb != nil {
		cb.RecordFailure()
	}
	return lastErr
}