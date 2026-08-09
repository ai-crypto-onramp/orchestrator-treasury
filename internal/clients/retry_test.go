package clients

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected allow before failures")
	}
	cb.RecordFailure()
	if !cb.Allow() {
		t.Fatal("expected allow after 1 failure")
	}
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("expected open after 2 failures")
	}
}

func TestCircuitBreaker_ResetsAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(1, 40*time.Millisecond)
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("expected open")
	}
	time.Sleep(50 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected half-open allow after reset timeout")
	}
	// Half-open: a success closes.
	cb.RecordSuccess()
	if cb.IsOpen() {
		t.Fatal("expected closed after success")
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(1, 40*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(50 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected half-open allow")
	}
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("expected re-open after half-open failure")
	}
}

func TestDo_RetriesOnUnavailable(t *testing.T) {
	calls := 0
	err := Do(context.Background(), RetryOptions{MaxAttempts: 3, Backoff: time.Millisecond}, nil, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return ErrUnavailable
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err=%v", nil)
	}
	if calls != 3 {
		t.Fatalf("calls=%d want 3", calls)
	}
}

func TestDo_DoesNotRetryOnConflict(t *testing.T) {
	calls := 0
	err := Do(context.Background(), RetryOptions{MaxAttempts: 5, Backoff: time.Millisecond}, nil, func(ctx context.Context) error {
		calls++
		return ErrConflict
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want 1 (no retry on conflict)", calls)
	}
}

func TestDo_DoesNotRetryOnBadRequest(t *testing.T) {
	calls := 0
	_ = Do(context.Background(), RetryOptions{MaxAttempts: 5, Backoff: time.Millisecond}, nil, func(ctx context.Context) error {
		calls++
		return ErrBadRequest
	})
	if calls != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
}

func TestDo_CircuitOpenFailsFast(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Hour)
	cb.RecordFailure()
	calls := 0
	err := Do(context.Background(), RetryOptions{MaxAttempts: 3, Backoff: time.Millisecond}, cb, func(ctx context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err=%v want ErrCircuitOpen", err)
	}
	if calls != 0 {
		t.Fatalf("calls=%d want 0 (fail fast)", calls)
	}
}

func TestResilientLiquidity_Retries(t *testing.T) {
	f := NewFakeLiquidity(FillResult{FillPrice: decimal.NewFromInt(1), TotalFilled: decimal.NewFromInt(1)})
	f.SetError(ErrUnavailable)
	r := NewResilientLiquidity(f, RetryOptions{MaxAttempts: 2, Backoff: time.Millisecond}, nil)
	// First call errors with ErrUnavailable; the fake clears the error
	// after one call, so the retry succeeds and the wrapper returns the
	// fill.
	out, err := r.SubmitAggregate(context.Background(), AggregateOrderRequest{}, "k")
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if out == nil || !out.FillPrice.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("expected fill, got %+v", out)
	}
	calls := f.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls=%d want 2 (1 error + 1 success)", len(calls))
	}
}

func TestDefaultCircuitBreaker(t *testing.T) {
	cb := DefaultCircuitBreaker()
	if cb == nil {
		t.Fatal("expected breaker")
	}
	if !cb.Allow() {
		t.Fatal("expected allow initially")
	}
}
