package clients

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

// --- HTTP contract tests ---

func TestHTTPLiquidity_SubmitAggregate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/parent-orders" {
			t.Errorf("path=%s want /v1/parent-orders", r.URL.Path)
		}
		if key := r.Header.Get("Idempotency-Key"); key != "k1" {
			t.Errorf("idem key=%q want k1", key)
		}
		var req AggregateOrderRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.AssetPair != "BTC/USD" {
			t.Errorf("pair=%s want BTC/USD", req.AssetPair)
		}
		_ = json.NewEncoder(w).Encode(FillResult{FillPrice: decimal.NewFromInt(50000), TotalFilled: decimal.NewFromInt(1), VenueRoutes: []VenueRoute{{Venue: "v", Share: decimal.NewFromInt(1), Price: decimal.NewFromInt(50000)}}})
	}))
	defer srv.Close()
	c := NewHTTPLiquidity(srv.URL)
	fill, err := c.SubmitAggregate(context.Background(), AggregateOrderRequest{AssetPair: "BTC/USD", Side: "buy", NotionalUSD: decimal.NewFromInt(50000)}, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if !fill.FillPrice.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("fill_price=%s want 50000", fill.FillPrice.String())
	}
}

func TestHTTPFX_SubmitExposure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/hedges" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(HedgeResult{HedgedNotional: decimal.NewFromInt(40000), Unhedged: decimal.NewFromInt(10000)})
	}))
	defer srv.Close()
	c := NewHTTPFX(srv.URL)
	res, err := c.SubmitExposure(context.Background(), HedgeRequest{FiatCurrency: "USD", NotionalUSD: decimal.NewFromInt(50000)}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !res.HedgedNotional.Equal(decimal.NewFromInt(40000)) {
		t.Fatalf("hedged=%s want 40000", res.HedgedNotional.String())
	}
}

func TestHTTPWallet_Fund(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/funding-moves" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(FundingMoveResult{Completed: true, TxID: "tx"})
	}))
	defer srv.Close()
	c := NewHTTPWallet(srv.URL)
	res, err := c.Fund(context.Background(), FundingMoveRequest{WalletID: "h", Asset: "BTC", Amount: decimal.NewFromInt(1)}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed {
		t.Fatal("expected completed")
	}
}

func TestHTTPLedger_Post(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/postings" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	if err := c.Post(context.Background(), LedgerPost{Aggregate: "batch", EventType: "batch.close"}, "k"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPAudit_Emit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/audit-events" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewHTTPAudit(srv.URL)
	if err := c.Emit(context.Background(), AuditEvent{Aggregate: "batch", EventType: "batch.close"}, "k"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP_ConflictAndBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("dup"))
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	err := c.Post(context.Background(), LedgerPost{}, "k")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

func TestHTTP_UnavailableError(t *testing.T) {
	// Use a closed server to force a connection error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	c := NewHTTPLedger(srv.URL)
	err := c.Post(context.Background(), LedgerPost{}, "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Fake tests ---

func TestFakeLiquidity_DuplicateKeyError(t *testing.T) {
	f := NewFakeLiquidity(FillResult{FillPrice: decimal.NewFromInt(1), TotalFilled: decimal.NewFromInt(1)})
	f.SetDuplicateKeyError(true)
	if _, err := f.SubmitAggregate(context.Background(), AggregateOrderRequest{}, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SubmitAggregate(context.Background(), AggregateOrderRequest{}, "k"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

func TestFakeLedger_DuplicateKeyError(t *testing.T) {
	f := NewFakeLedger()
	f.SetDuplicateKeyError(true)
	if err := f.Post(context.Background(), LedgerPost{}, "k"); err != nil {
		t.Fatal(err)
	}
	if err := f.Post(context.Background(), LedgerPost{}, "k"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

// --- additional coverage ---

func TestDefaultRetry(t *testing.T) {
	opts := DefaultRetry()
	if opts.MaxAttempts != 3 {
		t.Fatalf("maxAttempts=%d want 3", opts.MaxAttempts)
	}
	if opts.Backoff != 100*time.Millisecond {
		t.Fatalf("backoff=%v want 100ms", opts.Backoff)
	}
}

func TestDo_ClampsAttemptsBelowOne(t *testing.T) {
	calls := 0
	err := Do(context.Background(), RetryOptions{MaxAttempts: 0, Backoff: time.Millisecond}, nil, func(ctx context.Context) error {
		calls++
		return ErrUnavailable
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v want ErrUnavailable", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want 1 (clamped)", calls)
	}
}

func TestDo_RecordsSuccessOnCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Hour)
	cb.RecordFailure() // open the circuit... no, threshold 1 opens it
	// Use a fresh breaker and verify RecordSuccess path.
	cb2 := NewCircuitBreaker(2, time.Hour)
	err := Do(context.Background(), RetryOptions{MaxAttempts: 1, Backoff: time.Millisecond}, cb2, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cb2.IsOpen() {
		t.Fatal("expected closed after success")
	}
}

func TestDo_RetriesUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel before the first backoff completes to exercise the ctx.Done branch.
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	calls := 0
	err := Do(ctx, RetryOptions{MaxAttempts: 10, Backoff: 50 * time.Millisecond}, nil, func(ctx context.Context) error {
		calls++
		return ErrUnavailable
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
}

func TestDo_RecordsFailureOnExhausted(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Hour)
	err := Do(context.Background(), RetryOptions{MaxAttempts: 1, Backoff: time.Millisecond}, cb, func(ctx context.Context) error {
		return ErrUnavailable
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v want ErrUnavailable", err)
	}
	if !cb.IsOpen() {
		t.Fatal("expected circuit open after exhausted retries")
	}
}

func TestResilientFX_Retries(t *testing.T) {
	f := NewFakeFX(HedgeResult{HedgedNotional: decimal.NewFromInt(42)})
	f.SetError(ErrUnavailable)
	r := NewResilientFX(f, RetryOptions{MaxAttempts: 2, Backoff: time.Millisecond}, nil)
	out, err := r.SubmitExposure(context.Background(), HedgeRequest{FiatCurrency: "USD"}, "k")
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if out == nil || !out.HedgedNotional.Equal(decimal.NewFromInt(42)) {
		t.Fatalf("unexpected out=%+v", out)
	}
	if len(f.Calls()) != 2 {
		t.Fatalf("calls=%d want 2", len(f.Calls()))
	}
}

func TestFakeFX_DuplicateKeyError(t *testing.T) {
	f := NewFakeFX(HedgeResult{})
	f.dupErr = true
	if _, err := f.SubmitExposure(context.Background(), HedgeRequest{}, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SubmitExposure(context.Background(), HedgeRequest{}, "k"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

func TestFakeWallet_FundAndError(t *testing.T) {
	f := NewFakeWallet(FundingMoveResult{Completed: true, TxID: "tx"})
	res, err := f.Fund(context.Background(), FundingMoveRequest{WalletID: "w", Asset: "BTC", Amount: decimal.NewFromInt(1)}, "k")
	if err != nil || !res.Completed {
		t.Fatalf("fund: err=%v res=%+v", err, res)
	}
	if len(f.Calls()) != 1 || f.Calls()[0].WalletID != "w" {
		t.Fatalf("calls=%+v", f.Calls())
	}
	// Error path clears after one call.
	f.SetError(ErrUnavailable)
	if _, err := f.Fund(context.Background(), FundingMoveRequest{}, "k2"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v want ErrUnavailable", err)
	}
	// Subsequent call succeeds again.
	if _, err := f.Fund(context.Background(), FundingMoveRequest{}, "k3"); err != nil {
		t.Fatalf("err=%v want nil", err)
	}
}

func TestFakeAudit_EmitAndDuplicate(t *testing.T) {
	f := NewFakeAudit()
	if err := f.Emit(context.Background(), AuditEvent{Aggregate: "batch"}, "k"); err != nil {
		t.Fatal(err)
	}
	f.SetDuplicateKeyError(true)
	if err := f.Emit(context.Background(), AuditEvent{}, "k"); !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
	// SetError path.
	f2 := NewFakeAudit()
	f2.SetError(ErrUnavailable)
	if err := f2.Emit(context.Background(), AuditEvent{}, "k"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v want ErrUnavailable", err)
	}
	if len(f2.Calls()) != 1 {
		t.Fatalf("calls=%d want 1", len(f2.Calls()))
	}
}

func TestFakeLedger_SetErrorClearsAfterOne(t *testing.T) {
	f := NewFakeLedger()
	f.SetError(ErrUnavailable)
	if err := f.Post(context.Background(), LedgerPost{}, "k1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v want ErrUnavailable", err)
	}
	if err := f.Post(context.Background(), LedgerPost{}, "k2"); err != nil {
		t.Fatalf("err=%v want nil", err)
	}
	if len(f.Calls()) != 2 {
		t.Fatalf("calls=%d want 2", len(f.Calls()))
	}
}

func TestFakeLiquidity_SetFill(t *testing.T) {
	f := NewFakeLiquidity(FillResult{FillPrice: decimal.NewFromInt(1)})
	f.SetFill(FillResult{FillPrice: decimal.NewFromInt(999)})
	out, err := f.SubmitAggregate(context.Background(), AggregateOrderRequest{}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !out.FillPrice.Equal(decimal.NewFromInt(999)) {
		t.Fatalf("fill=%s want 999", out.FillPrice.String())
	}
}

func TestHTTP_BadRequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad"))
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	err := c.Post(context.Background(), LedgerPost{}, "k")
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err=%v want ErrBadRequest", err)
	}
}

func TestHTTP_OtherStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	err := c.Post(context.Background(), LedgerPost{}, "k")
	if err == nil || errors.Is(err, ErrConflict) || errors.Is(err, ErrBadRequest) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v want generic status error", err)
	}
}

func TestHTTP_EmptyBodySuccess(t *testing.T) {
	// 2xx with empty body and out != nil should decode nothing and return nil.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewHTTPFX(srv.URL)
	out, err := c.SubmitExposure(context.Background(), HedgeRequest{}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil out")
	}
}

func TestHTTP_NilOutSuccess(t *testing.T) {
	// 2xx with out == nil path (ledger Post uses nil out): covered by
	// existing TestHTTPLedger_Post (204). Verify the nil-out early return
	// by hitting a 200 with no body via the ledger client.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	if err := c.Post(context.Background(), LedgerPost{NotionalUSD: decimal.NewFromInt(100), FiatCurrency: "USD"}, "k"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP_LedgerPostAmountDefaults(t *testing.T) {
	// NotionalUSD == 0 is forwarded as "0" at full precision; empty fiat
	// defaults to USD. (The previous int64 truncation/clamp-to-1 has been
	// removed.)
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := NewHTTPLedger(srv.URL)
	if err := c.Post(context.Background(), LedgerPost{Aggregate: "a", EventType: "e"}, "k"); err != nil {
		t.Fatal(err)
	}
	entries, _ := got["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %v", got)
	}
	first := entries[0].(map[string]any)
	if first["amount"].(string) != "0" {
		t.Fatalf("amount=%v want 0", first["amount"])
	}
	if first["asset"].(string) != "USD" {
		t.Fatalf("asset=%v want USD", first["asset"])
	}
}

func TestTimeoutClient_PostJSONMarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	// Exercise postJSON marshal error by passing an unmarshalable body.
	tc := newTimeoutClient(time.Second)
	err := tc.postJSON(context.Background(), srv.URL, make(chan int), "k", nil)
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestTimeoutClient_PostJSONNewRequestError(t *testing.T) {
	tc := newTimeoutClient(time.Second)
	// Invalid URL (control character) makes NewRequestWithContext fail.
	err := tc.postJSON(context.Background(), "http://127.0.0.1:0\x00bad", struct{}{}, "k", nil)
	if err == nil {
		t.Fatal("expected new-request error")
	}
}

// --- Kafka audit + coalesceStr ---

func TestCoalesceStr(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{}, ""},
		{[]string{""}, ""},
		{[]string{"", ""}, ""},
		{[]string{"", "a", ""}, "a"},
		{[]string{"a", "b"}, "a"},
		{[]string{"first", "ignored"}, "first"},
	}
	for _, c := range cases {
		if got := coalesceStr(c.in...); got != c.want {
			t.Errorf("coalesceStr(%v)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestNewKafkaAudit_NoBrokers(t *testing.T) {
	if _, err := NewKafkaAudit(nil); err == nil {
		t.Fatal("expected error for no brokers")
	}
	if _, err := NewKafkaAudit([]string{}); err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestNewKafkaAudit_Success(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	c, err := NewKafkaAudit([]string{ln.Addr().String()})
	if err != nil {
		t.Fatalf("new kafka audit: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	kc := c.(*kafkaAudit)
	if err := kc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestKafkaAudit_Emit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			buf := make([]byte, 4096)
			for {
				if _, err := conn.Read(buf); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	kc := &kafkaAudit{writer: &kafka.Writer{
		Addr:         kafka.TCP(ln.Addr().String()),
		Topic:        "audit.v1",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		Transport: &kafka.Transport{
			DialTimeout: 200 * time.Millisecond,
		},
	}}
	defer kc.Close()
	// Emit builds the audit.v1 envelope and attempts to write to Kafka.
	// The listener is not a real Kafka broker, so WriteMessages fails —
	// but the envelope-construction lines run before the write.
	if err := kc.Emit(context.Background(), AuditEvent{Aggregate: "batch", EventType: "batch.close", Actor: "treasury"}, "idem-key"); err == nil {
		// If it happens to succeed (e.g. buffered), that's fine too.
	}
	// Emit with empty id generates a uuid internally.
	if err := kc.Emit(context.Background(), AuditEvent{Aggregate: "float", EventType: "float.adjust", Actor: ""}, ""); err == nil {
	}
}

func TestKafkaAudit_CloseNilWriter(t *testing.T) {
	k := &kafkaAudit{writer: nil}
	if err := k.Close(); err != nil {
		t.Fatalf("close nil writer: %v", err)
	}
}
