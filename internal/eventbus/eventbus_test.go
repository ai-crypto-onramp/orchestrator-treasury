package eventbus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestInMemory_PushAndSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewInMemory()
	ch, stop, err := bus.Subscribe(ctx, "tx.completed")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ev := TxCompletedEvent{TxID: "tx1", Asset: "BTC", FiatCurrency: "USD"}
	if err := bus.Push(ctx, "tx.completed", ev); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got.TxID != "tx1" {
			t.Fatalf("got tx=%s want tx1", got.TxID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestInMemory_UnsubscribeStopsChannel(t *testing.T) {
	ctx := context.Background()
	bus := NewInMemory()
	ch, stop, _ := bus.Subscribe(ctx, "tx.completed")
	stop()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("expected channel closed")
	}
}

func TestInMemory_TopicIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := NewInMemory()
	ch, stop, _ := bus.Subscribe(ctx, "tx.completed")
	defer stop()
	_ = bus.Push(ctx, "other.topic", TxCompletedEvent{TxID: "x"})
	select {
	case <-ch:
		t.Fatal("expected no event on different topic")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHTTPPush_Handler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHTTPPush()
	ch, stop, _ := h.Subscribe(ctx, "tx.completed")
	defer stop()
	srv := httptest.NewServer(h.HTTPHandler())
	defer srv.Close()
	body := `{"tx_id":"tx2","asset":"ETH","fiat_currency":"USD","amount":1,"notional_usd":2000}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/events/tx.completed", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d want 202", resp.StatusCode)
	}
	select {
	case got := <-ch:
		if got.TxID != "tx2" {
			t.Fatalf("got tx=%s want tx2", got.TxID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestHTTPPush_MethodNotAllowed(t *testing.T) {
	h := NewHTTPPush()
	req := httptest.NewRequest(http.MethodGet, "/v1/events/tx.completed", nil)
	rec := httptest.NewRecorder()
	h.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", rec.Code)
	}
}

// --- additional coverage ---

func TestHTTPPush_MissingTopic(t *testing.T) {
	h := NewHTTPPush()
	req := httptest.NewRequest(http.MethodPost, "/v1/events/", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	h.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", rec.Code)
	}
}

func TestHTTPPush_MalformedJSON(t *testing.T) {
	h := NewHTTPPush()
	req := httptest.NewRequest(http.MethodPost, "/v1/events/tx.completed", strings.NewReader("{not json"))
	rec := httptest.NewRecorder()
	h.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", rec.Code)
	}
}

func TestHTTPPush_PushDelegatesToInner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHTTPPush()
	ch, stop, _ := h.Subscribe(ctx, "tx.completed")
	defer stop()
	if err := h.Push(ctx, "tx.completed", TxCompletedEvent{TxID: "p1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got.TxID != "p1" {
			t.Fatalf("tx=%s want p1", got.TxID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected event via HTTPPush.Push")
	}
}

func TestInMemory_PushFullChannelDrops(t *testing.T) {
	ctx := context.Background()
	bus := NewInMemory()
	// Subscribe a tiny-buffer channel; the channel is buffered to 1024
	// (fixed in Subscribe). Push more than that to force the default
	// drop branch.
	ch, stop, _ := bus.Subscribe(ctx, "drop.topic")
	defer stop()
	for i := 0; i < 2000; i++ {
		_ = bus.Push(ctx, "drop.topic", TxCompletedEvent{TxID: "x"})
	}
	// Drain at least one so the test does not deadlock; the rest may be
	// dropped. We only assert the call returns without blocking.
	select {
	case <-ch:
	default:
		t.Fatal("expected at least one event delivered")
	}
}

func TestInMemory_SubscribeCtxCancelStopsChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bus := NewInMemory()
	ch, stop, _ := bus.Subscribe(ctx, "ctx.topic")
	// Cancel the parent context; the goroutine in Subscribe will call
	// stop() and close the channel.
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed after ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("expected channel closed")
	}
	// stop() must be idempotent (calling after ctx-cancelled close should
	// not panic).
	stop()
}

func TestNewKafkaSubscriber_NoBrokersError(t *testing.T) {
	if _, err := NewKafkaSubscriber(nil, "t", "g"); err == nil {
		t.Fatal("expected error for no brokers")
	}
}

func TestNewKafkaSubscriber_DefaultsTopicAndGroup(t *testing.T) {
	s, err := NewKafkaSubscriber([]string{"broker1:9092"}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if s.reader == nil {
		t.Fatal("expected reader configured")
	}
	_ = s.reader.Close()
}

func TestNewKafkaSubscriberFromURL_ErrorsOnNonKafkaScheme(t *testing.T) {
	if _, err := NewKafkaSubscriberFromURL("http://x", "g"); err == nil {
		t.Fatal("expected error for non-kafka scheme")
	}
}

func TestNewKafkaSubscriberFromURL_ParsesBrokersAndTopic(t *testing.T) {
	s, err := NewKafkaSubscriberFromURL("kafka://h1:9092,h2:9092?topic=custom.topic", "grp")
	if err != nil {
		t.Fatal(err)
	}
	if s.reader == nil {
		t.Fatal("expected reader")
	}
	_ = s.reader.Close()
}

func TestNewKafkaSubscriberFromURL_TrimSpacesAndSkipsEmpty(t *testing.T) {
	// Whitespace around brokers should be trimmed and empty entries
	// skipped. With only empty entries after trim, NewKafkaSubscriber
	// should error out.
	if _, err := NewKafkaSubscriberFromURL("kafka://  ,  ", "g"); err == nil {
		t.Fatal("expected error when all brokers are empty after trim")
	}
}

func TestKafkaSubscriber_PushIsNoOp(t *testing.T) {
	// Push is a no-op for the Kafka subscriber; construct via URL parse
	// to avoid needing a real broker for reader config.
	s, err := NewKafkaSubscriberFromURL("kafka://h:9092?topic=t", "g")
	if err != nil {
		t.Fatal(err)
	}
	defer s.reader.Close()
	if err := s.Push(context.Background(), "t", TxCompletedEvent{TxID: "x"}); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func TestKafkaSubscriber_SubscribeStopClosesChannel(t *testing.T) {
	// Subscribe spawns a reader goroutine that blocks on ReadMessage
	// until ctx is cancelled or the reader is closed. Calling stop
	// closes the reader and the channel. We exercise the stop path
	// without a real broker by immediately invoking stop.
	s, err := NewKafkaSubscriberFromURL("kafka://h:9092?topic=t", "g")
	if err != nil {
		t.Fatal(err)
	}
	defer s.reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, stop, err := s.Subscribe(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("expected channel closed")
	}
	// Idempotent stop.
	stop()
}

func TestTxCompletedEvent_JSONRoundtrip(t *testing.T) {
	ev := TxCompletedEvent{
		TxID:         "tx-7",
		Amount:       decimal.NewFromFloat(1.5),
		Asset:        "BTC",
		FiatCurrency: "USD",
		NotionalUSD:  decimal.NewFromInt(75000),
		UserID:       "u42",
		CompletedAt:  "2024-01-02T03:04:05Z",
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var got TxCompletedEvent
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.TxID != ev.TxID || !got.Amount.Equal(ev.Amount) || !got.NotionalUSD.Equal(ev.NotionalUSD) || got.Asset != ev.Asset || got.FiatCurrency != ev.FiatCurrency || got.UserID != ev.UserID || got.CompletedAt != ev.CompletedAt {
		t.Fatalf("roundtrip mismatch: got=%+v want=%+v", got, ev)
	}
}
