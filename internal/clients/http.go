package clients

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

// postJSON performs a POST with JSON body and an Idempotency-Key header,
// then decodes the JSON response into out. Returns typed errors for
// non-2xx responses.
func (c *timeoutClient) postJSON(ctx context.Context, url string, body any, idempotencyKey string, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out == nil {
			return nil
		}
		if len(respBody) == 0 {
			return nil
		}
		return json.Unmarshal(respBody, out)
	}
	switch resp.StatusCode {
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrConflict, string(respBody))
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", ErrBadRequest, string(respBody))
	default:
		return fmt.Errorf("clients: status %d: %s", resp.StatusCode, string(respBody))
	}
}

// --- liquidity-routing HTTP ---

func (h *httpLiquidity) SubmitAggregate(ctx context.Context, req AggregateOrderRequest, key string) (*FillResult, error) {
	var out FillResult
	url := h.baseURL + "/v1/parent-orders"
	if err := h.client.postJSON(ctx, url, req, key, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- fx-hedging ---

// HedgeRequest is the FX exposure payload forwarded to fx-hedging.
type HedgeRequest struct {
	FiatCurrency string          `json:"fiat_currency"`
	NotionalUSD  decimal.Decimal `json:"notional_usd"`
	BatchID      uuid.UUID       `json:"batch_id"`
}

// HedgeResult is the fx-hedging response.
type HedgeResult struct {
	HedgedNotional decimal.Decimal `json:"hedged_notional"`
	Unhedged       decimal.Decimal `json:"unhedged"`
}

// FXHedging is the client interface for the fx-hedging service.
type FXHedging interface {
	SubmitExposure(ctx context.Context, req HedgeRequest, idempotencyKey string) (*HedgeResult, error)
}

type httpFX struct {
	baseURL string
	client  *timeoutClient
}

// NewHTTPFX returns an HTTP-backed fx-hedging client.
func NewHTTPFX(baseURL string) FXHedging {
	return &httpFX{baseURL: baseURL, client: newTimeoutClient(10 * time.Second)}
}

func (h *httpFX) SubmitExposure(ctx context.Context, req HedgeRequest, key string) (*HedgeResult, error) {
	var out HedgeResult
	url := h.baseURL + "/v1/hedges"
	if err := h.client.postJSON(ctx, url, req, key, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FakeFX is a test double for FXHedging.
type FakeFX struct {
	calls  []HedgeRequest
	keys   map[string]bool
	result HedgeResult
	err    error
	dupErr bool
}

// NewFakeFX returns a fake fx-hedging client.
func NewFakeFX(result HedgeResult) *FakeFX {
	return &FakeFX{keys: map[string]bool{}, result: result}
}

// SetError configures the next call to return err.
func (f *FakeFX) SetError(err error) { f.err = err }

// SubmitExposure implements FXHedging.
func (f *FakeFX) SubmitExposure(_ context.Context, req HedgeRequest, key string) (*HedgeResult, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	if f.dupErr && f.keys[key] {
		return nil, ErrConflict
	}
	f.keys[key] = true
	out := f.result
	return &out, nil
}

// Calls returns the recorded requests.
func (f *FakeFX) Calls() []HedgeRequest {
	out := make([]HedgeRequest, len(f.calls))
	copy(out, f.calls)
	return out
}

// --- wallet-management ---

// FundingMoveRequest instructs wallet-management to move funds.
type FundingMoveRequest struct {
	WalletID string          `json:"wallet_id"`
	Asset    string          `json:"asset"`
	Amount   decimal.Decimal `json:"amount"`
	Source   string          `json:"source_venue"`
}

// FundingMoveResult is the wallet-management response.
type FundingMoveResult struct {
	Completed bool   `json:"completed"`
	TxID      string `json:"tx_id"`
}

// WalletManagement is the client interface for wallet-management.
type WalletManagement interface {
	Fund(ctx context.Context, req FundingMoveRequest, idempotencyKey string) (*FundingMoveResult, error)
}

type httpWallet struct {
	baseURL string
	client  *timeoutClient
}

// NewHTTPWallet returns an HTTP-backed wallet-management client.
func NewHTTPWallet(baseURL string) WalletManagement {
	return &httpWallet{baseURL: baseURL, client: newTimeoutClient(10 * time.Second)}
}

func (h *httpWallet) Fund(ctx context.Context, req FundingMoveRequest, key string) (*FundingMoveResult, error) {
	var out FundingMoveResult
	url := h.baseURL + "/v1/funding-moves"
	if err := h.client.postJSON(ctx, url, req, key, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FakeWallet is a test double for WalletManagement.
type FakeWallet struct {
	calls  []FundingMoveRequest
	result FundingMoveResult
	err    error
}

// NewFakeWallet returns a fake wallet-management client.
func NewFakeWallet(result FundingMoveResult) *FakeWallet {
	return &FakeWallet{result: result}
}

// SetError configures the next call to return err.
func (f *FakeWallet) SetError(err error) { f.err = err }

// Fund implements WalletManagement.
func (f *FakeWallet) Fund(_ context.Context, req FundingMoveRequest, _ string) (*FundingMoveResult, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	out := f.result
	return &out, nil
}

// Calls returns the recorded requests.
func (f *FakeWallet) Calls() []FundingMoveRequest {
	out := make([]FundingMoveRequest, len(f.calls))
	copy(out, f.calls)
	return out
}

// --- ledger-accounting ---

// LedgerPost is one ledger posting.
type LedgerPost struct {
	Aggregate    string          `json:"aggregate"`
	EventType    string          `json:"event_type"`
	NotionalUSD  decimal.Decimal `json:"notional_usd"`
	Asset        string          `json:"asset"`
	FiatCurrency string          `json:"fiat_currency"`
	BatchID      uuid.UUID       `json:"batch_id,omitempty"`
}

// LedgerAccounting is the client interface for ledger-accounting.
type LedgerAccounting interface {
	Post(ctx context.Context, post LedgerPost, idempotencyKey string) error
}

type httpLedger struct {
	baseURL string
	client  *timeoutClient
}

// NewHTTPLedger returns an HTTP-backed ledger client.
func NewHTTPLedger(baseURL string) LedgerAccounting {
	return &httpLedger{baseURL: baseURL, client: newTimeoutClient(10 * time.Second)}
}

func (h *httpLedger) Post(ctx context.Context, post LedgerPost, key string) error {
	url := h.baseURL + "/v1/postings"
	// Transform the treasury's domain-specific post into a balanced
	// double-entry posting the ledger expects: debit treasury_crypto,
	// credit operational_fiat for the same amount + asset. The amount is
	// forwarded at full precision as a string; the previous int64
	// truncation that dropped fractional cents and turned zero-amount
	// posts into 1 has been removed.
	amount := post.NotionalUSD
	asset := post.FiatCurrency
	if asset == "" {
		asset = "USD"
	}
	body := map[string]any{
		"posting_id": key,
		"memo":       post.Aggregate + ":" + post.EventType,
		"entries": []map[string]any{
			{"account_id": "treasury_crypto", "direction": "debit", "amount": amount.String(), "asset": asset},
			{"account_id": "operational_fiat", "direction": "credit", "amount": amount.String(), "asset": asset},
		},
	}
	return h.client.postJSON(ctx, url, body, key, nil)
}

// FakeLedger is a test double for LedgerAccounting.
type FakeLedger struct {
	mu     sync.Mutex
	calls  []LedgerPost
	keys   map[string]bool
	err    error
	dupErr bool
}

// NewFakeLedger returns a fake ledger client.
func NewFakeLedger() *FakeLedger {
	return &FakeLedger{keys: map[string]bool{}}
}

// SetError configures the next call to return err.
func (f *FakeLedger) SetError(err error) { f.err = err }

// SetDuplicateKeyError makes Post return ErrConflict on duplicate keys.
func (f *FakeLedger) SetDuplicateKeyError(on bool) { f.dupErr = on }

// Post implements LedgerAccounting.
func (f *FakeLedger) Post(_ context.Context, post LedgerPost, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, post)
	if f.err != nil {
		err := f.err
		f.err = nil
		return err
	}
	if f.dupErr && f.keys[key] {
		return ErrConflict
	}
	f.keys[key] = true
	return nil
}

// Calls returns the recorded posts.
func (f *FakeLedger) Calls() []LedgerPost {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]LedgerPost, len(f.calls))
	copy(out, f.calls)
	return out
}

// --- audit-event-log ---

// AuditEvent is one append-only audit record.
type AuditEvent struct {
	Aggregate string    `json:"aggregate"`
	EventType string    `json:"event_type"`
	Actor     string    `json:"actor"`
	BatchID   uuid.UUID `json:"batch_id,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// AuditLog is the client interface for audit-event-log.
type AuditLog interface {
	Emit(ctx context.Context, ev AuditEvent, idempotencyKey string) error
}

type httpAudit struct {
	baseURL string
	client  *timeoutClient
}

// NewHTTPAudit returns an HTTP-backed audit-event-log client. Deprecated:
// production wiring uses NewKafkaAudit (publishes the canonical audit.v1
// envelope to the `audit.v1` Kafka topic).
func NewHTTPAudit(baseURL string) AuditLog {
	return &httpAudit{baseURL: baseURL, client: newTimeoutClient(10 * time.Second)}
}

func (h *httpAudit) Emit(ctx context.Context, ev AuditEvent, key string) error {
	url := h.baseURL + "/v1/audit-events"
	return h.client.postJSON(ctx, url, ev, key, nil)
}

// kafkaAudit publishes the canonical audit.v1 envelope (see
// .github/contracts/asyncapi/audit/v1/asyncapi.yaml) to the `audit.v1` topic.
type kafkaAudit struct {
	writer *kafka.Writer
}

// NewKafkaAudit returns a Kafka-backed audit-event-log client targeting the
// `audit.v1` topic on the given brokers.
func NewKafkaAudit(brokers []string) (AuditLog, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("audit kafka: no brokers provided")
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "audit.v1",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	return &kafkaAudit{writer: w}, nil
}

func (k *kafkaAudit) Emit(ctx context.Context, ev AuditEvent, key string) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	payloadHash := "sha256:" + hex.EncodeToString(sum[:])
	id := key
	if id == "" {
		id = uuid.NewString()
	}
	envelope := map[string]any{
		"schema_version": "1",
		"id":             id,
		"ts":             time.Now().UTC().Format(time.RFC3339Nano),
		"source_service": "treasury-orchestration",
		"actor_id":       coalesceStr(ev.Actor, "treasury-orchestration"),
		"action":         ev.EventType,
		"target_type":    "batch",
		"target_id":      ev.Aggregate,
		"payload_hash":   payloadHash,
		"payload":        json.RawMessage(payload),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return k.writer.WriteMessages(ctx, kafka.Message{Key: []byte(id), Value: body})
}

func (k *kafkaAudit) Close() error {
	if k.writer == nil {
		return nil
	}
	return k.writer.Close()
}

func coalesceStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// FakeAudit is a test double for AuditLog.
type FakeAudit struct {
	mu     sync.Mutex
	calls  []AuditEvent
	keys   map[string]bool
	err    error
	dupErr bool
}

// NewFakeAudit returns a fake audit-event-log client.
func NewFakeAudit() *FakeAudit {
	return &FakeAudit{keys: map[string]bool{}}
}

// SetError configures the next call to return err.
func (f *FakeAudit) SetError(err error) { f.err = err }

// SetDuplicateKeyError makes Emit return ErrConflict on duplicate keys.
func (f *FakeAudit) SetDuplicateKeyError(on bool) { f.dupErr = on }

// Emit implements AuditLog.
func (f *FakeAudit) Emit(_ context.Context, ev AuditEvent, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ev)
	if f.err != nil {
		err := f.err
		f.err = nil
		return err
	}
	if f.dupErr && f.keys[key] {
		return ErrConflict
	}
	f.keys[key] = true
	return nil
}

// Calls returns the recorded events.
func (f *FakeAudit) Calls() []AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]AuditEvent, len(f.calls))
	copy(out, f.calls)
	return out
}

// unused guards
var (
	_ = errors.New
	_ = strconv.Itoa
)
