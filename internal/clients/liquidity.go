package clients

import (
	"context"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// VenueRoute describes one slice of an aggregate fill returned by
// liquidity-routing. Mirrors store.VenueRoute but kept here to avoid an
// import cycle with the store package in the clients layer.
type VenueRoute struct {
	Venue string          `json:"venue"`
	Share decimal.Decimal `json:"share"`
	Price decimal.Decimal `json:"price"`
}

// FillResult is the aggregate parent-order fill returned by
// liquidity-routing.
type FillResult struct {
	FillPrice   decimal.Decimal `json:"fill_price"`
	TotalFilled decimal.Decimal `json:"total_filled"`
	VenueRoutes []VenueRoute    `json:"venue_routes"`
}

// LiquidityRouting is the client interface for the liquidity-routing
// service.
type LiquidityRouting interface {
	// SubmitAggregate submits an aggregate parent buy order. The
	// idempotencyKey is sent so replays are safe on the server side.
	SubmitAggregate(ctx context.Context, req AggregateOrderRequest, idempotencyKey string) (*FillResult, error)
}

// AggregateOrderRequest is the parent-order payload sent to
// liquidity-routing.
type AggregateOrderRequest struct {
	AssetPair   string          `json:"asset_pair"`
	Side        string          `json:"side"`
	NotionalUSD decimal.Decimal `json:"notional_usd"`
	TotalTarget decimal.Decimal `json:"total_target"`
}

// --- Fake ---

// FakeLiquidityRouting is a test double that returns a configurable fill.
type FakeLiquidityRouting struct {
	mu        sync.Mutex
	calls     []AggregateOrderRequest
	keys      map[string]bool
	fill      FillResult
	err       error
	keyDupErr bool
}

// NewFakeLiquidity returns a fake that returns the given fill.
func NewFakeLiquidity(fill FillResult) *FakeLiquidityRouting {
	return &FakeLiquidityRouting{keys: map[string]bool{}, fill: fill}
}

// SetError configures the fake to return err on the next call.
func (f *FakeLiquidityRouting) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// SetFill updates the configured fill result.
func (f *FakeLiquidityRouting) SetFill(fill FillResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fill = fill
}

// SubmitAggregate implements LiquidityRouting.
func (f *FakeLiquidityRouting) SubmitAggregate(_ context.Context, req AggregateOrderRequest, key string) (*FillResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	if f.keys[key] && f.keyDupErr {
		return nil, ErrConflict
	}
	f.keys[key] = true
	fill := f.fill
	if fill.VenueRoutes == nil {
		fill.VenueRoutes = []VenueRoute{{Venue: "primary", Share: decimal.NewFromInt(1), Price: fill.FillPrice}}
	}
	return &fill, nil
}

// Calls returns the recorded requests (test helper).
func (f *FakeLiquidityRouting) Calls() []AggregateOrderRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]AggregateOrderRequest, len(f.calls))
	copy(out, f.calls)
	return out
}

// SetDuplicateKeyError makes the fake return ErrConflict on a duplicate
// idempotency key.
func (f *FakeLiquidityRouting) SetDuplicateKeyError(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keyDupErr = on
}

// --- HTTP impl ---

type httpLiquidity struct {
	baseURL string
	client  *timeoutClient
}

// NewHTTPLiquidity returns an HTTP-backed liquidity-routing client.
func NewHTTPLiquidity(baseURL string) LiquidityRouting {
	return &httpLiquidity{baseURL: baseURL, client: newTimeoutClient(10 * time.Second)}
}
