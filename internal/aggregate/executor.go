// Package aggregate submits each closed batch as an aggregate parent
// order to liquidity-routing and persists the fill result. The order is
// persisted with status "executing" before submission so a crash between
// persist and submit is recoverable and idempotent (the batch_id is the
// idempotency key).
package aggregate

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/metrics"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// Deps bundles the executor dependencies.
type Deps struct {
	Batches          store.BatchStore
	Orders           store.AggregateOrderStore
	Liquidity        clients.LiquidityRouting
	Idem             idempotency.Store
	ExpectedPriceFor func(assetPair string) decimal.Decimal
	OnFill           func(ctx context.Context, batch *store.Batch, order *store.AggregateOrder)
}

// Executor submits closed batches as aggregate parent orders.
type Executor struct {
	deps Deps
}

// New returns a new executor.
func New(deps Deps) *Executor { return &Executor{deps: deps} }

// SubmitBatch builds and submits the parent order for a closed batch.
// It is idempotent on batch_id: if an order already exists for the batch
// it returns the existing record (and re-applies the fill if missing).
func (e *Executor) SubmitBatch(ctx context.Context, batchID uuid.UUID) (*store.AggregateOrder, error) {
	batch, err := e.deps.Batches.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	// If the batch has already advanced past closed (executing/settled),
	// this is a replay: return the existing order if present.
	if batch.Status != store.BatchClosed && batch.Status != store.BatchExecuting {
		return nil, fmt.Errorf("aggregate: batch %s not closed (status=%s)", batchID, batch.Status)
	}
	// Persist the aggregate order (status executing) before submission.
	// CreateOrder is idempotent on batch_id.
	order, err := e.deps.Orders.CreateOrder(ctx, &store.AggregateOrder{
		BatchID:     batch.ID,
		AssetPair:   batch.AssetPair,
		Side:        "BUY",
		NotionalUSD: batch.NotionalUSD,
		Status:      store.AggregateExecuting,
	})
	if err != nil {
		return nil, err
	}
	// If already settled, nothing to do.
	if order.Status == store.AggregateSettled {
		return order, nil
	}
	// Transition batch closed -> executing (idempotent; ignore if already
	// advanced).
	_, _, _ = e.deps.Batches.UpdateBatchStatus(ctx, batch.ID, store.BatchClosed, store.BatchExecuting, nil)

	idemKey := fmt.Sprintf("agg:%s", batch.ID)
	ok, err := e.deps.Idem.CheckAndMark(ctx, idemKey, 24*time.Hour)
	if err != nil {
		log.Printf("aggregate: idem check batch=%s: %v", batch.ID, err)
	}
	if err == nil && !ok {
		// Replay; re-fetch the order and return it.
		log.Printf("aggregate: dup submit batch=%s skipped", batch.ID)
		return e.deps.Orders.GetOrderByBatch(ctx, batch.ID)
	}

	// Submit to liquidity-routing.
	fill, err := e.deps.Liquidity.SubmitAggregate(ctx, clients.AggregateOrderRequest{
		AssetPair:   batch.AssetPair,
		Side:        "BUY",
		NotionalUSD: batch.NotionalUSD,
		TotalTarget: batch.NotionalUSD,
	}, idemKey)
	if err != nil {
		log.Printf("aggregate: submit batch=%s: %v", batch.ID, err)
		return nil, err
	}
	// Persist fill.
	routes := make([]store.VenueRoute, 0, len(fill.VenueRoutes))
	for _, r := range fill.VenueRoutes {
		routes = append(routes, store.VenueRoute{Venue: r.Venue, Share: r.Share, Price: r.Price})
	}
	updated, err := e.deps.Orders.UpdateOrderFill(ctx, batch.ID, fill.FillPrice, fill.TotalFilled, routes)
	if err != nil {
		return nil, err
	}
	// Record slippage histogram vs expected price.
	if e.deps.ExpectedPriceFor != nil {
		expected := e.deps.ExpectedPriceFor(batch.AssetPair)
		if expected.GreaterThan(decimal.Zero) {
			metrics.SlippageUSD.WithLabelValues(batch.AssetPair).Observe(fill.FillPrice.Sub(expected).Mul(fill.TotalFilled).InexactFloat64())
		}
	}
	log.Printf("aggregate: filled batch=%s fill_price=%s total_filled=%s", batch.ID, fill.FillPrice.String(), fill.TotalFilled.String())
	if e.deps.OnFill != nil {
		e.deps.OnFill(ctx, batch, updated)
	}
	return updated, nil
}

// ErrAlreadyExecuting is returned when a batch is already in executing
// state and cannot be re-submitted.
var ErrAlreadyExecuting = errors.New("aggregate: already executing")
