package aggregate

import (
	"context"
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func TestExecutor_SubmitsAndPersistsFill(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	fill := clients.FillResult{FillPrice: decimal.NewFromInt(50100), TotalFilled: decimal.NewFromInt(1), VenueRoutes: []clients.VenueRoute{{Venue: "v1", Share: decimal.NewFromInt(1), Price: decimal.NewFromInt(50100)}}}
	liq := clients.NewFakeLiquidity(fill)
	var onFillCalled bool
	ex := New(Deps{
		Batches:          all.Batch,
		Orders:           all.Order,
		Liquidity:        liq,
		Idem:             idem,
		ExpectedPriceFor: func(string) decimal.Decimal { return decimal.NewFromInt(50000) },
		OnFill: func(ctx context.Context, b *store.Batch, o *store.AggregateOrder) {
			onFillCalled = true
		},
	})
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// Close the batch first.
	closed, _, _ := all.Batch.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchClosed, func(x *store.Batch) { x.NotionalUSD = decimal.NewFromInt(50000) })
	order, err := ex.SubmitBatch(ctx, closed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !order.FillPrice.Equal(decimal.NewFromInt(50100)) {
		t.Fatalf("fill_price=%s want 50100", order.FillPrice.String())
	}
	if !order.TotalFilled.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("total_filled=%s want 1", order.TotalFilled.String())
	}
	if !onFillCalled {
		t.Fatal("expected OnFill callback")
	}
	calls := liq.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 liquidity call, got %d", len(calls))
	}
	if calls[0].AssetPair != "BTC/USD" {
		t.Fatalf("pair=%s want BTC/USD", calls[0].AssetPair)
	}
}

func TestExecutor_IdempotentReplay(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	fill := clients.FillResult{FillPrice: decimal.NewFromInt(50000), TotalFilled: decimal.NewFromInt(1)}
	liq := clients.NewFakeLiquidity(fill)
	ex := New(Deps{
		Batches:   all.Batch,
		Orders:    all.Order,
		Liquidity: liq,
		Idem:      idem,
	})
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	closed, _, _ := all.Batch.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchClosed, func(x *store.Batch) { x.NotionalUSD = decimal.NewFromInt(50000) })
	if _, err := ex.SubmitBatch(ctx, closed.ID); err != nil {
		t.Fatal(err)
	}
	// Replay: should not call liquidity again.
	if _, err := ex.SubmitBatch(ctx, closed.ID); err != nil {
		t.Fatal(err)
	}
	calls := liq.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 liquidity call after replay, got %d", len(calls))
	}
}

func TestExecutor_RequiresClosedBatch(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	ex := New(Deps{
		Batches:   all.Batch,
		Orders:    all.Order,
		Liquidity: clients.NewFakeLiquidity(clients.FillResult{}),
		Idem:      idempotency.NewMem(),
	})
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	if _, err := ex.SubmitBatch(ctx, b.ID); err == nil {
		t.Fatal("expected error for non-closed batch")
	}
}

func TestExecutor_LiquidityErrorPropagates(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	liq := clients.NewFakeLiquidity(clients.FillResult{})
	liq.SetError(errors.New("boom"))
	ex := New(Deps{
		Batches:   all.Batch,
		Orders:    all.Order,
		Liquidity: liq,
		Idem:      idem,
	})
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	closed, _, _ := all.Batch.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchClosed, func(x *store.Batch) { x.NotionalUSD = decimal.NewFromInt(50000) })
	// First call fails. Note: idem key is marked before the call, so a
	// second attempt would be skipped. To test propagation we delete the
	// idem key first — but the order row was already created (executing),
	// so the second SubmitBatch returns the existing executing order
	// without re-submitting. We verify the error surfaces on first call.
	_, err := ex.SubmitBatch(ctx, closed.ID)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom, got %v", err)
	}
}
