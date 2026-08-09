package memstore

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

func TestBatchStore_OpenAndTransition(t *testing.T) {
	ctx := context.Background()
	s := NewBatchStore()
	b, err := s.OpenBatch(ctx, "BTC/USD")
	if err != nil {
		t.Fatal(err)
	}
	if b.Status != store.BatchOpen {
		t.Fatalf("expected open, got %s", b.Status)
	}
	// Idempotent: open returns the same open batch.
	b2, _ := s.OpenBatch(ctx, "BTC/USD")
	if b2.ID != b.ID {
		t.Fatalf("expected same batch id, got %s vs %s", b2.ID, b.ID)
	}
	// Transition open -> closed.
	updated, ok, err := s.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchClosed, func(x *store.Batch) { x.NotionalUSD = decimal.NewFromInt(100) })
	if err != nil || !ok {
		t.Fatalf("transition open->closed: %v ok=%v", err, ok)
	}
	if updated.Status != store.BatchClosed {
		t.Fatalf("expected closed, got %s", updated.Status)
	}
	if !updated.NotionalUSD.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("expected notional 100, got %s", updated.NotionalUSD.String())
	}
	if updated.ClosedAt.IsZero() {
		t.Fatal("expected closed_at set")
	}
	// Invalid transition closed -> settled (wrong from-status returns no-op).
	_, ok2, err := s.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchSettled, nil)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if ok2 {
		t.Fatal("expected no-op on from mismatch")
	}
}

func TestMembershipStore_DedupByTxID(t *testing.T) {
	ctx := context.Background()
	bs := NewBatchStore()
	b, _ := bs.OpenBatch(ctx, "ETH/USD")
	ms := NewMembershipStore()
	m := &store.Membership{BatchID: b.ID, TxID: "tx1", Amount: decimal.NewFromInt(1), Asset: "ETH", FiatCurrency: "USD", NotionalUSD: decimal.NewFromInt(2000)}
	ok, err := ms.AddMembership(ctx, m)
	if err != nil || !ok {
		t.Fatalf("first add: %v ok=%v", err, ok)
	}
	ok, err = ms.AddMembership(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected dup add to return false")
	}
	exists, _ := ms.ExistsByTxID(ctx, "tx1")
	if !exists {
		t.Fatal("expected exists")
	}
	sum, _ := ms.SumNotional(ctx, b.ID)
	if !sum.Equal(decimal.NewFromInt(2000)) {
		t.Fatalf("sum=%s want 2000", sum.String())
	}
}

func TestAggregateOrderStore_CreateIdempotent(t *testing.T) {
	ctx := context.Background()
	s := NewAggregateOrderStore()
	batchID, _ := uuid.NewV7()
	o := &store.AggregateOrder{BatchID: batchID, AssetPair: "BTC/USD", NotionalUSD: decimal.NewFromInt(50000)}
	a, err := s.CreateOrder(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != store.AggregateExecuting {
		t.Fatalf("expected executing, got %s", a.Status)
	}
	b, _ := s.CreateOrder(ctx, o)
	if b.ID != a.ID {
		t.Fatalf("expected idempotent create, got %s vs %s", b.ID, a.ID)
	}
	updated, _ := s.UpdateOrderFill(ctx, batchID, decimal.NewFromInt(50001), decimal.NewFromInt(1), []store.VenueRoute{{Venue: "v1", Share: decimal.NewFromInt(1), Price: decimal.NewFromInt(50001)}})
	if !updated.FillPrice.Equal(decimal.NewFromInt(50001)) {
		t.Fatalf("fill_price=%s want 50001", updated.FillPrice.String())
	}
	settled, _ := s.SettleOrder(ctx, batchID, decimal.NewFromInt(40000))
	if settled.Status != store.AggregateSettled {
		t.Fatalf("expected settled, got %s", settled.Status)
	}
	if !settled.HedgedNotional.Equal(decimal.NewFromInt(40000)) {
		t.Fatalf("hedged=%s want 40000", settled.HedgedNotional.String())
	}
}

func TestFloatStore_AddAndSettle(t *testing.T) {
	ctx := context.Background()
	s := NewFloatStore()
	due := time.Now().UTC().Add(48 * time.Hour)
	p, err := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1000), LongCryptoAmount: decimal.NewFromFloat(0.02), LongCryptoAsset: "BTC", SettlementDueAt: due})
	if err != nil {
		t.Fatal(err)
	}
	if !p.ShortFiatAmount.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("short=%s want 1000", p.ShortFiatAmount.String())
	}
	// Accumulate into the same row.
	p2, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(500), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC"})
	if p2.ID != p.ID {
		t.Fatalf("expected same row, got %s vs %s", p2.ID, p.ID)
	}
	if !p2.ShortFiatAmount.Equal(decimal.NewFromInt(1500)) {
		t.Fatalf("short=%s want 1500", p2.ShortFiatAmount.String())
	}
	// GetFloat aggregates.
	agg, _ := s.GetFloat(ctx, "USD")
	if !agg.ShortFiatAmount.Equal(decimal.NewFromInt(1500)) {
		t.Fatalf("agg short=%s want 1500", agg.ShortFiatAmount.String())
	}
	// Matured list (due in the past).
	before := time.Now().UTC()
	past, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "EUR", ShortFiatAmount: decimal.NewFromInt(100), LongCryptoAmount: decimal.NewFromFloat(0.001), LongCryptoAsset: "BTC", SettlementDueAt: before.Add(-1 * time.Hour)})
	matured, _ := s.ListMaturedFloat(ctx, before.Add(time.Minute))
	if len(matured) != 1 || matured[0].ID != past.ID {
		t.Fatalf("expected 1 matured, got %v", matured)
	}
	if _, err := s.SettleFloat(ctx, past.ID); err != nil {
		t.Fatal(err)
	}
	matured2, _ := s.ListMaturedFloat(ctx, time.Now().Add(time.Hour))
	if len(matured2) != 0 {
		t.Fatalf("expected 0 matured after settle, got %d", len(matured2))
	}
}

func TestOutboxStore_Dedup(t *testing.T) {
	ctx := context.Background()
	s := NewOutboxStore()
	e := &store.OutboxEntry{Aggregate: "batch", EventType: "batch.close", DedupKey: "batch.close:1", Payload: []byte("{}")}
	ok, _ := s.Append(ctx, e)
	if !ok {
		t.Fatal("expected first append ok")
	}
	ok, _ = s.Append(ctx, e)
	if ok {
		t.Fatal("expected dup append false")
	}
	pending, _ := s.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}
	if err := s.MarkEmitted(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	pending2, _ := s.ListPending(ctx, 10)
	if len(pending2) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending2))
	}
}

func TestNewAll_WiresAllStores(t *testing.T) {
	all := NewAll()
	if all.Batch == nil || all.Membership == nil || all.Order == nil ||
		all.Funding == nil || all.Float == nil || all.Rebalance == nil || all.Outbox == nil {
		t.Fatal("NewAll left a store nil")
	}
}

// TestAll_UpdateBatchStatusWithOutbox verifies the in-memory Unit
// transitions the batch and appends the outbox event together: a
// successful transition must persist exactly one outbox entry, and a
// no-op transition (wrong from-status) must append nothing.
func TestAll_UpdateBatchStatusWithOutbox(t *testing.T) {
	ctx := context.Background()
	all := NewAll()
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	if _, _, err := all.Batch.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchClosed, nil); err != nil {
		t.Fatalf("setup close: %v", err)
	}
	ent := &store.OutboxEntry{Aggregate: "aggregate", EventType: "aggregate.execute", DedupKey: "k1", Payload: []byte("{}")}
	_, ok, err := all.UpdateBatchStatusWithOutbox(ctx, b.ID, store.BatchClosed, store.BatchExecuting, nil, []*store.OutboxEntry{ent})
	if err != nil || !ok {
		t.Fatalf("transition+outbox: ok=%v err=%v", ok, err)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(pending))
	}

	// No-op transition (already advanced) must not append a duplicate.
	ent2 := &store.OutboxEntry{Aggregate: "aggregate", EventType: "aggregate.execute", DedupKey: "k2", Payload: []byte("{}")}
	_, ok2, err := all.UpdateBatchStatusWithOutbox(ctx, b.ID, store.BatchClosed, store.BatchExecuting, nil, []*store.OutboxEntry{ent2})
	if err != nil || ok2 {
		t.Fatalf("no-op transition: ok=%v err=%v", ok2, err)
	}
	pending, _ = all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("outbox entries after no-op=%d want 1", len(pending))
	}
}

func TestAll_OpenBatchWithOutbox_AppendsOnlyOnCreation(t *testing.T) {
	ctx := context.Background()
	all := NewAll()
	buildEvents := func(b *store.Batch) []*store.OutboxEntry {
		return []*store.OutboxEntry{{Aggregate: "batch", EventType: "batch.open", DedupKey: "batch.open:" + b.ID.String(), Payload: []byte("{}")}}
	}
	b1, err := all.OpenBatchWithOutbox(ctx, "BTC/USD", buildEvents)
	if err != nil {
		t.Fatal(err)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("after first open: outbox entries=%d want 1", len(pending))
	}
	// Reuse the existing open batch: buildEvents must NOT be invoked and
	// no new outbox entry must be appended.
	b2, err := all.OpenBatchWithOutbox(ctx, "BTC/USD", buildEvents)
	if err != nil {
		t.Fatal(err)
	}
	if b2.ID != b1.ID {
		t.Fatalf("expected reuse of open batch, got %s vs %s", b2.ID, b1.ID)
	}
	pending, _ = all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("after reuse: outbox entries=%d want 1 (no append on reuse)", len(pending))
	}
}

func TestAll_AddFloatWithOutbox_AppendsEntries(t *testing.T) {
	ctx := context.Background()
	all := NewAll()
	events := []*store.OutboxEntry{{Aggregate: "float", EventType: "float.adjust", DedupKey: "fa1", Payload: []byte("{}")}}
	if _, err := all.AddFloatWithOutbox(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(100), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC"}, events); err != nil {
		t.Fatal(err)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(pending))
	}
	pos, _ := all.Float.GetFloat(ctx, "USD")
	if !pos.ShortFiatAmount.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("short=%s want 100", pos.ShortFiatAmount.String())
	}
}

func TestAll_CreateFundingWithOutbox_AppendsEntries(t *testing.T) {
	ctx := context.Background()
	all := NewAll()
	events := []*store.OutboxEntry{{Aggregate: "funding", EventType: "funding.create", DedupKey: "fc1", Payload: []byte("{}")}}
	fr, err := all.CreateFundingWithOutbox(ctx, &store.FundingRequest{WalletID: "w", Asset: "BTC", Amount: decimal.NewFromInt(5)}, events)
	if err != nil {
		t.Fatal(err)
	}
	if fr.ID == uuid.Nil {
		t.Fatal("expected funding id assigned")
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(pending))
	}
}

func TestBatchStore_GetListAndNotional(t *testing.T) {
	ctx := context.Background()
	s := NewBatchStore()
	// NotFound path.
	if _, err := s.GetBatch(ctx, uuid.New()); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	b1, _ := s.OpenBatch(ctx, "BTC/USD")
	b2, _ := s.OpenBatch(ctx, "ETH/USD")
	// Close b1.
	if _, _, err := s.UpdateBatchStatus(ctx, b1.ID, store.BatchOpen, store.BatchClosed, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBatch(ctx, b1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.BatchClosed {
		t.Fatalf("expected closed, got %s", got.Status)
	}
	// ListBatches with from filter excludes b2 (opened after from).
	now := time.Now().UTC().Add(time.Second)
	list, err := s.ListBatches(ctx, now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("expected 0 with future from filter, got %d", len(list))
	}
	// ListBatches with to filter excludes nothing when to is zero.
	list, _ = s.ListBatches(ctx, time.Time{}, time.Time{})
	if len(list) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(list))
	}
	// ListBatches with past to excludes both (both opened after to).
	list, _ = s.ListBatches(ctx, time.Time{}, time.Now().UTC().Add(-time.Hour))
	if len(list) != 0 {
		t.Fatalf("expected 0 with past to filter, got %d", len(list))
	}
	// ListOpenBatches returns only open batches.
	open, _ := s.ListOpenBatches(ctx)
	if len(open) != 1 || open[0].ID != b2.ID {
		t.Fatalf("expected 1 open (b2), got %+v", open)
	}
	// SetBatchNotional.
	if err := s.SetBatchNotional(ctx, b1.ID, decimal.NewFromInt(1234)); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.GetBatch(ctx, b1.ID)
	if !got2.NotionalUSD.Equal(decimal.NewFromInt(1234)) {
		t.Fatalf("notional=%s want 1234", got2.NotionalUSD.String())
	}
	if err := s.SetBatchNotional(ctx, uuid.New(), decimal.NewFromInt(1)); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestBatchStore_InvalidTransitionConflict(t *testing.T) {
	ctx := context.Background()
	s := NewBatchStore()
	b, _ := s.OpenBatch(ctx, "BTC/USD")
	// open -> settled is not allowed (CanTransitionTo false) -> ErrConflict.
	_, ok, err := s.UpdateBatchStatus(ctx, b.ID, store.BatchOpen, store.BatchSettled, nil)
	if err != store.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v ok=%v", err, ok)
	}
	if ok {
		t.Fatal("expected ok=false on conflict")
	}
	// Unknown id -> ErrNotFound.
	_, ok2, err := s.UpdateBatchStatus(ctx, uuid.New(), store.BatchOpen, store.BatchClosed, nil)
	if err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v ok=%v", err, ok2)
	}
	if ok2 {
		t.Fatal("expected ok=false on missing id")
	}
}

func TestMembershipStore_ListAndMissing(t *testing.T) {
	ctx := context.Background()
	bs := NewBatchStore()
	b, _ := bs.OpenBatch(ctx, "ETH/USD")
	ms := NewMembershipStore()
	// ListMemberships empty for unknown batch.
	list, _ := ms.ListMemberships(ctx, uuid.New())
	if len(list) != 0 {
		t.Fatalf("expected 0, got %d", len(list))
	}
	_, _ = ms.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "a", NotionalUSD: decimal.NewFromInt(10)})
	_, _ = ms.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "b", NotionalUSD: decimal.NewFromInt(30)})
	got, _ := ms.ListMemberships(ctx, b.ID)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
	if got[0].ID.String() > got[1].ID.String() {
		t.Fatal("expected sorted by id")
	}
	// SumNotional on unknown batch returns 0.
	if sum, _ := ms.SumNotional(ctx, uuid.New()); !sum.Equal(decimal.Zero) {
		t.Fatalf("expected 0 sum, got %s", sum.String())
	}
	// ExistsByTxID false for unknown.
	if ex, _ := ms.ExistsByTxID(ctx, "nope"); ex {
		t.Fatal("expected false")
	}
}

func TestAggregateOrderStore_NotFoundPaths(t *testing.T) {
	ctx := context.Background()
	s := NewAggregateOrderStore()
	unknownBatch, _ := uuid.NewV7()
	if _, err := s.GetOrderByBatch(ctx, unknownBatch); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, err := s.UpdateOrderFill(ctx, unknownBatch, decimal.NewFromInt(1), decimal.NewFromInt(1), nil); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, err := s.SettleOrder(ctx, unknownBatch, decimal.NewFromInt(1)); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// CreateOrder with defaults applied when side/status empty.
	o, _ := s.CreateOrder(ctx, &store.AggregateOrder{BatchID: unknownBatch})
	if o.Side != "BUY" || o.Status != store.AggregateExecuting {
		t.Fatalf("defaults wrong: side=%s status=%s", o.Side, o.Status)
	}
	// GetOrderByBatch returns the created order.
	g, _ := s.GetOrderByBatch(ctx, unknownBatch)
	if g.ID != o.ID {
		t.Fatalf("expected same id, got %s vs %s", g.ID, o.ID)
	}
}

func TestFundingStore_CRUD(t *testing.T) {
	ctx := context.Background()
	s := NewFundingStore()
	// GetFunding on empty.
	if _, err := s.GetFunding(ctx, uuid.New()); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// Create with explicit status keeps it.
	f0, _ := s.CreateFunding(ctx, &store.FundingRequest{Asset: "BTC", Amount: decimal.NewFromInt(1), Status: store.FundingExecuting})
	if f0.ID == uuid.Nil || f0.Status != store.FundingExecuting {
		t.Fatalf("unexpected f0=%+v", f0)
	}
	// Create with empty status defaults to pending.
	f1, _ := s.CreateFunding(ctx, &store.FundingRequest{Asset: "ETH", Amount: decimal.NewFromInt(2)})
	if f1.Status != store.FundingPending {
		t.Fatalf("expected pending default, got %s", f1.Status)
	}
	// GetFunding found.
	g, _ := s.GetFunding(ctx, f1.ID)
	if g.ID != f1.ID {
		t.Fatalf("expected same id, got %s vs %s", g.ID, f1.ID)
	}
	// UpdateFundingStatus sets CompletedAt for terminal statuses.
	if err := s.UpdateFundingStatus(ctx, f1.ID, store.FundingCompleted); err != nil {
		t.Fatal(err)
	}
	upd, _ := s.GetFunding(ctx, f1.ID)
	if upd.Status != store.FundingCompleted || upd.CompletedAt.IsZero() {
		t.Fatalf("completed state wrong: %+v", upd)
	}
	// UpdateFundingStatus rejected also sets CompletedAt.
	if err := s.UpdateFundingStatus(ctx, f0.ID, store.FundingRejected); err != nil {
		t.Fatal(err)
	}
	upd0, _ := s.GetFunding(ctx, f0.ID)
	if upd0.Status != store.FundingRejected || upd0.CompletedAt.IsZero() {
		t.Fatalf("rejected state wrong: %+v", upd0)
	}
	// UpdateFundingStatus on executing does not set CompletedAt.
	f2, _ := s.CreateFunding(ctx, &store.FundingRequest{Asset: "ETH", Amount: decimal.NewFromInt(3)})
	if err := s.UpdateFundingStatus(ctx, f2.ID, store.FundingExecuting); err != nil {
		t.Fatal(err)
	}
	upd2, _ := s.GetFunding(ctx, f2.ID)
	if !upd2.CompletedAt.IsZero() {
		t.Fatal("expected zero CompletedAt for executing")
	}
	// UpdateFundingStatus not found.
	if err := s.UpdateFundingStatus(ctx, uuid.New(), store.FundingCompleted); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// ListFunding filter by status.
	list, _ := s.ListFunding(ctx, string(store.FundingRejected))
	if len(list) != 1 || list[0].ID != f0.ID {
		t.Fatalf("expected 1 rejected, got %+v", list)
	}
	all, _ := s.ListFunding(ctx, "")
	if len(all) != 3 {
		t.Fatalf("expected 3 total, got %d", len(all))
	}
}

func TestFloatStore_EdgeCases(t *testing.T) {
	ctx := context.Background()
	s := NewFloatStore()
	// GetFloat on empty returns zero position.
	g, err := s.GetFloat(ctx, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if !g.ShortFiatAmount.Equal(decimal.Zero) || !g.LongCryptoAmount.Equal(decimal.Zero) {
		t.Fatalf("expected zero float, got %+v", g)
	}
	// ListMaturedFloat on empty.
	m, _ := s.ListMaturedFloat(ctx, time.Now().Add(time.Hour))
	if len(m) != 0 {
		t.Fatalf("expected 0 matured, got %d", len(m))
	}
	// AddFloat without due date (accumulation path with no settlement update).
	p1, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(100), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC"})
	p2, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(50), LongCryptoAmount: decimal.NewFromFloat(0.005), LongCryptoAsset: ""})
	if p2.ID != p1.ID {
		t.Fatalf("expected accumulation into same row, got %s vs %s", p2.ID, p1.ID)
	}
	// SettleFloat not found.
	if _, err := s.SettleFloat(ctx, uuid.New()); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// Settled row accumulates into a NEW row instead (since old is settled).
	if _, err := s.SettleFloat(ctx, p1.ID); err != nil {
		t.Fatal(err)
	}
	p3, _ := s.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(20), LongCryptoAmount: decimal.NewFromFloat(0.002), LongCryptoAsset: "BTC"})
	if p3.ID == p1.ID {
		t.Fatal("expected new row after settle, got same id")
	}
	// GetFloat aggregates both settled and unsettled rows.
	agg, _ := s.GetFloat(ctx, "USD")
	if !agg.ShortFiatAmount.Equal(decimal.NewFromInt(170)) {
		t.Fatalf("agg short=%s want 170", agg.ShortFiatAmount.String())
	}
}

func TestRebalancingStore_CRUD(t *testing.T) {
	ctx := context.Background()
	s := NewRebalancingStore()
	// ListJobs empty.
	list, _ := s.ListJobs(ctx, "")
	if len(list) != 0 {
		t.Fatalf("expected 0, got %d", len(list))
	}
	// Create with default status.
	j0, _ := s.CreateJob(ctx, &store.RebalancingJob{FromRef: "w1", ToRef: "w2", Asset: "BTC", Amount: decimal.NewFromInt(100)})
	if j0.ID == uuid.Nil || j0.Status != store.RebalancePending {
		t.Fatalf("unexpected j0=%+v", j0)
	}
	// Create with explicit status keeps it.
	j1, _ := s.CreateJob(ctx, &store.RebalancingJob{FromRef: "w3", ToRef: "w4", Asset: "ETH", Amount: decimal.NewFromInt(50), Status: store.RebalanceExecuting})
	if j1.Status != store.RebalanceExecuting {
		t.Fatalf("expected executing kept, got %s", j1.Status)
	}
	// UpdateJobStatus completed sets CompletedAt.
	if err := s.UpdateJobStatus(ctx, j0.ID, store.RebalanceCompleted); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateJobStatus(ctx, j1.ID, store.RebalanceRejected); err != nil {
		t.Fatal(err)
	}
	// UpdateJobStatus executing does not set CompletedAt.
	j2, _ := s.CreateJob(ctx, &store.RebalancingJob{FromRef: "w5", ToRef: "w6", Asset: "SOL", Amount: decimal.NewFromInt(5)})
	if err := s.UpdateJobStatus(ctx, j2.ID, store.RebalanceExecuting); err != nil {
		t.Fatal(err)
	}
	// ListJobs filter by status.
	completed, _ := s.ListJobs(ctx, string(store.RebalanceCompleted))
	if len(completed) != 1 || completed[0].ID != j0.ID {
		t.Fatalf("expected 1 completed, got %+v", completed)
	}
	all, _ := s.ListJobs(ctx, "")
	if len(all) != 3 {
		t.Fatalf("expected 3 total, got %d", len(all))
	}
	// UpdateJobStatus not found.
	if err := s.UpdateJobStatus(ctx, uuid.New(), store.RebalanceCompleted); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestOutboxStore_EdgeCases(t *testing.T) {
	ctx := context.Background()
	s := NewOutboxStore()
	// ListPending on empty.
	p, _ := s.ListPending(ctx, 5)
	if len(p) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(p))
	}
	// MarkEmitted not found.
	if err := s.MarkEmitted(ctx, uuid.New()); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// ListPending limit behavior.
	for i := 0; i < 3; i++ {
		_, _ = s.Append(ctx, &store.OutboxEntry{Aggregate: "batch", EventType: "e", DedupKey: "k" + string(rune('a'+i)), Payload: []byte("{}")})
	}
	lim, _ := s.ListPending(ctx, 2)
	if len(lim) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(lim))
	}
	// Snapshot helper returns all rows (including emitted).
	_, _ = s.Append(ctx, &store.OutboxEntry{Aggregate: "batch", EventType: "e", DedupKey: "snap", Payload: []byte("{}")})
	snap := s.Snapshot()
	if len(snap) != 4 {
		t.Fatalf("expected 4 snapshot rows, got %d", len(snap))
	}
}
