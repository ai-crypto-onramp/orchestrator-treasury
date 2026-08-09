// Package postgres is normally exercised by the docker-compose integration
// suite. These unit tests cover the error/branch paths that do not require
// a running PostgreSQL: the DB accessors, the helper functions, and every
// store method's connection-error early return (wired against a lazily
// created pgxpool that points at an unreachable host with a short connect
// timeout). No real database is required.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// newUnreachableDB builds a *DB whose pool lazily connects to an
// unreachable host. The pool itself is created without error; the
// connection is only attempted on the first query/exec, which fails fast
// thanks to the short connect_timeout.
func newUnreachableDB(t *testing.T) *DB {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://nobody:nopw@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 2 * time.Second
	cfg.MaxConnLifetime = 2 * time.Second
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	d := &DB{pool: p}
	d.batch = &BatchStore{db: p}
	d.membership = &MembershipStore{db: p}
	d.order = &AggregateOrderStore{db: p}
	d.funding = &FundingStore{db: p}
	d.float = &FloatStore{db: p}
	d.rebalance = &RebalancingStore{db: p}
	d.outbox = &OutboxStore{db: p}
	return d
}

func TestDB_AccessorsAndClose(t *testing.T) {
	d := newUnreachableDB(t)
	if d.Batch() == nil {
		t.Fatal("Batch() nil")
	}
	if d.Membership() == nil {
		t.Fatal("Membership() nil")
	}
	if d.Order() == nil {
		t.Fatal("Order() nil")
	}
	if d.Funding() == nil {
		t.Fatal("Funding() nil")
	}
	if d.Float() == nil {
		t.Fatal("Float() nil")
	}
	if d.Rebalance() == nil {
		t.Fatal("Rebalance() nil")
	}
	if d.Outbox() == nil {
		t.Fatal("Outbox() nil")
	}
	if d.Pool() == nil {
		t.Fatal("Pool() nil")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestOpen_UnreachableReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := Open(ctx, "postgres://nobody:nopw@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Fatal("expected error for unreachable DB")
	}
}

func TestOpen_EmptyDSNReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// An empty DSN parses but pinging localhost postgres will fail.
	_, err := Open(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty DSN (no server)")
	}
}

// TestDB_AllStoreMethodsReturnErrors exercises every store method against
// the unreachable pool. Each call should return a non-nil error (the
// connection error) without blocking. This covers the early-return
// branches of all 30+ store methods.
func TestDB_AllStoreMethodsReturnErrors(t *testing.T) {
	d := newUnreachableDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id := uuid.New()

	// BatchStore
	if _, err := d.Batch().OpenBatch(ctx, "BTC/USD"); err == nil {
		t.Fatal("OpenBatch: expected error")
	}
	if _, err := d.Batch().GetBatch(ctx, id); err == nil {
		t.Fatal("GetBatch: expected error")
	}
	if _, err := d.Batch().ListBatches(ctx, time.Time{}, time.Time{}); err == nil {
		t.Fatal("ListBatches: expected error")
	}
	if _, err := d.Batch().ListBatches(ctx, time.Now(), time.Now()); err == nil {
		t.Fatal("ListBatches(with from/to): expected error")
	}
	if _, err := d.Batch().ListOpenBatches(ctx); err == nil {
		t.Fatal("ListOpenBatches: expected error")
	}
	if _, _, err := d.Batch().UpdateBatchStatus(ctx, id, store.BatchOpen, store.BatchClosed, nil); err == nil {
		t.Fatal("UpdateBatchStatus: expected error")
	}
	if _, _, err := d.UpdateBatchStatusWithOutbox(ctx, id, store.BatchOpen, store.BatchClosed, nil, []*store.OutboxEntry{{Aggregate: "batch", EventType: "batch.close", DedupKey: "k", Payload: []byte("{}")}}); err == nil {
		t.Fatal("UpdateBatchStatusWithOutbox: expected error")
	}
	if err := d.Batch().SetBatchNotional(ctx, id, decimal.NewFromInt(100)); err == nil {
		t.Fatal("SetBatchNotional: expected error")
	}

	// MembershipStore
	if _, err := d.Membership().AddMembership(ctx, &store.Membership{BatchID: id, TxID: "t"}); err == nil {
		t.Fatal("AddMembership: expected error")
	}
	if _, err := d.Membership().ListMemberships(ctx, id); err == nil {
		t.Fatal("ListMemberships: expected error")
	}
	if _, err := d.Membership().SumNotional(ctx, id); err == nil {
		t.Fatal("SumNotional: expected error")
	}
	if _, err := d.Membership().ExistsByTxID(ctx, "t"); err == nil {
		t.Fatal("ExistsByTxID: expected error")
	}

	// AggregateOrderStore — pass a populated order so the pre-DB branches
	// run.
	o := &store.AggregateOrder{
		BatchID:     id,
		AssetPair:   "BTC/USD",
		Side:        "BUY",
		NotionalUSD: decimal.NewFromInt(1000),
		Status:      store.AggregateExecuting,
		VenueRoutes: []store.VenueRoute{{Venue: "v", Share: decimal.NewFromInt(1), Price: decimal.NewFromInt(1)}},
	}
	if _, err := d.Order().CreateOrder(ctx, o); err == nil {
		t.Fatal("CreateOrder: expected error")
	}
	if _, err := d.Order().GetOrderByBatch(ctx, id); err == nil {
		t.Fatal("GetOrderByBatch: expected error")
	}
	if _, err := d.Order().ListOrders(ctx, ""); err == nil {
		t.Fatal("ListOrders: expected error")
	}
	if _, err := d.Order().ListOrders(ctx, string(store.AggregateSettled)); err == nil {
		t.Fatal("ListOrders(status): expected error")
	}
	if _, err := d.Order().UpdateOrderFill(ctx, id, decimal.NewFromInt(1), decimal.NewFromInt(1), []store.VenueRoute{{Venue: "v", Share: decimal.NewFromInt(1), Price: decimal.NewFromInt(1)}}); err == nil {
		t.Fatal("UpdateOrderFill: expected error")
	}
	if _, err := d.Order().SettleOrder(ctx, id, decimal.NewFromInt(1)); err == nil {
		t.Fatal("SettleOrder: expected error")
	}

	// FundingStore
	f := &store.FundingRequest{WalletID: "w", Asset: "BTC", Amount: decimal.NewFromInt(1), Status: store.FundingPending}
	if _, err := d.Funding().CreateFunding(ctx, f); err == nil {
		t.Fatal("CreateFunding: expected error")
	}
	if _, err := d.Funding().GetFunding(ctx, id); err == nil {
		t.Fatal("GetFunding: expected error")
	}
	if err := d.Funding().UpdateFundingStatus(ctx, id, store.FundingCompleted); err == nil {
		t.Fatal("UpdateFundingStatus: expected error")
	}
	if err := d.Funding().UpdateFundingStatus(ctx, id, store.FundingPending); err == nil {
		t.Fatal("UpdateFundingStatus(pending): expected error")
	}
	if _, err := d.Funding().ListFunding(ctx, ""); err == nil {
		t.Fatal("ListFunding: expected error")
	}
	if _, err := d.Funding().ListFunding(ctx, string(store.FundingCompleted)); err == nil {
		t.Fatal("ListFunding(status): expected error")
	}

	// FloatStore
	fp := &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC", SettlementDueAt: time.Now().Add(24 * time.Hour)}
	if _, err := d.Float().AddFloat(ctx, fp); err == nil {
		t.Fatal("AddFloat: expected error")
	}
	if _, err := d.Float().GetFloat(ctx, "USD"); err == nil {
		t.Fatal("GetFloat: expected error")
	}
	if _, err := d.Float().ListFloat(ctx); err == nil {
		t.Fatal("ListFloat: expected error")
	}
	if _, err := d.Float().ListMaturedFloat(ctx, time.Now()); err == nil {
		t.Fatal("ListMaturedFloat: expected error")
	}
	if _, err := d.Float().SettleFloat(ctx, id); err == nil {
		t.Fatal("SettleFloat: expected error")
	}

	// RebalancingStore
	j := &store.RebalancingJob{FromRef: "a", ToRef: "b", Asset: "BTC", Amount: decimal.NewFromInt(1), Status: store.RebalancePending, Reason: "drift"}
	if _, err := d.Rebalance().CreateJob(ctx, j); err == nil {
		t.Fatal("CreateJob: expected error")
	}
	if _, err := d.Rebalance().ListJobs(ctx, ""); err == nil {
		t.Fatal("ListJobs: expected error")
	}
	if _, err := d.Rebalance().ListJobs(ctx, string(store.RebalanceCompleted)); err == nil {
		t.Fatal("ListJobs(status): expected error")
	}
	if err := d.Rebalance().UpdateJobStatus(ctx, id, store.RebalanceCompleted); err == nil {
		t.Fatal("UpdateJobStatus: expected error")
	}
	if err := d.Rebalance().UpdateJobStatus(ctx, id, store.RebalancePending); err == nil {
		t.Fatal("UpdateJobStatus(pending): expected error")
	}

	// OutboxStore
	e := &store.OutboxEntry{Aggregate: "batch", EventType: "batch.close", DedupKey: "k1", Payload: []byte("{}")}
	if _, err := d.Outbox().Append(ctx, e); err == nil {
		t.Fatal("Append: expected error")
	}
	if _, err := d.Outbox().ListPending(ctx, 10); err == nil {
		t.Fatal("ListPending: expected error")
	}
	if err := d.Outbox().MarkEmitted(ctx, id); err == nil {
		t.Fatal("MarkEmitted: expected error")
	}
}

func TestPing_UnreachableReturnsError(t *testing.T) {
	d := newUnreachableDB(t)
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err == nil {
		t.Fatal("expected ping error")
	}
}

// TestJoinStrings covers the unexported helper directly.
func TestJoinStrings(t *testing.T) {
	cases := []struct {
		in   []string
		sep  string
		want string
	}{
		{nil, ", ", ""},
		{[]string{"a"}, ", ", "a"},
		{[]string{"a", "b"}, ", ", "a, b"},
		{[]string{"a", "b", "c"}, " AND ", "a AND b AND c"},
	}
	for _, c := range cases {
		if got := joinStrings(c.in, c.sep); got != c.want {
			t.Errorf("joinStrings(%v, %q)=%q want %q", c.in, c.sep, got, c.want)
		}
	}
}

// TestDB_WithOutboxMethodsReturnErrors exercises the DB-level *WithOutbox
// Unit methods against the unreachable pool: OpenBatchWithOutbox,
// CreateFundingWithOutbox, AddFloatWithOutbox. Each should return a
// connection error (the tx Begin fails fast). buildEvents is not invoked
// because the Begin error short-circuits.
func TestDB_WithOutboxMethodsReturnErrors(t *testing.T) {
	d := newUnreachableDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := uuid.New()

	if _, err := d.OpenBatchWithOutbox(ctx, "BTC/USD", func(b *store.Batch) []*store.OutboxEntry {
		t.Fatal("buildEvents must not be called when Begin fails")
		return nil
	}); err == nil {
		t.Fatal("OpenBatchWithOutbox: expected error")
	}

	fp := &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC"}
	if _, err := d.AddFloatWithOutbox(ctx, fp, []*store.OutboxEntry{{Aggregate: "float", EventType: "float.adjust", DedupKey: "k", Payload: []byte("{}")}}); err == nil {
		t.Fatal("AddFloatWithOutbox: expected error")
	}

	fr := &store.FundingRequest{WalletID: "w", Asset: "BTC", Amount: decimal.NewFromInt(1), Status: store.FundingPending}
	if _, err := d.CreateFundingWithOutbox(ctx, fr, []*store.OutboxEntry{{Aggregate: "funding", EventType: "funding.create", DedupKey: "k", Payload: []byte("{}")}}); err == nil {
		t.Fatal("CreateFundingWithOutbox: expected error")
	}

	// UpdateBatchStatusWithOutbox with nil events slice: still fails at Begin.
	if _, _, err := d.UpdateBatchStatusWithOutbox(ctx, id, store.BatchOpen, store.BatchClosed, nil, nil); err == nil {
		t.Fatal("UpdateBatchStatusWithOutbox(nil events): expected error")
	}
}

// TestDB_OpenWithNilEvents ensures the *WithOutbox methods tolerate nil
// entries in the events slice on the error path (the Begin failure
// short-circuits before iteration, so this just guards the contract).
func TestDB_OpenWithNilEvents(t *testing.T) {
	d := newUnreachableDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := []*store.OutboxEntry{nil, {Aggregate: "a", EventType: "e", DedupKey: "k", Payload: []byte("{}")}, nil}
	fp := &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1)}
	if _, err := d.AddFloatWithOutbox(ctx, fp, events); err == nil {
		t.Fatal("expected error with nil-containing events")
	}
}
