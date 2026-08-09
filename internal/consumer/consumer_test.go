package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/eventbus"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func newDeps(t *testing.T) (*Consumer, *memstore.All, *eventbus.InMemorySubscriber, idempotency.Store) {
	t.Helper()
	all := memstore.NewAll()
	bus := eventbus.NewInMemory()
	idem := idempotency.NewMem()
	c := New(Deps{
		Topic:       "tx.completed",
		Batches:     all.Batch,
		Memberships: all.Membership,
		Idem:        idem,
		Subscriber:  bus,
	})
	return c, all, bus, idem
}

func TestConsumer_AutoOpensBatchAndDedupes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, all, bus, _ := newDeps(t)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	ev := eventbus.TxCompletedEvent{TxID: "tx1", Amount: decimal.NewFromInt(1), Asset: "BTC", FiatCurrency: "USD", NotionalUSD: decimal.NewFromInt(1000)}
	_ = bus.Push(ctx, "tx.completed", ev)
	_ = bus.Push(ctx, "tx.completed", ev) // dup

	// Wait for processing.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		exists, _ := all.Membership.ExistsByTxID(ctx, "tx1")
		if exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	exists, _ := all.Membership.ExistsByTxID(ctx, "tx1")
	if !exists {
		t.Fatal("expected membership")
	}
	open, _ := all.Batch.ListOpenBatches(ctx)
	if len(open) != 1 {
		t.Fatalf("expected 1 open batch, got %d", len(open))
	}
	if open[0].AssetPair != "BTC/USD" {
		t.Fatalf("pair=%s want BTC/USD", open[0].AssetPair)
	}
	ms, _ := all.Membership.ListMemberships(ctx, open[0].ID)
	if len(ms) != 1 {
		t.Fatalf("expected 1 membership after dup, got %d", len(ms))
	}
}

func TestConsumer_DeadLettersPoisonMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _, bus, _ := newDeps(t)
	dl := make(chan DeadLetter, 8)
	c.Deps.DeadLetters = dl
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "", Asset: "BTC"})
	select {
	case got := <-dl:
		if got.Reason == "" {
			t.Fatal("expected reason")
		}
	case <-time.After(time.Second):
		t.Fatal("expected dead-letter")
	}
}

func TestConsumer_OpenBatchReusedPerAssetPair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, all, bus, _ := newDeps(t)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond) // allow subscriber to register

	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "a", Asset: "BTC", FiatCurrency: "USD"})
	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "b", Asset: "BTC", FiatCurrency: "USD"})
	_ = bus.Push(ctx, "tx.completed", eventbus.TxCompletedEvent{TxID: "c", Asset: "ETH", FiatCurrency: "USD"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		open, _ := all.Batch.ListOpenBatches(ctx)
		if len(open) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	open, _ := all.Batch.ListOpenBatches(ctx)
	if len(open) != 2 {
		t.Fatalf("expected 2 open batches (BTC/USD, ETH/USD), got %d", len(open))
	}
}

// --- additional coverage ---

func TestConsumer_Stop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _, _, _ := newDeps(t)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	c.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not unblock Run")
	}
}

func TestConsumer_StopWhenNotRunning(t *testing.T) {
	c, _, _, _ := newDeps(t)
	// Stop with nil stop func should be a no-op, not panic.
	c.Stop()
}

func TestConsumer_HandleNotionalFallback(t *testing.T) {
	ctx := context.Background()
	c, all, _, _ := newDeps(t)
	// NotionalUSD == 0 -> use Amount.
	ev := eventbus.TxCompletedEvent{TxID: "nf1", Asset: "BTC", FiatCurrency: "USD", Amount: decimal.NewFromInt(2), NotionalUSD: decimal.Decimal{}}
	c.handle(ctx, ev)
	exists, _ := all.Membership.ExistsByTxID(ctx, "nf1")
	if !exists {
		t.Fatal("expected membership persisted")
	}
	open, _ := all.Batch.ListOpenBatches(ctx)
	if len(open) != 1 || !open[0].NotionalUSD.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("notional=%s want 2 (from Amount)", open[0].NotionalUSD.String())
	}
}

func TestConsumer_HandleOnBatchOpenHook(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := newDeps(t)
	opened := make(chan uuid.UUID, 4)
	c.Deps.OnBatchOpen = func(_ context.Context, b *store.Batch) { opened <- b.ID }
	// First event opens a new batch.
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "h1", Asset: "BTC", FiatCurrency: "USD"})
	// Second event for the same pair reuses the batch -> no new open hook.
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "h2", Asset: "BTC", FiatCurrency: "USD"})
	select {
	case id := <-opened:
		if id == uuid.Nil {
			t.Fatalf("expected non-nil batch id, got %s", id)
		}
	default:
		t.Fatal("expected OnBatchOpen to fire for the first event")
	}
	select {
	case <-opened:
		t.Fatal("OnBatchOpen should not fire for the reused batch")
	default:
	}
}

// TestConsumer_HandleUnitAppendsOutboxOnOpen verifies that when a Unit is
// configured the batch-open outbox event is appended inside the same DB
// transaction as the batch creation (via Unit.OpenBatchWithOutbox), and
// that no outbox event is appended when an existing open batch is reused.
func TestConsumer_HandleUnitAppendsOutboxOnOpen(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	bus := eventbus.NewInMemory()
	c := New(Deps{
		Topic:       "tx.completed",
		Batches:     all.Batch,
		Memberships: all.Membership,
		Idem:        idempotency.NewMem(),
		Subscriber:  bus,
		Unit:        all,
		BuildBatchOpenOutbox: func(_ context.Context, b *store.Batch) []*store.OutboxEntry {
			return []*store.OutboxEntry{{Aggregate: "batch", EventType: "batch.open", DedupKey: "bo:" + b.ID.String(), Payload: []byte("{}")}}
		},
	})
	// First event opens a new batch -> one outbox entry.
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "u1", Asset: "BTC", FiatCurrency: "USD"})
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("after first open: outbox entries=%d want 1", len(pending))
	}
	// Second event for the same pair reuses the batch -> no new outbox entry.
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "u2", Asset: "BTC", FiatCurrency: "USD"})
	pending, _ = all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("after reuse: outbox entries=%d want 1 (no append on reuse)", len(pending))
	}
}

func TestConsumer_HandleEmptyAssetDeadLetters(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := newDeps(t)
	dl := make(chan DeadLetter, 4)
	c.Deps.DeadLetters = dl
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "x", Asset: ""})
	select {
	case <-dl:
	default:
		t.Fatal("expected a dead-letter for empty asset")
	}
}

func TestConsumer_HandleIdemErrorSkips(t *testing.T) {
	ctx := context.Background()
	c, all, _, _ := newDeps(t)
	c.Deps.Idem = errIdemStore{}
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "ie", Asset: "BTC", FiatCurrency: "USD"})
	// Nothing persisted.
	exists, _ := all.Membership.ExistsByTxID(ctx, "ie")
	if exists {
		t.Fatal("expected no membership on idem error")
	}
}

func TestConsumer_HandleExistsByTxIDErrorSkips(t *testing.T) {
	ctx := context.Background()
	c, _, bus, _ := newDeps(t)
	c.Deps.Memberships = errMembershipStore{}
	_ = bus
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "ee", Asset: "BTC", FiatCurrency: "USD"})
	// Nothing persisted (no panic).
}

func TestConsumer_HandleOpenBatchErrorSkips(t *testing.T) {
	ctx := context.Background()
	c, all, _, _ := newDeps(t)
	c.Deps.Batches = errBatchStore{}
	_ = all
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "oe", Asset: "BTC", FiatCurrency: "USD"})
}

func TestConsumer_HandleAddMembershipErrorSkips(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := newDeps(t)
	c.Deps.Memberships = &addErrMembershipStore{}
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "ae", Asset: "BTC", FiatCurrency: "USD"})
}

func TestConsumer_HandleDurableDupSkips(t *testing.T) {
	ctx := context.Background()
	c, all, _, _ := newDeps(t)
	// Pre-populate the membership store with the tx so the durable dedup
	// path triggers (the idem store still claims the key, but ExistsByTxID
	// returns true).
	batchID, _ := uuid.NewV7()
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: batchID, TxID: "dd", Asset: "BTC", FiatCurrency: "USD"})
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "dd", Asset: "BTC", FiatCurrency: "USD"})
	ms, _ := all.Membership.ListMemberships(ctx, batchID)
	if len(ms) != 1 {
		t.Fatalf("expected 1 membership (durable dup skipped), got %d", len(ms))
	}
}

func TestConsumer_DeadLetterChannelFullDrops(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := newDeps(t)
	// A zero-capacity channel that's never read: the non-blocking send
	// falls through to the default branch (logged drop).
	dl := make(chan DeadLetter)
	c.Deps.DeadLetters = dl
	// Should not block.
	c.handle(ctx, eventbus.TxCompletedEvent{TxID: "", Asset: ""})
	select {
	case <-dl:
		t.Fatal("expected no dead-letter (channel full)")
	default:
	}
}

func TestConsumer_RunSubscriberError(t *testing.T) {
	c, _, _, _ := newDeps(t)
	c.Deps.Subscriber = errSubscriber{}
	if err := c.Run(context.Background()); !errors.Is(err, errSub) {
		t.Fatalf("err=%v want errSub", err)
	}
}

func TestConsumer_RunChannelClosedReturnsNil(t *testing.T) {
	c, _, _, _ := newDeps(t)
	ch := make(chan eventbus.TxCompletedEvent)
	sub := &closeChanSubscriber{ch: ch}
	c.Deps.Subscriber = sub
	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()
	// Close the channel to make Run return nil.
	close(ch)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err=%v want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after channel close")
	}
}

// --- fakes ---

var errSub = errors.New("subscribe boom")

type errSubscriber struct{}

func (errSubscriber) Subscribe(context.Context, string) (<-chan eventbus.TxCompletedEvent, func(), error) {
	return nil, func() {}, errSub
}
func (errSubscriber) Push(context.Context, string, eventbus.TxCompletedEvent) error { return nil }

type closeChanSubscriber struct {
	ch chan eventbus.TxCompletedEvent
}

func (s *closeChanSubscriber) Subscribe(context.Context, string) (<-chan eventbus.TxCompletedEvent, func(), error) {
	return s.ch, func() {}, nil
}
func (s *closeChanSubscriber) Push(context.Context, string, eventbus.TxCompletedEvent) error {
	return nil
}

type errIdemStore struct{}

func (errIdemStore) CheckAndMark(context.Context, string, time.Duration) (bool, error) {
	return false, errSub
}
func (errIdemStore) Delete(context.Context, string) error { return nil }

var errStore = errors.New("store boom")

type errBatchStore struct{}

func (errBatchStore) OpenBatch(context.Context, string) (*store.Batch, error) { return nil, errStore }
func (errBatchStore) GetBatch(context.Context, uuid.UUID) (*store.Batch, error) {
	return nil, errStore
}
func (errBatchStore) ListBatches(context.Context, time.Time, time.Time) ([]*store.Batch, error) {
	return nil, errStore
}
func (errBatchStore) ListOpenBatches(context.Context) ([]*store.Batch, error) { return nil, errStore }
func (errBatchStore) UpdateBatchStatus(context.Context, uuid.UUID, store.BatchStatus, store.BatchStatus, func(*store.Batch)) (*store.Batch, bool, error) {
	return nil, false, errStore
}
func (errBatchStore) SetBatchNotional(context.Context, uuid.UUID, decimal.Decimal) error {
	return errStore
}

type errMembershipStore struct{}

func (errMembershipStore) AddMembership(context.Context, *store.Membership) (bool, error) {
	return false, errStore
}
func (errMembershipStore) ListMemberships(context.Context, uuid.UUID) ([]*store.Membership, error) {
	return nil, errStore
}
func (errMembershipStore) SumNotional(context.Context, uuid.UUID) (decimal.Decimal, error) {
	return decimal.Decimal{}, errStore
}
func (errMembershipStore) ExistsByTxID(context.Context, string) (bool, error) { return false, errStore }

// addErrMembershipStore succeeds on ExistsByTxID / ListMemberships but
// errors on AddMembership.
type addErrMembershipStore struct {
	rows []*store.Membership
}

func (a *addErrMembershipStore) ListMemberships(_ context.Context, _ uuid.UUID) ([]*store.Membership, error) {
	return a.rows, nil
}
func (a *addErrMembershipStore) ExistsByTxID(context.Context, string) (bool, error) {
	return false, nil
}
func (a *addErrMembershipStore) AddMembership(context.Context, *store.Membership) (bool, error) {
	return false, errStore
}
func (a *addErrMembershipStore) SumNotional(context.Context, uuid.UUID) (decimal.Decimal, error) {
	return decimal.Decimal{}, nil
}
