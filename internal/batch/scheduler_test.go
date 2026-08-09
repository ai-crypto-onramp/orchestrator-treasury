package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func newSchedulerDeps(t *testing.T, cfg config.Config) (*Scheduler, *memstore.All, *idempotency.MemStore) {
	t.Helper()
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	lock := idempotency.NewCadenceLock(idem, time.Duration(cfg.BatchIntervalSeconds)*time.Second)
	var closed []*store.Batch
	s := New(Deps{
		Cfg:         cfg,
		Batches:     all.Batch,
		Memberships: all.Membership,
		Lock:        lock,
		OnClose: func(ctx context.Context, b *store.Batch, reason CloseReason) {
			closed = append(closed, b)
		},
	})
	_ = closed
	return s, all, idem
}

func TestScheduler_SizeThresholdClosesBatch(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(5000)}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", NotionalUSD: decimal.NewFromInt(6000), Asset: "BTC", FiatCurrency: "USD"})
	_ = all.Batch.SetBatchNotional(ctx, b.ID, decimal.NewFromInt(6000))
	reason := s.shouldClose(ctx, b)
	if reason != ReasonSize {
		t.Fatalf("reason=%q want size", reason)
	}
	closed, err := s.CloseBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", closed.Status)
	}
}

func TestScheduler_ManualCloseForcesAggregation(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(100000)}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// No memberships; thresholds unmet.
	reason := s.shouldClose(ctx, b)
	if reason != "" {
		t.Fatalf("reason=%q want empty", reason)
	}
	closed, err := s.CloseBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", closed.Status)
	}
	// Closing again fails.
	if _, err := s.CloseBatch(ctx, b.ID); err != ErrNotOpen {
		t.Fatalf("expected ErrNotOpen, got %v", err)
	}
}

// TestScheduler_CloseUnitAppendsOutbox verifies that when a Unit is
// configured the close transition and the BuildCloseOutbox event are
// persisted together via Unit.UpdateBatchStatusWithOutbox.
func TestScheduler_CloseUnitAppendsOutbox(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(100000)}
	all := memstore.NewAll()
	idem := idempotency.NewMem()
	lock := idempotency.NewCadenceLock(idem, time.Duration(cfg.BatchIntervalSeconds)*time.Second)
	s := New(Deps{
		Cfg:         cfg,
		Batches:     all.Batch,
		Memberships: all.Membership,
		Lock:        lock,
		Unit:        all,
		BuildCloseOutbox: func(_ context.Context, b *store.Batch, _ CloseReason) []*store.OutboxEntry {
			return []*store.OutboxEntry{{Aggregate: "batch", EventType: "batch.close", DedupKey: "bc:" + b.ID.String(), Payload: []byte("{}")}}
		},
	})
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	closed, err := s.CloseBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", closed.Status)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(pending))
	}
}

func TestScheduler_TimeThresholdClosesBatch(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 1, BatchSizeThresholdUSD: decimal.NewFromInt(1000000)}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// Rely on the small interval (1s) and wait so the time threshold
	// triggers.
	_ = b
	time.Sleep(1100 * time.Millisecond)
	reason := s.shouldClose(ctx, b)
	if reason != ReasonTime {
		t.Fatalf("reason=%q want time", reason)
	}
}

func TestScheduler_TickClosesOpenBatches(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 1, BatchSizeThresholdUSD: decimal.NewFromInt(1000000)}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// Make it old enough by waiting.
	time.Sleep(1100 * time.Millisecond)
	s.tick(ctx)
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", got.Status)
	}
}

func TestScheduler_CadenceLockPreventsDoubleClose(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 1, BatchSizeThresholdUSD: decimal.NewFromInt(1000000)}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	time.Sleep(1100 * time.Millisecond)
	// Acquire the cadence lock manually first.
	ok, _ := s.deps.Lock.Acquire(ctx, "BTC/USD")
	if !ok {
		t.Fatal("expected lock acquire")
	}
	s.tick(ctx)
	// Batch should remain open because lock was held.
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchOpen {
		t.Fatalf("expected open (lock held), got %s", got.Status)
	}
}

// --- fakes for error-path tests ---

type errBatchStore struct{ err error }

func (e errBatchStore) OpenBatch(context.Context, string) (*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) GetBatch(context.Context, uuid.UUID) (*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) ListBatches(context.Context, time.Time, time.Time) ([]*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) ListOpenBatches(context.Context) ([]*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) UpdateBatchStatus(context.Context, uuid.UUID, store.BatchStatus, store.BatchStatus, func(*store.Batch)) (*store.Batch, bool, error) {
	return nil, false, e.err
}
func (e errBatchStore) SetBatchNotional(context.Context, uuid.UUID, decimal.Decimal) error {
	return e.err
}

type noOpMembership struct{}

func (noOpMembership) AddMembership(context.Context, *store.Membership) (bool, error) {
	return true, nil
}
func (noOpMembership) ListMemberships(context.Context, uuid.UUID) ([]*store.Membership, error) {
	return nil, nil
}
func (noOpMembership) SumNotional(context.Context, uuid.UUID) (decimal.Decimal, error) {
	return decimal.Decimal{}, nil
}
func (noOpMembership) ExistsByTxID(context.Context, string) (bool, error) { return false, nil }

func TestScheduler_TickListErrorReturns(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 30, BatchSizeThresholdUSD: decimal.NewFromInt(100)}
	s := New(Deps{
		Cfg:         cfg,
		Batches:     errBatchStore{err: errStore},
		Memberships: noOpMembership{},
		Lock:        idempotency.NewCadenceLock(idempotency.NewMem(), time.Minute),
	})
	// Should not panic; just logs and returns.
	s.tick(ctx)
}

func TestScheduler_TickCloseBatchGetError(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 30, BatchSizeThresholdUSD: decimal.NewFromInt(100)}
	s := New(Deps{
		Cfg:         cfg,
		Batches:     errBatchStore{err: errStore},
		Memberships: noOpMembership{},
		Lock:        idempotency.NewCadenceLock(idempotency.NewMem(), time.Minute),
	})
	if _, err := s.CloseBatch(ctx, uuid.New()); err != errStore {
		t.Fatalf("expected errStore, got %v", err)
	}
}

var errStore = errors.New("store boom")

// TestScheduler_RunAndStop covers the Run loop and Stop. Run blocks; we
// start it and Stop it shortly after to exercise the ctx.Done() return
// path and the stop mutex.
func TestScheduler_RunAndStop(t *testing.T) {
	cfg := config.Config{BatchIntervalSeconds: 30, BatchSizeThresholdUSD: decimal.NewFromInt(100)}
	all := memstore.NewAll()
	s := New(Deps{
		Cfg:         cfg,
		Batches:     all.Batch,
		Memberships: all.Membership,
		Lock:        idempotency.NewCadenceLock(idempotency.NewMem(), time.Minute),
	})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- s.Run(ctx) }()
	// Let the loop install stop and tick at least once.
	time.Sleep(150 * time.Millisecond)
	// Stop via the public Stop API (exercises the stop mutex / nil guard).
	s.Stop()
	// Calling Stop again is a no-op (stop already nil-guarded).
	s.Stop()
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		// Run may still be ticking if Stop didn't propagate before cancel;
		// cancel ensures it returns.
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Fatalf("run returned %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return")
		}
	}
}

// TestScheduler_ConcurrentManualCloses verifies that two concurrent manual
// closes on the same open batch do not both succeed (one wins, the other
// gets ErrNotOpen via the ok=false branch).
func TestScheduler_ConcurrentManualCloses(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(100000)}
	s, all, _ := newSchedulerDeps(t, cfg)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")

	wins := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := s.CloseBatch(ctx, b.ID)
			wins <- err
		}()
	}
	var nils, notOpens int
	for i := 0; i < 2; i++ {
		e := <-wins
		switch e {
		case nil:
			nils++
		case ErrNotOpen:
			notOpens++
		default:
			t.Fatalf("unexpected err: %v", e)
		}
	}
	if nils != 1 || notOpens != 1 {
		t.Fatalf("expected 1 win + 1 not-open, got nils=%d notOpens=%d", nils, notOpens)
	}
}

// TestScheduler_CloseBatchUnknownID exercises the GetBatch error path
// with a real store (ErrNotFound).
func TestScheduler_CloseBatchUnknownID(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(100000)}
	s, _, _ := newSchedulerDeps(t, cfg)
	if _, err := s.CloseBatch(ctx, uuid.New()); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestScheduler_TickLockAcquireError exercises the branch where Lock.Acquire
// returns an error (store-backed idem that errors).
func TestScheduler_TickLockAcquireError(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(100)}
	all := memstore.NewAll()
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	// Make it size-eligible so tick tries to acquire the lock.
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t", NotionalUSD: decimal.NewFromInt(1000), Asset: "BTC", FiatCurrency: "USD"})
	s := New(Deps{
		Cfg:         cfg,
		Batches:     all.Batch,
		Memberships: all.Membership,
		Lock:        idempotency.NewCadenceLock(errBatchStore{err: errStore}, time.Minute), // errBatchStore implements idempotency.Store
	})
	// Should log and continue without closing.
	s.tick(ctx)
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchOpen {
		t.Fatalf("expected batch still open after lock error, got %s", got.Status)
	}
}

// errStore also implements idempotency.Store (CheckAndMark/Delete error).
func (e errBatchStore) CheckAndMark(context.Context, string, time.Duration) (bool, error) {
	return false, errStore
}
func (e errBatchStore) Delete(context.Context, string) error { return errStore }
