// Package batch implements the batch formation policy: batches close
// deterministically on a time cadence, a notional size threshold, or a
// manual operator trigger.
//
// The scheduler loop runs on a configurable tick. On each tick it scans
// open batches and closes any whose time or size threshold is met. A
// Redis cadence lock per asset pair prevents double-close under leader
// election. Manual close is exposed via CloseBatch.
package batch

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/metrics"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// CloseReason describes why a batch was closed.
type CloseReason string

const (
	ReasonTime   CloseReason = "time"
	ReasonSize   CloseReason = "size"
	ReasonManual CloseReason = "MANUAL"
)

// Deps bundles the scheduler dependencies.
type Deps struct {
	Cfg         config.Config
	Batches     store.BatchStore
	Memberships store.MembershipStore
	Lock        *idempotency.CadenceLock
	Unit        store.Unit
	OnClose     func(ctx context.Context, b *store.Batch, reason CloseReason)
	// BuildCloseOutbox returns outbox entries to append inside the same
	// DB transaction as the batch close transition. Used when Unit is
	// non-nil; the entries are passed to Unit.UpdateBatchStatusWithOutbox
	// so a crash between the state change and the outbox insert cannot
	// lose the audit event.
	BuildCloseOutbox func(ctx context.Context, b *store.Batch, reason CloseReason) []*store.OutboxEntry
}

// Scheduler runs the batch close cadence loop.
type Scheduler struct {
	deps Deps
	mu   sync.Mutex
	stop context.CancelFunc
}

// New returns a new scheduler.
func New(deps Deps) *Scheduler { return &Scheduler{deps: deps} }

// Run starts the scheduler loop. It blocks until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.stop = cancel
	s.mu.Unlock()
	tick := time.NewTicker(time.Duration(s.deps.Cfg.BatchIntervalSeconds) * time.Second / 4)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			s.tick(ctx)
		}
	}
}

// Stop cancels the scheduler loop.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		s.stop()
	}
}

// tick scans open batches and closes any that meet time or size
// thresholds. Each close is serialized per asset pair via the cadence
// lock so concurrent schedulers never double-close.
func (s *Scheduler) tick(ctx context.Context) {
	open, err := s.deps.Batches.ListOpenBatches(ctx)
	if err != nil {
		log.Printf("scheduler: list open: %v", err)
		return
	}
	for _, b := range open {
		reason := s.shouldClose(ctx, b)
		if reason == "" {
			continue
		}
		ok, err := s.deps.Lock.Acquire(ctx, b.AssetPair)
		if err != nil {
			log.Printf("scheduler: cadence lock pair=%s: %v", b.AssetPair, err)
			continue
		}
		if !ok {
			continue
		}
		if _, err := s.close(ctx, b.ID, reason); err != nil {
			log.Printf("scheduler: close batch=%s: %v", b.ID, err)
		}
	}
}

// shouldClose returns the close reason ("", "time", or "size") for a
// batch based on the configured policy.
func (s *Scheduler) shouldClose(ctx context.Context, b *store.Batch) CloseReason {
	sum, _ := s.deps.Memberships.SumNotional(ctx, b.ID)
	threshold := s.deps.Cfg.BatchThresholdFor(b.AssetPair)
	if threshold.GreaterThan(decimal.Zero) && sum.GreaterThanOrEqual(threshold) {
		return ReasonSize
	}
	interval := s.deps.Cfg.BatchIntervalDuration(b.AssetPair)
	if interval > 0 && time.Since(b.OpenedAt) >= interval {
		return ReasonTime
	}
	return ""
}

// CloseBatch forces a manual close of an open batch regardless of
// thresholds. Returns ErrNotOpen if the batch is not open.
func (s *Scheduler) CloseBatch(ctx context.Context, id uuid.UUID) (*store.Batch, error) {
	return s.close(ctx, id, ReasonManual)
}

func (s *Scheduler) close(ctx context.Context, id uuid.UUID, reason CloseReason) (*store.Batch, error) {
	b, err := s.deps.Batches.GetBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	if b.Status != store.BatchOpen {
		return nil, ErrNotOpen
	}
	sum, _ := s.deps.Memberships.SumNotional(ctx, id)
	mutator := func(b *store.Batch) {
		b.NotionalUSD = sum
	}
	var (
		updated *store.Batch
		ok      bool
	)
	if s.deps.Unit != nil {
		var events []*store.OutboxEntry
		if s.deps.BuildCloseOutbox != nil {
			events = s.deps.BuildCloseOutbox(ctx, b, reason)
		}
		updated, ok, err = s.deps.Unit.UpdateBatchStatusWithOutbox(ctx, id, store.BatchOpen, store.BatchClosed, mutator, events)
	} else {
		updated, ok, err = s.deps.Batches.UpdateBatchStatus(ctx, id, store.BatchOpen, store.BatchClosed, mutator)
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotOpen
	}
	metrics.BatchesClosed.WithLabelValues(b.AssetPair, string(reason)).Inc()
	metrics.CloseLatency.WithLabelValues(b.AssetPair).Observe(time.Since(updated.OpenedAt).Seconds())
	log.Printf("scheduler: closed batch=%s pair=%s reason=%s notional=%s", id, b.AssetPair, reason, sum.String())
	if s.deps.OnClose != nil {
		s.deps.OnClose(ctx, updated, reason)
	}
	return updated, nil
}

// ErrNotOpen is returned when CloseBatch targets a non-open batch.
var ErrNotOpen = errors.New("batch: not open")
