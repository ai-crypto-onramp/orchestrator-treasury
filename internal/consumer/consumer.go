// Package consumer turns tx completion events into batch memberships.
// On receipt it:
//   - dedupes by tx_id via the idempotency store
//   - opens (or reuses) the currently-open batch for the asset pair
//   - persists a batch_membership row
//   - emits a structured log line + Prometheus counter
//
// Poison messages (undecodable / invalid payloads) are routed to a
// dead-letter channel so the consumer can keep making progress.
package consumer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/eventbus"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/metrics"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// DeadLetter is a poison-message record.
type DeadLetter struct {
	Topic   string
	Payload eventbus.TxCompletedEvent
	Reason  string
	At      time.Time
}

// Deps bundles consumer dependencies.
type Deps struct {
	Topic       string
	Batches     store.BatchStore
	Memberships store.MembershipStore
	Idem        idempotency.Store
	Subscriber  eventbus.EventSubscriber
	DeadLetters chan<- DeadLetter
	Unit        store.Unit
	// OnBatchOpen is called when a new batch is opened (not when an
	// existing open batch is reused), so the caller can emit a ledger/
	// audit event. Used only when Unit is nil (non-transactional
	// fallback).
	OnBatchOpen func(ctx context.Context, batch *store.Batch)
	// BuildBatchOpenOutbox returns outbox entries to append inside the
	// same DB transaction as the batch open. Used when Unit is non-nil;
	// the entries are passed to Unit.OpenBatchWithOutbox, which appends
	// them only when a new batch is created.
	BuildBatchOpenOutbox func(ctx context.Context, batch *store.Batch) []*store.OutboxEntry
}

// Consumer drains tx completion events and persists memberships.
type Consumer struct {
	Deps Deps
	mu   sync.Mutex
	stop context.CancelFunc
}

// New returns a new consumer.
func New(deps Deps) *Consumer { return &Consumer{Deps: deps} }

// Run starts the consumer loop. It blocks until ctx is canceled or the
// subscriber channel is closed.
func (c *Consumer) Run(ctx context.Context) error {
	ch, stop, err := c.Deps.Subscriber.Subscribe(ctx, c.Deps.Topic)
	if err != nil {
		return err
	}
	ctx2, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.stop = cancel
	c.mu.Unlock()
	defer stop()
	for {
		select {
		case <-ctx2.Done():
			return ctx2.Err()
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			c.handle(ctx2, ev)
		}
	}
}

// Stop cancels the consumer loop.
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stop != nil {
		c.stop()
	}
}

// openBatch opens (or reuses) a batch for the asset pair. When a Unit is
// configured it uses OpenBatchWithOutbox so the batch-open outbox event is
// appended in the same DB transaction as the batch creation; otherwise it
// falls back to the plain BatchStore and the OnBatchOpen hook.
func (c *Consumer) openBatch(ctx context.Context, assetPair string) (*store.Batch, error) {
	if c.Deps.Unit != nil {
		return c.Deps.Unit.OpenBatchWithOutbox(ctx, assetPair, func(b *store.Batch) []*store.OutboxEntry {
			if c.Deps.BuildBatchOpenOutbox == nil {
				return nil
			}
			return c.Deps.BuildBatchOpenOutbox(ctx, b)
		})
	}
	return c.Deps.Batches.OpenBatch(ctx, assetPair)
}

// handle processes one event: dedupe, open batch, add membership.
func (c *Consumer) handle(ctx context.Context, ev eventbus.TxCompletedEvent) {
	if ev.TxID == "" || ev.Asset == "" {
		c.deadLetter("empty tx_id or asset", ev)
		metrics.DeadLettered.WithLabelValues(c.Deps.Topic).Inc()
		return
	}
	assetPair := ev.Asset + "/" + ev.FiatCurrency
	// Idempotency: dedupe by tx_id. TTL keeps the key long enough to
	// absorb broker replays.
	ok, err := c.Deps.Idem.CheckAndMark(ctx, "tx:"+ev.TxID, 24*time.Hour)
	if err != nil {
		log.Printf("consumer: idem check tx=%s: %v", ev.TxID, err)
		metrics.EventsConsumed.WithLabelValues(assetPair, "idem_error").Inc()
		return
	}
	if !ok {
		log.Printf("consumer: dup tx=%s skipped", ev.TxID)
		metrics.EventsConsumed.WithLabelValues(assetPair, "dup").Inc()
		return
	}
	// Also check the durable store so crash-recovery dedupes too.
	exists, err := c.Deps.Memberships.ExistsByTxID(ctx, ev.TxID)
	if err != nil {
		log.Printf("consumer: exists tx=%s: %v", ev.TxID, err)
		metrics.EventsConsumed.WithLabelValues(assetPair, "store_error").Inc()
		return
	}
	if exists {
		metrics.EventsConsumed.WithLabelValues(assetPair, "dup").Inc()
		return
	}
	batch, err := c.openBatch(ctx, assetPair)
	if err != nil {
		log.Printf("consumer: open batch pair=%s: %v", assetPair, err)
		metrics.EventsConsumed.WithLabelValues(assetPair, "open_error").Inc()
		return
	}
	// Detect a freshly-opened batch (no memberships yet) so we can fire
	// the OnBatchOpen hook for audit emission.
	existing, _ := c.Deps.Memberships.ListMemberships(ctx, batch.ID)
	isNew := len(existing) == 0
	notional := ev.NotionalUSD
	if notional.IsZero() {
		notional = ev.Amount
	}
	_, err = c.Deps.Memberships.AddMembership(ctx, &store.Membership{
		BatchID:      batch.ID,
		TxID:         ev.TxID,
		Amount:       ev.Amount,
		Asset:        ev.Asset,
		FiatCurrency: ev.FiatCurrency,
		NotionalUSD:  notional,
		UserID:       ev.UserID,
	})
	if err != nil {
		log.Printf("consumer: add membership tx=%s: %v", ev.TxID, err)
		metrics.EventsConsumed.WithLabelValues(assetPair, "store_error").Inc()
		return
	}
	if isNew && c.Deps.OnBatchOpen != nil {
		c.Deps.OnBatchOpen(ctx, batch)
	}
	// Keep the batch notional rolling total in sync for size-threshold
	// checks.
	sum, _ := c.Deps.Memberships.SumNotional(ctx, batch.ID)
	_ = c.Deps.Batches.SetBatchNotional(ctx, batch.ID, sum)
	log.Printf("consumer: added tx=%s pair=%s batch=%s notional=%s", ev.TxID, assetPair, batch.ID, sum.String())
	metrics.EventsConsumed.WithLabelValues(assetPair, "ok").Inc()
}

func (c *Consumer) deadLetter(reason string, ev eventbus.TxCompletedEvent) {
	if c.Deps.DeadLetters == nil {
		return
	}
	select {
	case c.Deps.DeadLetters <- DeadLetter{Topic: c.Deps.Topic, Payload: ev, Reason: reason, At: time.Now().UTC()}:
	default:
		log.Printf("consumer: dead-letter channel full, dropping event tx=%s", ev.TxID)
	}
}

// ErrAlreadyStopped is returned when Stop is called on a stopped consumer.
var ErrAlreadyStopped = errors.New("consumer: already stopped")
