// Package ledger posts every capital movement to ledger-accounting and
// emits audit events for every aggregate action. It uses an outbox
// pattern to guarantee delivery even when a downstream is temporarily
// unavailable: events are first appended to the local outbox (within
// the same DB transaction as the state change, when a DB is present),
// then a dispatcher drains the outbox and posts to ledger + audit with
// idempotency keys.
package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/metrics"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// Aggregate is the entity an event belongs to (batch, aggregate, funding,
// float, rebalance).
type Aggregate string

const (
	AggBatch     Aggregate = "batch"
	AggAggregate Aggregate = "aggregate"
	AggFunding   Aggregate = "funding"
	AggFloat     Aggregate = "float"
	AggRebalance Aggregate = "rebalance"
)

// EventType enumerates the audit/ledger event types.
type EventType string

const (
	EvBatchOpen     EventType = "batch.open"
	EvBatchClose    EventType = "batch.close"
	EvAggregateExec EventType = "aggregate.execute"
	EvFunding       EventType = "funding.create"
	EvFloatAdjust   EventType = "float.adjust"
	EvRebalance     EventType = "rebalance.create"
)

// Payload is the outbox payload shape.
type Payload struct {
	Aggregate    Aggregate       `json:"aggregate"`
	EventType    EventType       `json:"event_type"`
	BatchID      uuid.UUID       `json:"batch_id,omitempty"`
	NotionalUSD  decimal.Decimal `json:"notional_usd,omitempty"`
	Asset        string          `json:"asset,omitempty"`
	FiatCurrency string          `json:"fiat_currency,omitempty"`
	Detail       string          `json:"detail,omitempty"`
}

// Deps bundles the dispatcher dependencies.
type Deps struct {
	Outbox store.OutboxStore
	Ledger clients.LedgerAccounting
	Audit  clients.AuditLog
}

// Emitter appends events to the outbox and dispatches them to ledger +
// audit.
type Emitter struct {
	deps Deps
}

// New returns a new emitter.
func New(deps Deps) *Emitter { return &Emitter{deps: deps} }

// Append records an event in the outbox. dedupKey should be unique per
// logical event (e.g. "batch.close:42") so replays do not double-post.
func (e *Emitter) Append(ctx context.Context, agg Aggregate, evType EventType, dedupKey string, p Payload) error {
	ent := BuildEntry(agg, evType, dedupKey, p)
	_, err := e.deps.Outbox.Append(ctx, ent)
	return err
}

// BuildEntry constructs an outbox entry for the given event without
// writing it. Callers that need to append the event inside the same DB
// transaction as a state change use this to hand the entry to a
// transactional store method (store.Unit).
func BuildEntry(agg Aggregate, evType EventType, dedupKey string, p Payload) *store.OutboxEntry {
	p.Aggregate = agg
	p.EventType = evType
	body, _ := json.Marshal(p)
	return &store.OutboxEntry{
		Aggregate: string(agg),
		EventType: string(evType),
		DedupKey:  dedupKey,
		Payload:   body,
	}
}

// Dispatch drains up to limit pending outbox entries and posts each to
// ledger-accounting and audit-event-log. Returns the number of entries
// processed.
func (e *Emitter) Dispatch(ctx context.Context, limit int) (int, error) {
	pending, err := e.deps.Outbox.ListPending(ctx, limit)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, ent := range pending {
		var p Payload
		_ = json.Unmarshal(ent.Payload, &p)
		key := ent.DedupKey
		// Ledger posting.
		// Ledger posting.
		if e.deps.Ledger != nil {
			if err := e.deps.Ledger.Post(ctx, clients.LedgerPost{
				Aggregate:    string(p.Aggregate),
				EventType:    string(p.EventType),
				NotionalUSD:  p.NotionalUSD,
				Asset:        p.Asset,
				FiatCurrency: p.FiatCurrency,
				BatchID:      p.BatchID,
			}, key); err != nil {
				metrics.LedgerPost.WithLabelValues(string(p.Aggregate), "error").Inc()
				log.Printf("ledger: post id=%s: %v", ent.ID, err)
				continue
			}
			metrics.LedgerPost.WithLabelValues(string(p.Aggregate), "ok").Inc()
		}
		// Audit emission.
		if e.deps.Audit != nil {
			if err := e.deps.Audit.Emit(ctx, clients.AuditEvent{
				Aggregate: string(p.Aggregate),
				EventType: string(p.EventType),
				BatchID:   p.BatchID,
				Detail:    p.Detail,
			}, key); err != nil {
				metrics.AuditEmit.WithLabelValues(string(p.Aggregate), "error").Inc()
				log.Printf("audit: emit id=%s: %v", ent.ID, err)
				continue
			}
			metrics.AuditEmit.WithLabelValues(string(p.Aggregate), "ok").Inc()
		}
		if err := e.deps.Outbox.MarkEmitted(ctx, ent.ID); err != nil {
			log.Printf("outbox: mark emitted id=%s: %v", ent.ID, err)
			continue
		}
		processed++
	}
	return processed, nil
}

// RunDispatcherLoop periodically drains the outbox. Blocks until ctx is
// canceled.
func (e *Emitter) RunDispatcherLoop(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := e.Dispatch(ctx, 100); err != nil {
				log.Printf("dispatcher: %v", err)
			}
		}
	}
}

// Key builds a deterministic dedup key for an event.
func Key(agg Aggregate, evType EventType, id uuid.UUID) string {
	return fmt.Sprintf("%s.%s:%s", agg, evType, id)
}
