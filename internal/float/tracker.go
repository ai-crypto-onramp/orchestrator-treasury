// Package float tracks the T+0 long-crypto / short-fiat position created
// by fronting crypto before fiat settles. On each aggregate fill the
// float position for the fiat currency is incremented. Matured floats
// are swept by the sweeper; breaches of MIN_FLOAT_USD / MAX_FLOAT_USD
// are logged, alerted via Prometheus, and trigger a forced rebalance/
// hedge signal through the OnBreach callback.
package float

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/metrics"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// Deps bundles the tracker dependencies.
type Deps struct {
	Cfg      config.Config
	Floats   store.FloatStore
	Unit     store.Unit
	OnBreach func(ctx context.Context, fiat string, amount decimal.Decimal, bound string)
	// OnAdjust is called after a float position is incremented by an
	// aggregate fill, so the caller can append a ledger/audit outbox
	// event. Used only when Unit is nil (non-transactional fallback).
	OnAdjust func(ctx context.Context, fiat string, amount decimal.Decimal, batchID uuid.UUID)
	// BuildAdjustOutbox returns outbox entries to append inside the same
	// DB transaction as the float increment. Used when Unit is non-nil;
	// the entries are passed to Unit.AddFloatWithOutbox so a crash between
	// the state change and the outbox insert cannot lose the audit event.
	BuildAdjustOutbox func(ctx context.Context, fiat string, amount decimal.Decimal, batchID uuid.UUID) []*store.OutboxEntry
}

// Tracker manages float positions.
type Tracker struct {
	deps Deps
}

// New returns a new tracker.
func New(deps Deps) *Tracker { return &Tracker{deps: deps} }

// OnAggregateFill increments the float for the fiat currency of a
// completed aggregate fill. The crypto asset is recorded; settlement
// due date is now + T+n for the fiat rail. When a store.Unit is
// configured the float increment and the adjust outbox event are
// persisted in the same DB transaction.
func (t *Tracker) OnAggregateFill(ctx context.Context, batch *store.Batch, order *store.AggregateOrder, fiatCurrency, cryptoAsset string) error {
	settlementDays := t.deps.Cfg.SettlementDaysFor(fiatCurrency)
	due := time.Now().UTC().Add(time.Duration(settlementDays) * 24 * time.Hour)
	pos := &store.FloatPosition{
		FiatCurrency:     fiatCurrency,
		ShortFiatAmount:  order.NotionalUSD,
		LongCryptoAmount: order.TotalFilled,
		LongCryptoAsset:  cryptoAsset,
		SettlementDueAt:  due,
		BatchID:          batch.ID,
	}
	if t.deps.Unit != nil {
		var events []*store.OutboxEntry
		if t.deps.BuildAdjustOutbox != nil {
			events = t.deps.BuildAdjustOutbox(ctx, fiatCurrency, order.NotionalUSD, batch.ID)
		}
		if _, err := t.deps.Unit.AddFloatWithOutbox(ctx, pos, events); err != nil {
			return err
		}
	} else {
		if _, err := t.deps.Floats.AddFloat(ctx, pos); err != nil {
			return err
		}
		if t.deps.OnAdjust != nil {
			t.deps.OnAdjust(ctx, fiatCurrency, order.NotionalUSD, batch.ID)
		}
	}
	t.enforceBounds(ctx, fiatCurrency)
	return nil
}

// Get returns the aggregate float position for a fiat currency.
func (t *Tracker) Get(ctx context.Context, fiatCurrency string) (*store.FloatPosition, error) {
	return t.deps.Floats.GetFloat(ctx, fiatCurrency)
}

// List returns the aggregate float position for every fiat currency.
func (t *Tracker) List(ctx context.Context) ([]*store.FloatPosition, error) {
	return t.deps.Floats.ListFloat(ctx)
}

// enforceBounds checks MIN_FLOAT_USD / MAX_FLOAT_USD and emits alerts /
// breach signals on violation.
func (t *Tracker) enforceBounds(ctx context.Context, fiat string) {
	pos, err := t.deps.Floats.GetFloat(ctx, fiat)
	if err != nil || pos == nil {
		return
	}
	metrics.FloatUSD.WithLabelValues(fiat).Set(pos.ShortFiatAmount.InexactFloat64())
	if t.deps.Cfg.MaxFloatUSD.GreaterThan(decimal.Zero) && pos.ShortFiatAmount.GreaterThan(t.deps.Cfg.MaxFloatUSD) {
		metrics.FloatBreach.WithLabelValues(fiat, "max").Inc()
		log.Printf("float: MAX breach %s amount=%s", fiat, pos.ShortFiatAmount.String())
		if t.deps.OnBreach != nil {
			t.deps.OnBreach(ctx, fiat, pos.ShortFiatAmount, "max")
		}
	}
	if t.deps.Cfg.MinFloatUSD.GreaterThan(decimal.Zero) && pos.ShortFiatAmount.LessThan(t.deps.Cfg.MinFloatUSD) {
		metrics.FloatBreach.WithLabelValues(fiat, "min").Inc()
		log.Printf("float: MIN breach %s amount=%s", fiat, pos.ShortFiatAmount.String())
		if t.deps.OnBreach != nil {
			t.deps.OnBreach(ctx, fiat, pos.ShortFiatAmount, "min")
		}
	}
}

// SweepMatured settles floats whose settlement_due_at has passed and
// decrements the short fiat leg. Returns the number of floats swept.
// Capital efficiency policy: sweeps happen within bounded latency of
// settlement_due_at.
func (t *Tracker) SweepMatured(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	matured, err := t.deps.Floats.ListMaturedFloat(ctx, now)
	if err != nil {
		return 0, err
	}
	swept := 0
	for _, p := range matured {
		if _, err := t.deps.Floats.SettleFloat(ctx, p.ID); err != nil {
			log.Printf("float: settle id=%s: %v", p.ID, err)
			continue
		}
		log.Printf("float: swept id=%s fiat=%s amount=%s due=%s", p.ID, p.FiatCurrency, p.ShortFiatAmount.String(), p.SettlementDueAt.Format(time.RFC3339))
		swept++
	}
	if swept > 0 {
		t.enforceBounds(ctx, matured[0].FiatCurrency)
	}
	return swept, nil
}

// RunSweeperLoop periodically sweeps matured floats. Blocks until ctx is
// canceled. The sweep interval is 1/4 of the smallest configured
// settlement window, bounded to at least 5 seconds and at most 60
// seconds.
func (t *Tracker) RunSweeperLoop(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := t.SweepMatured(ctx); err != nil {
				log.Printf("float: sweep: %v", err)
			}
		}
	}
}

// ErrUnknownCurrency is returned when a fiat currency is not configured.
var ErrUnknownCurrency = fmt.Errorf("float: unknown currency")
