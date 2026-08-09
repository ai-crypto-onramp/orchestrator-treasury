package float

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func newTracker(t *testing.T, cfg config.Config) (*Tracker, *memstore.FloatStore) {
	t.Helper()
	all := memstore.NewAll()
	tr := New(Deps{Cfg: cfg, Floats: all.Float})
	return tr, all.Float
}

func TestTracker_OnAggregateFillIncrementsFloat(t *testing.T) {
	ctx := context.Background()
	tr, floats := newTracker(t, config.Config{SettlementDays: map[string]int{"USD": 2}})
	batch := &store.Batch{ID: uuid.New(), AssetPair: "BTC/USD"}
	order := &store.AggregateOrder{ID: uuid.New(), BatchID: batch.ID, NotionalUSD: decimal.NewFromInt(50000), TotalFilled: decimal.NewFromInt(1)}
	if err := tr.OnAggregateFill(ctx, batch, order, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	pos, _ := floats.GetFloat(ctx, "USD")
	if !pos.ShortFiatAmount.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("short=%s want 50000", pos.ShortFiatAmount.String())
	}
	if !pos.LongCryptoAmount.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("long=%s want 1", pos.LongCryptoAmount.String())
	}
	if pos.LongCryptoAsset != "BTC" {
		t.Fatalf("asset=%s want BTC", pos.LongCryptoAsset)
	}
}

func TestTracker_SettlementDueAtIsTPlusN(t *testing.T) {
	ctx := context.Background()
	tr, floats := newTracker(t, config.Config{SettlementDays: map[string]int{"USD": 2, "JPY": 3}})
	batch := &store.Batch{ID: uuid.New(), AssetPair: "BTC/JPY"}
	order := &store.AggregateOrder{ID: uuid.New(), BatchID: batch.ID, NotionalUSD: decimal.NewFromInt(1000), TotalFilled: decimal.NewFromFloat(0.1)}
	if err := tr.OnAggregateFill(ctx, batch, order, "JPY", "BTC"); err != nil {
		t.Fatal(err)
	}
	matured, _ := floats.ListMaturedFloat(ctx, time.Now().Add(2*24*time.Hour+time.Hour))
	if len(matured) != 0 {
		t.Fatalf("expected 0 matured before T+3, got %d", len(matured))
	}
	matured2, _ := floats.ListMaturedFloat(ctx, time.Now().Add(3*24*time.Hour+time.Hour))
	if len(matured2) != 1 {
		t.Fatalf("expected 1 matured at T+3, got %d", len(matured2))
	}
}

func TestTracker_BreachMaxTriggersAlert(t *testing.T) {
	ctx := context.Background()
	var breaches []string
	all := memstore.NewAll()
	tr := New(Deps{
		Cfg:    config.Config{MaxFloatUSD: decimal.NewFromInt(100000), SettlementDays: map[string]int{"USD": 2}},
		Floats: all.Float,
		OnBreach: func(ctx context.Context, fiat string, amount decimal.Decimal, bound string) {
			breaches = append(breaches, fiat+":"+bound)
		},
	})
	batch := &store.Batch{ID: uuid.New(), AssetPair: "BTC/USD"}
	order := &store.AggregateOrder{ID: uuid.New(), BatchID: batch.ID, NotionalUSD: decimal.NewFromInt(200000), TotalFilled: decimal.NewFromInt(4)}
	if err := tr.OnAggregateFill(ctx, batch, order, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	if len(breaches) != 1 || breaches[0] != "USD:max" {
		t.Fatalf("breaches=%v want [USD:max]", breaches)
	}
}

func TestTracker_BreachMinTriggersAlert(t *testing.T) {
	ctx := context.Background()
	var breaches []string
	all := memstore.NewAll()
	tr := New(Deps{
		Cfg:    config.Config{MinFloatUSD: decimal.NewFromInt(300000), SettlementDays: map[string]int{"USD": 2}},
		Floats: all.Float,
		OnBreach: func(ctx context.Context, fiat string, amount decimal.Decimal, bound string) {
			breaches = append(breaches, fiat+":"+bound)
		},
	})
	batch := &store.Batch{ID: uuid.New(), AssetPair: "BTC/USD"}
	order := &store.AggregateOrder{ID: uuid.New(), BatchID: batch.ID, NotionalUSD: decimal.NewFromInt(1000), TotalFilled: decimal.NewFromFloat(0.01)}
	if err := tr.OnAggregateFill(ctx, batch, order, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	if len(breaches) != 1 || breaches[0] != "USD:min" {
		t.Fatalf("breaches=%v want [USD:min]", breaches)
	}
}

func TestTracker_SweepMaturedSettles(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	tr := New(Deps{Cfg: config.Config{}, Floats: all.Float})
	past := time.Now().UTC().Add(-1 * time.Hour)
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1000), LongCryptoAmount: decimal.NewFromFloat(0.02), LongCryptoAsset: "BTC", SettlementDueAt: past})
	n, err := tr.SweepMatured(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("swept=%d want 1", n)
	}
	matured, _ := all.Float.ListMaturedFloat(ctx, time.Now())
	if len(matured) != 0 {
		t.Fatalf("expected 0 matured after sweep, got %d", len(matured))
	}
}

// --- additional coverage ---

func TestTracker_Get(t *testing.T) {
	ctx := context.Background()
	tr, floats := newTracker(t, config.Config{SettlementDays: map[string]int{"USD": 2}})
	_, _ = floats.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(750), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC"})
	pos, err := tr.Get(ctx, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if !pos.ShortFiatAmount.Equal(decimal.NewFromInt(750)) {
		t.Fatalf("short=%s want 750", pos.ShortFiatAmount.String())
	}
	// Get on an unknown currency returns a zero position (no error).
	pos2, err := tr.Get(ctx, "EUR")
	if err != nil {
		t.Fatal(err)
	}
	if !pos2.ShortFiatAmount.Equal(decimal.Zero) {
		t.Fatalf("short=%s want 0", pos2.ShortFiatAmount.String())
	}
}

func TestTracker_OnAggregateFillAddFloatError(t *testing.T) {
	ctx := context.Background()
	tr := New(Deps{Cfg: config.Config{SettlementDays: map[string]int{"USD": 2}}, Floats: errFloatStore{}})
	err := tr.OnAggregateFill(ctx, &store.Batch{ID: uuid.New()}, &store.AggregateOrder{NotionalUSD: decimal.NewFromInt(100), TotalFilled: decimal.NewFromInt(1)}, "USD", "BTC")
	if !errors.Is(err, errFloat) {
		t.Fatalf("err=%v want errFloat", err)
	}
}

func TestTracker_OnAggregateFillOnAdjustHook(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	adjusted := make(chan struct{}, 4)
	tr := New(Deps{
		Cfg:      config.Config{SettlementDays: map[string]int{"USD": 2}},
		Floats:   all.Float,
		OnAdjust: func(context.Context, string, decimal.Decimal, uuid.UUID) { adjusted <- struct{}{} },
	})
	if err := tr.OnAggregateFill(ctx, &store.Batch{ID: uuid.New()}, &store.AggregateOrder{NotionalUSD: decimal.NewFromInt(100), TotalFilled: decimal.NewFromInt(1)}, "USD", "BTC"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-adjusted:
	default:
		t.Fatal("expected OnAdjust to fire")
	}
}

// TestTracker_OnAggregateFillUnitAppendsOutbox verifies that when a Unit
// is configured the float increment and the BuildAdjustOutbox event are
// persisted together via Unit.AddFloatWithOutbox (one outbox entry).
func TestTracker_OnAggregateFillUnitAppendsOutbox(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	tr := New(Deps{
		Cfg:    config.Config{SettlementDays: map[string]int{"USD": 2}},
		Floats: all.Float,
		Unit:   all,
		BuildAdjustOutbox: func(_ context.Context, fiat string, amount decimal.Decimal, batchID uuid.UUID) []*store.OutboxEntry {
			return []*store.OutboxEntry{{Aggregate: "float", EventType: "float.adjust", DedupKey: "fa:" + batchID.String(), Payload: []byte("{}")}}
		},
	})
	batch := &store.Batch{ID: uuid.New()}
	if err := tr.OnAggregateFill(ctx, batch, &store.AggregateOrder{NotionalUSD: decimal.NewFromInt(100), TotalFilled: decimal.NewFromInt(1)}, "USD", "BTC"); err != nil {
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

func TestTracker_SweepMaturedListError(t *testing.T) {
	tr := New(Deps{Cfg: config.Config{}, Floats: errFloatStore{}})
	n, err := tr.SweepMatured(context.Background())
	if !errors.Is(err, errFloat) {
		t.Fatalf("err=%v want errFloat", err)
	}
	if n != 0 {
		t.Fatalf("n=%d want 0", n)
	}
}

func TestTracker_SweepMaturedSettleErrorContinues(t *testing.T) {
	ctx := context.Background()
	// settleErrFloatStore returns one matured float, then errors on SettleFloat.
	tr := New(Deps{Cfg: config.Config{}, Floats: &settleErrFloatStore{}})
	n, err := tr.SweepMatured(ctx)
	if err != nil {
		t.Fatalf("err=%v want nil (logged and continues)", err)
	}
	if n != 0 {
		t.Fatalf("n=%d want 0 (settle failed)", n)
	}
}

func TestTracker_RunSweeperLoopCancelImmediately(t *testing.T) {
	tr, _ := newTracker(t, config.Config{SettlementDays: map[string]int{"USD": 2}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tr.RunSweeperLoop(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
}

func TestTracker_RunSweeperLoopDispatches(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	tr := New(Deps{Cfg: config.Config{}, Floats: all.Float})
	past := time.Now().UTC().Add(-1 * time.Hour)
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(100), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC", SettlementDueAt: past})

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tr.RunSweeperLoop(runCtx, 50*time.Millisecond) }()
	// Wait for a sweep to settle the matured float.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := all.Float.ListMaturedFloat(ctx, time.Now())
		if len(m) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunSweeperLoop did not return after cancel")
	}
	m, _ := all.Float.ListMaturedFloat(ctx, time.Now())
	if len(m) != 0 {
		t.Fatalf("expected matured swept, got %d", len(m))
	}
}

// --- fakes ---

var errFloat = errors.New("float boom")

type errFloatStore struct{}

func (errFloatStore) AddFloat(context.Context, *store.FloatPosition) (*store.FloatPosition, error) {
	return nil, errFloat
}
func (errFloatStore) GetFloat(context.Context, string) (*store.FloatPosition, error) {
	return nil, errFloat
}
func (errFloatStore) ListMaturedFloat(context.Context, time.Time) ([]*store.FloatPosition, error) {
	return nil, errFloat
}
func (errFloatStore) ListFloat(context.Context) ([]*store.FloatPosition, error) {
	return nil, errFloat
}
func (errFloatStore) SettleFloat(context.Context, uuid.UUID) (*store.FloatPosition, error) {
	return nil, errFloat
}

// settleErrFloatStore returns one matured float on ListMaturedFloat but
// errors on SettleFloat.
type settleErrFloatStore struct {
	rows []*store.FloatPosition
}

func (s *settleErrFloatStore) AddFloat(_ context.Context, p *store.FloatPosition) (*store.FloatPosition, error) {
	s.rows = append(s.rows, p)
	return p, nil
}
func (s *settleErrFloatStore) GetFloat(context.Context, string) (*store.FloatPosition, error) {
	return &store.FloatPosition{}, nil
}
func (s *settleErrFloatStore) ListMaturedFloat(_ context.Context, _ time.Time) ([]*store.FloatPosition, error) {
	return []*store.FloatPosition{{ID: uuid.New(), FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(100), SettlementDueAt: time.Now().UTC().Add(-time.Hour)}}, nil
}
func (s *settleErrFloatStore) ListFloat(context.Context) ([]*store.FloatPosition, error) {
	return nil, errFloat
}
func (s *settleErrFloatStore) SettleFloat(context.Context, uuid.UUID) (*store.FloatPosition, error) {
	return nil, errFloat
}
