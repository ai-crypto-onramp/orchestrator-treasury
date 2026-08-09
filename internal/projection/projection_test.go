package projection

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestModel_ProjectedDemand(t *testing.T) {
	m := New(time.Minute)
	m.Observe("BTC", decimal.NewFromInt(1000), time.Now())
	m.Observe("BTC", decimal.NewFromInt(2000), time.Now())
	m.Observe("ETH", decimal.NewFromInt(500), time.Now())
	if got := m.ProjectedDemand("BTC"); !got.Equal(decimal.NewFromInt(3000)) {
		t.Fatalf("btc=%s want 3000", got.String())
	}
	if got := m.ProjectedDemand("ETH"); !got.Equal(decimal.NewFromInt(500)) {
		t.Fatalf("eth=%s want 500", got.String())
	}
	if got := m.ProjectedDemand("XRP"); !got.Equal(decimal.Zero) {
		t.Fatalf("xrp=%s want 0", got.String())
	}
}

func TestModel_EvictsOldSamples(t *testing.T) {
	m := New(time.Minute)
	m.Observe("BTC", decimal.NewFromInt(1000), time.Now().Add(-2*time.Minute))
	m.Observe("BTC", decimal.NewFromInt(2000), time.Now())
	if got := m.ProjectedDemand("BTC"); !got.Equal(decimal.NewFromInt(2000)) {
		t.Fatalf("btc=%s want 2000 (old evicted)", got.String())
	}
}

func TestModel_VelocityPerSecond(t *testing.T) {
	m := New(time.Minute)
	m.Observe("BTC", decimal.NewFromInt(6000), time.Now())
	v := m.VelocityPerSecond("BTC")
	if !v.GreaterThan(decimal.Zero) {
		t.Fatalf("velocity=%s want >0", v.String())
	}
	// 6000 / 60 = 100 per second.
	if v.LessThan(decimal.NewFromInt(99)) || v.GreaterThan(decimal.NewFromInt(101)) {
		t.Fatalf("velocity=%s want ~100", v.String())
	}
}
