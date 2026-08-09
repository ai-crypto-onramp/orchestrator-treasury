// Package projection computes a rolling demand projection from inbound
// order velocity. The model keeps a time-windowed count of memberships
// per asset and produces a projected forward demand used to trigger hot
// wallet pre-funding.
package projection

import (
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// Model is a rolling-window demand projection per asset. It is safe for
// concurrent use.
type Model struct {
	mu      sync.Mutex
	window  time.Duration
	samples map[string][]sample
}

type sample struct {
	at       time.Time
	notional decimal.Decimal
}

// New returns a model with the given rolling window.
func New(window time.Duration) *Model {
	return &Model{window: window, samples: map[string][]sample{}}
}

// Observe records a notional amount for an asset at the given time (now
// if zero).
func (m *Model) Observe(asset string, notional decimal.Decimal, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.samples[asset] = append(m.samples[asset], sample{at: at, notional: notional})
	m.evictLocked(asset, at)
}

// ProjectedDemand returns the projected forward demand for an asset over
// the next window, computed as the rolling-window velocity scaled to the
// window. Velocity = sum of notional observed in the trailing window;
// projected forward demand = velocity (i.e. assume the recent rate
// continues for one more window).
func (m *Model) ProjectedDemand(asset string) decimal.Decimal {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictLocked(asset, now)
	sum := decimal.Decimal{}
	for _, s := range m.samples[asset] {
		sum = sum.Add(s.notional)
	}
	return sum
}

// VelocityPerSecond returns the notional per second for the asset over
// the trailing window.
func (m *Model) VelocityPerSecond(asset string) decimal.Decimal {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictLocked(asset, now)
	sum := decimal.Decimal{}
	for _, s := range m.samples[asset] {
		sum = sum.Add(s.notional)
	}
	if m.window <= 0 {
		return decimal.Decimal{}
	}
	return sum.Div(decimal.NewFromFloat(m.window.Seconds()))
}

func (m *Model) evictLocked(asset string, now time.Time) {
	cutoff := now.Add(-m.window)
	kept := m.samples[asset][:0]
	for _, s := range m.samples[asset] {
		if s.at.After(cutoff) {
			kept = append(kept, s)
		}
	}
	m.samples[asset] = kept
}
