package funding

import (
	"context"
	"log"
	"time"

	"github.com/shopspring/decimal"
)

// RunRebalanceLoop periodically checks projected demand against the
// per-asset target and creates a rebalancing job when projected balance
// drifts below target. It blocks until ctx is canceled.
//
// The "projected balance" is approximated as target - projectedDemand:
// when demand outpaces the configured target, a top-up funding request
// is created. This is the capital efficiency policy: only pre-fund to
// projected demand, never hold excess beyond policy.
func (m *Manager) RunRebalanceLoop(ctx context.Context, interval time.Duration, asset, walletID string) error {
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
			m.checkAndRebalance(ctx, asset, walletID)
		}
	}
}

// checkAndRebalance creates a funding request when projected demand
// exceeds the configured target buffer (i.e. the hot wallet would fall
// below target).
func (m *Manager) checkAndRebalance(ctx context.Context, asset, walletID string) {
	target := m.deps.Cfg.HotWalletTargetFor(asset)
	if target.LessThanOrEqual(decimal.Zero) {
		return
	}
	demand := decimal.Decimal{}
	if m.deps.Projection != nil {
		demand = m.deps.Projection.ProjectedDemand(asset)
	}
	// If projected demand exceeds the target, top up by the shortfall.
	shortfall := demand.Sub(target)
	if !shortfall.GreaterThan(decimal.Zero) {
		return
	}
	maxAmt := decimal.NewFromInt(MaxFundingAmount)
	if shortfall.GreaterThan(maxAmt) {
		shortfall = maxAmt
	}
	if _, err := m.CreateFundingRequest(ctx, walletID, asset, shortfall, "rebalance-loop"); err != nil {
		log.Printf("rebalance-loop: asset=%s shortfall=%s: %v", asset, shortfall.String(), err)
	}
}
