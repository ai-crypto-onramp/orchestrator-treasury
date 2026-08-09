package funding

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/projection"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

type mgrFixture struct {
	mgr  *Manager
	proj *projection.Model
}

func newMgrWithCfg(t *testing.T, cfg config.Config) mgrFixture {
	t.Helper()
	all := memstore.NewAll()
	proj := projection.New(time.Minute)
	mgr := New(Deps{
		Cfg:        cfg,
		Funding:    all.Funding,
		Rebalance:  all.Rebalance,
		Wallet:     clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:       idempotency.NewMem(),
		Projection: proj,
	})
	return mgrFixture{mgr: mgr, proj: proj}
}

func TestManager_RunRebalanceLoop_CreatesFundingWhenDemandExceedsTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newMgrWithCfg(t, config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}})
	f.proj.Observe("BTC", decimal.NewFromInt(50), time.Now())
	go func() {
		_ = f.mgr.RunRebalanceLoop(ctx, 50*time.Millisecond, "BTC", "hot1")
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		frs, _ := f.mgr.ListFunding(ctx, "")
		if len(frs) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected a funding request from rebalance loop")
}

func TestManager_RunRebalanceLoop_NoopWhenDemandBelowTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newMgrWithCfg(t, config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(1000)}})
	f.proj.Observe("BTC", decimal.NewFromInt(5), time.Now())
	go func() {
		_ = f.mgr.RunRebalanceLoop(ctx, 50*time.Millisecond, "BTC", "hot1")
	}()
	time.Sleep(150 * time.Millisecond)
	frs, _ := f.mgr.ListFunding(ctx, "")
	if len(frs) != 0 {
		t.Fatalf("expected 0 funding requests, got %d", len(frs))
	}
}
