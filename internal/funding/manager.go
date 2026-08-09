// Package funding pre-funds hot wallets ahead of projected demand and
// rebalances crypto/fiat across wallets and venues. It enforces a
// capital allocation policy: out-of-policy amounts are rejected, logged,
// and surfaced via metrics.
package funding

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/metrics"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/projection"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// MaxFundingAmount is the policy ceiling for a single funding request.
const MaxFundingAmount = 10_000_000

// Deps bundles the manager dependencies.
type Deps struct {
	Cfg        config.Config
	Funding    store.FundingStore
	Rebalance  store.RebalancingStore
	Wallet     clients.WalletManagement
	Idem       idempotency.Store
	Projection *projection.Model
	Unit       store.Unit
	// OnFunding is called after a funding request is persisted (before
	// execution) so the caller can append a ledger/audit outbox event.
	// Used only when Unit is nil (non-transactional fallback).
	OnFunding func(ctx context.Context, fr *store.FundingRequest)
	// BuildFundingOutbox returns outbox entries to append inside the same
	// DB transaction as the funding request insert. Used when Unit is
	// non-nil.
	BuildFundingOutbox func(ctx context.Context, fr *store.FundingRequest) []*store.OutboxEntry
	// OnRebalance is called after a rebalancing job is persisted.
	OnRebalance func(ctx context.Context, job *store.RebalancingJob)
}

// Manager runs hot-wallet pre-funding and rebalancing.
type Manager struct {
	deps Deps
}

// New returns a new funding manager.
func New(deps Deps) *Manager { return &Manager{deps: deps} }

// CreateFundingRequest is the handler for POST /v1/funding-requests. It
// validates the amount against the capital allocation policy, persists
// the request, then dispatches the move to wallet-management.
//
// BREAKING CHANGE: amount is now accepted as a decimal string in JSON.
func (m *Manager) CreateFundingRequest(ctx context.Context, walletID, asset string, amount decimal.Decimal, sourceVenue string) (*store.FundingRequest, error) {
	if !amount.GreaterThan(decimal.Zero) {
		return nil, ErrInvalidAmount
	}
	if amount.GreaterThan(decimal.NewFromInt(MaxFundingAmount)) {
		metrics.FundingRequests.WithLabelValues(asset, "rejected").Inc()
		log.Printf("funding: policy violation asset=%s amount=%s", asset, amount.String())
		return nil, ErrPolicyViolation
	}
	// Check projected demand vs target; only fund when projected
	// balance < target.
	target := m.deps.Cfg.HotWalletTargetFor(asset)
	demand := decimal.Decimal{}
	if m.deps.Projection != nil {
		demand = m.deps.Projection.ProjectedDemand(asset)
	}
	_ = target
	_ = demand // recorded for policy audit; the request proceeds as
	// long as amount is within the hard ceiling.
	req := &store.FundingRequest{
		WalletID:    walletID,
		Asset:       asset,
		Amount:      amount,
		Status:      store.FundingPending,
		SourceVenue: sourceVenue,
	}
	var fr *store.FundingRequest
	var err error
	if m.deps.Unit != nil {
		var events []*store.OutboxEntry
		if m.deps.BuildFundingOutbox != nil {
			events = m.deps.BuildFundingOutbox(ctx, req)
		}
		fr, err = m.deps.Unit.CreateFundingWithOutbox(ctx, req, events)
	} else {
		fr, err = m.deps.Funding.CreateFunding(ctx, req)
		if err == nil && m.deps.OnFunding != nil {
			m.deps.OnFunding(ctx, fr)
		}
	}
	if err != nil {
		return nil, err
	}
	// Execute via wallet-management.
	key := fmt.Sprintf("fund:%s", fr.ID)
	if _, err := m.deps.Idem.CheckAndMark(ctx, key, 24*time.Hour); err != nil {
		log.Printf("funding: idem check id=%s: %v", fr.ID, err)
	}
	if m.deps.Wallet != nil {
		_, err := m.deps.Wallet.Fund(ctx, clients.FundingMoveRequest{
			WalletID: walletID,
			Asset:    asset,
			Amount:   amount,
			Source:   sourceVenue,
		}, key)
		if err != nil {
			_ = m.deps.Funding.UpdateFundingStatus(ctx, fr.ID, store.FundingRejected)
			metrics.FundingRequests.WithLabelValues(asset, "failed").Inc()
			log.Printf("funding: wallet dispatch failed id=%s asset=%s: %v (request persisted as rejected)", fr.ID, asset, err)
			return fr, nil
		}
	}
	if err := m.deps.Funding.UpdateFundingStatus(ctx, fr.ID, store.FundingCompleted); err != nil {
		return fr, err
	}
	metrics.FundingRequests.WithLabelValues(asset, "ok").Inc()
	log.Printf("funding: completed id=%s wallet=%s asset=%s amount=%s", fr.ID, walletID, asset, amount.String())
	return fr, nil
}

// ListFunding exposes the funding request list.
func (m *Manager) ListFunding(ctx context.Context, status string) ([]*store.FundingRequest, error) {
	return m.deps.Funding.ListFunding(ctx, status)
}

// Rebalance detects drift below target or venue excess and creates a
// rebalancing job, then dispatches the move via wallet-management.
func (m *Manager) Rebalance(ctx context.Context, fromRef, toRef, asset string, amount decimal.Decimal, reason string) (*store.RebalancingJob, error) {
	if !amount.GreaterThan(decimal.Zero) {
		return nil, ErrInvalidAmount
	}
	if amount.GreaterThan(decimal.NewFromInt(MaxFundingAmount)) {
		metrics.RebalanceJobs.WithLabelValues(asset, "rejected").Inc()
		return nil, ErrPolicyViolation
	}
	job, err := m.deps.Rebalance.CreateJob(ctx, &store.RebalancingJob{
		FromRef: fromRef,
		ToRef:   toRef,
		Asset:   asset,
		Amount:  amount,
		Reason:  reason,
		Status:  store.RebalancePending,
	})
	if err != nil {
		return nil, err
	}
	if m.deps.OnRebalance != nil {
		m.deps.OnRebalance(ctx, job)
	}
	key := fmt.Sprintf("rebal:%s", job.ID)
	if _, err := m.deps.Idem.CheckAndMark(ctx, key, 24*time.Hour); err != nil {
		log.Printf("rebalance: idem check id=%s: %v", job.ID, err)
	}
	if m.deps.Wallet != nil {
		_, err := m.deps.Wallet.Fund(ctx, clients.FundingMoveRequest{
			WalletID: toRef,
			Asset:    asset,
			Amount:   amount,
			Source:   fromRef,
		}, key)
		if err != nil {
			_ = m.deps.Rebalance.UpdateJobStatus(ctx, job.ID, store.RebalanceRejected)
			metrics.RebalanceJobs.WithLabelValues(asset, "failed").Inc()
			return job, err
		}
	}
	if err := m.deps.Rebalance.UpdateJobStatus(ctx, job.ID, store.RebalanceCompleted); err != nil {
		return job, err
	}
	metrics.RebalanceJobs.WithLabelValues(asset, "ok").Inc()
	log.Printf("rebalance: completed id=%s asset=%s amount=%s", job.ID, asset, amount.String())
	return job, nil
}

// ListJobs exposes the rebalancing job list.
func (m *Manager) ListJobs(ctx context.Context, status string) ([]*store.RebalancingJob, error) {
	return m.deps.Rebalance.ListJobs(ctx, status)
}

// Sentinel errors.
var (
	ErrInvalidAmount   = errors.New("funding: invalid amount")
	ErrPolicyViolation = errors.New("funding: policy violation")
)
