// Package store defines the storage interfaces and record types for the
// Treasury Orchestration service. The package exposes interfaces
// (BatchStore, MembershipStore, AggregateOrderStore, FundingStore,
// FloatStore, RebalancingStore, OutboxStore) so the service can run against
// either a real PostgreSQL backend (internal/store/postgres) or an
// in-memory mock (internal/store/memstore) used by tests.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// BatchStatus enumerates the lifecycle states of a batch.
type BatchStatus string

const (
	BatchOpen      BatchStatus = "OPEN"
	BatchClosed    BatchStatus = "CLOSED"
	BatchExecuting BatchStatus = "EXECUTING"
	BatchSettled   BatchStatus = "SETTLED"
)

// CanTransitionTo reports whether a batch may move from s to t.
func (s BatchStatus) CanTransitionTo(t BatchStatus) bool {
	switch s {
	case BatchOpen:
		return t == BatchClosed
	case BatchClosed:
		return t == BatchExecuting
	case BatchExecuting:
		return t == BatchSettled
	}
	return false
}

// AggregateStatus enumerates aggregate parent-order lifecycle states.
type AggregateStatus string

const (
	AggregateExecuting AggregateStatus = "EXECUTING"
	AggregateSettled   AggregateStatus = "SETTLED"
	AggregateFailed    AggregateStatus = "FAILED"
)

// FundingStatus enumerates funding-request states.
type FundingStatus string

const (
	FundingPending   FundingStatus = "PENDING"
	FundingExecuting FundingStatus = "EXECUTING"
	FundingCompleted FundingStatus = "COMPLETED"
	FundingRejected  FundingStatus = "REJECTED"
)

// RebalanceStatus enumerates rebalancing-job states.
type RebalanceStatus string

const (
	RebalancePending   RebalanceStatus = "PENDING"
	RebalanceExecuting RebalanceStatus = "EXECUTING"
	RebalanceCompleted RebalanceStatus = "COMPLETED"
	RebalanceRejected  RebalanceStatus = "REJECTED"
)

// Batch is one aggregate parent order.
type Batch struct {
	ID          uuid.UUID       `json:"id"`
	AssetPair   string          `json:"asset_pair"`
	Status      BatchStatus     `json:"status"`
	NotionalUSD decimal.Decimal `json:"notional_usd"`
	OpenedAt    time.Time       `json:"opened_at"`
	ClosedAt    time.Time       `json:"closed_at"`
}

// Membership links a single Transaction Orchestrator tx into a batch.
type Membership struct {
	ID           uuid.UUID       `json:"id"`
	BatchID      uuid.UUID       `json:"batch_id"`
	TxID         string          `json:"tx_id"`
	Amount       decimal.Decimal `json:"amount"`
	Asset        string          `json:"asset"`
	FiatCurrency string          `json:"fiat_currency"`
	NotionalUSD  decimal.Decimal `json:"notional_usd"`
	UserID       string          `json:"user_id"`
	CreatedAt    time.Time       `json:"created_at"`
}

// AggregateOrder is the executed parent order against Liquidity Routing.
type AggregateOrder struct {
	ID             uuid.UUID       `json:"id"`
	BatchID        uuid.UUID       `json:"batch_id"`
	AssetPair      string          `json:"asset_pair"`
	Side           string          `json:"side"`
	NotionalUSD    decimal.Decimal `json:"notional_usd"`
	VenueRoutes    []VenueRoute    `json:"venue_routes"`
	FillPrice      decimal.Decimal `json:"fill_price"`
	TotalFilled    decimal.Decimal `json:"total_filled"`
	HedgedNotional decimal.Decimal `json:"hedged_notional"`
	Status         AggregateStatus `json:"status"`
	CreatedAt      time.Time       `json:"created_at"`
	SettledAt      time.Time       `json:"settled_at"`
}

// VenueRoute describes one slice of an aggregate fill.
type VenueRoute struct {
	Venue string          `json:"venue"`
	Share decimal.Decimal `json:"share"`
	Price decimal.Decimal `json:"price"`
}

// FundingRequest is a hot-wallet funding instruction.
type FundingRequest struct {
	ID          uuid.UUID       `json:"id"`
	WalletID    string          `json:"wallet_id"`
	Asset       string          `json:"asset"`
	Amount      decimal.Decimal `json:"amount"`
	Status      FundingStatus   `json:"status"`
	SourceVenue string          `json:"source_venue"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt time.Time       `json:"completed_at"`
}

// FloatPosition is the per-fiat-currency T+0 long-crypto / short-fiat
// position created by fronting crypto before fiat settles.
type FloatPosition struct {
	ID               uuid.UUID       `json:"id"`
	FiatCurrency     string          `json:"fiat_currency"`
	ShortFiatAmount  decimal.Decimal `json:"short_fiat_amount"`
	LongCryptoAmount decimal.Decimal `json:"long_crypto_amount"`
	LongCryptoAsset  string          `json:"long_crypto_asset"`
	SettlementDueAt  time.Time       `json:"settlement_due_at"`
	Settled          bool            `json:"settled"`
	BatchID          uuid.UUID       `json:"batch_id"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// RebalancingJob is a persisted wallet/venue rebalance record.
type RebalancingJob struct {
	ID          uuid.UUID       `json:"id"`
	FromRef     string          `json:"from"`
	ToRef       string          `json:"to"`
	Asset       string          `json:"asset"`
	Amount      decimal.Decimal `json:"amount"`
	Status      RebalanceStatus `json:"status"`
	Reason      string          `json:"reason"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt time.Time       `json:"completed_at"`
}

// OutboxEntry is a deduped outbound event awaiting ledger/audit emission.
type OutboxEntry struct {
	ID        uuid.UUID `json:"id"`
	Aggregate string    `json:"aggregate"`
	EventType string    `json:"event_type"`
	DedupKey  string    `json:"dedup_key"`
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
	EmittedAt time.Time `json:"emitted_at"`
}

// BatchStore persists batches.
type BatchStore interface {
	OpenBatch(ctx context.Context, assetPair string) (*Batch, error)
	GetBatch(ctx context.Context, id uuid.UUID) (*Batch, error)
	ListBatches(ctx context.Context, from, to time.Time) ([]*Batch, error)
	ListOpenBatches(ctx context.Context) ([]*Batch, error)
	UpdateBatchStatus(ctx context.Context, id uuid.UUID, from, to BatchStatus, mutator func(*Batch)) (*Batch, bool, error)
	SetBatchNotional(ctx context.Context, id uuid.UUID, notional decimal.Decimal) error
}

// MembershipStore persists tx memberships.
type MembershipStore interface {
	AddMembership(ctx context.Context, m *Membership) (bool, error)
	ListMemberships(ctx context.Context, batchID uuid.UUID) ([]*Membership, error)
	SumNotional(ctx context.Context, batchID uuid.UUID) (decimal.Decimal, error)
	ExistsByTxID(ctx context.Context, txID string) (bool, error)
}

// AggregateOrderStore persists aggregate parent orders.
type AggregateOrderStore interface {
	CreateOrder(ctx context.Context, o *AggregateOrder) (*AggregateOrder, error)
	GetOrderByBatch(ctx context.Context, batchID uuid.UUID) (*AggregateOrder, error)
	ListOrders(ctx context.Context, status string) ([]*AggregateOrder, error)
	UpdateOrderFill(ctx context.Context, batchID uuid.UUID, fillPrice, totalFilled decimal.Decimal, venueRoutes []VenueRoute) (*AggregateOrder, error)
	SettleOrder(ctx context.Context, batchID uuid.UUID, hedgedNotional decimal.Decimal) (*AggregateOrder, error)
}

// FundingStore persists hot-wallet funding requests.
type FundingStore interface {
	CreateFunding(ctx context.Context, f *FundingRequest) (*FundingRequest, error)
	GetFunding(ctx context.Context, id uuid.UUID) (*FundingRequest, error)
	UpdateFundingStatus(ctx context.Context, id uuid.UUID, status FundingStatus) error
	ListFunding(ctx context.Context, status string) ([]*FundingRequest, error)
}

// FloatStore persists per-currency float positions.
type FloatStore interface {
	AddFloat(ctx context.Context, p *FloatPosition) (*FloatPosition, error)
	GetFloat(ctx context.Context, fiatCurrency string) (*FloatPosition, error)
	ListFloat(ctx context.Context) ([]*FloatPosition, error)
	ListMaturedFloat(ctx context.Context, before time.Time) ([]*FloatPosition, error)
	SettleFloat(ctx context.Context, id uuid.UUID) (*FloatPosition, error)
}

// RebalancingStore persists rebalancing jobs.
type RebalancingStore interface {
	CreateJob(ctx context.Context, j *RebalancingJob) (*RebalancingJob, error)
	ListJobs(ctx context.Context, status string) ([]*RebalancingJob, error)
	UpdateJobStatus(ctx context.Context, id uuid.UUID, status RebalanceStatus) error
}

// OutboxStore persists events for at-least-once, deduped ledger/audit
// emission.
type OutboxStore interface {
	Append(ctx context.Context, e *OutboxEntry) (bool, error)
	ListPending(ctx context.Context, limit int) ([]*OutboxEntry, error)
	MarkEmitted(ctx context.Context, id uuid.UUID) error
}

// Unit performs a state change and an outbox append inside a single DB
// transaction, so a crash between the state change and the outbox insert
// cannot leave a persisted state change without its audit/ledger event.
// Implementations: *postgres.DB (real tx), *memstore.All (in-memory;
// best-effort sequential).
type Unit interface {
	UpdateBatchStatusWithOutbox(ctx context.Context, id uuid.UUID, from, to BatchStatus, mutator func(*Batch), events []*OutboxEntry) (*Batch, bool, error)
	OpenBatchWithOutbox(ctx context.Context, assetPair string, buildEvents func(*Batch) []*OutboxEntry) (*Batch, error)
	AddFloatWithOutbox(ctx context.Context, p *FloatPosition, events []*OutboxEntry) (*FloatPosition, error)
	CreateFundingWithOutbox(ctx context.Context, f *FundingRequest, events []*OutboxEntry) (*FundingRequest, error)
}

// ErrNotFound is returned when a row lookup misses.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when a conditional update does not match.
var ErrConflict = errors.New("conflict")

// IsNotFound reports whether err is ErrNotFound.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
