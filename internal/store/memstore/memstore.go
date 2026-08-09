// Package memstore is an in-memory implementation of the store interfaces,
// used by unit tests and the in-memory run mode so `go test ./...` passes
// without requiring Docker or a live Postgres. It is safe for concurrent
// use.
package memstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// All is a composite of all in-memory stores.
type All struct {
	Batch      *BatchStore
	Membership *MembershipStore
	Order      *AggregateOrderStore
	Funding    *FundingStore
	Float      *FloatStore
	Rebalance  *RebalancingStore
	Outbox     *OutboxStore
}

// NewAll returns a fully wired set of in-memory stores.
func NewAll() *All {
	return &All{
		Batch:      NewBatchStore(),
		Membership: NewMembershipStore(),
		Order:      NewAggregateOrderStore(),
		Funding:    NewFundingStore(),
		Float:      NewFloatStore(),
		Rebalance:  NewRebalancingStore(),
		Outbox:     NewOutboxStore(),
	}
}

// UpdateBatchStatusWithOutbox transitions a batch and appends outbox
// events. The in-memory store has no real transaction; the operations
// run sequentially (best-effort). The durable postgres implementation
// is transactional.
func (a *All) UpdateBatchStatusWithOutbox(ctx context.Context, id uuid.UUID, from, to store.BatchStatus, mutator func(*store.Batch), events []*store.OutboxEntry) (*store.Batch, bool, error) {
	b, ok, err := a.Batch.UpdateBatchStatus(ctx, id, from, to, mutator)
	if err != nil || !ok {
		return b, ok, err
	}
	for _, e := range events {
		if e == nil {
			continue
		}
		if _, err := a.Outbox.Append(ctx, e); err != nil {
			return nil, false, err
		}
	}
	return b, true, nil
}

// OpenBatchWithOutbox opens (or reuses) a batch and appends outbox events.
// Best-effort sequential (in-memory); the durable postgres implementation
// is transactional. buildEvents is called with the freshly created batch
// (only when a new row is created) and its returned entries are appended.
func (a *All) OpenBatchWithOutbox(ctx context.Context, assetPair string, buildEvents func(*store.Batch) []*store.OutboxEntry) (*store.Batch, error) {
	a.Batch.mu.Lock()
	var existing *store.Batch
	for _, b := range a.Batch.rows {
		if b.AssetPair == assetPair && b.Status == store.BatchOpen {
			existing = b
			break
		}
	}
	a.Batch.mu.Unlock()
	created := existing == nil
	b, err := a.Batch.OpenBatch(ctx, assetPair)
	if err != nil {
		return nil, err
	}
	if !created || buildEvents == nil {
		return b, nil
	}
	for _, e := range buildEvents(b) {
		if e == nil {
			continue
		}
		if _, err := a.Outbox.Append(ctx, e); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// AddFloatWithOutbox increments the float position and appends outbox
// events. Best-effort sequential (in-memory).
func (a *All) AddFloatWithOutbox(ctx context.Context, p *store.FloatPosition, events []*store.OutboxEntry) (*store.FloatPosition, error) {
	out, err := a.Float.AddFloat(ctx, p)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if e == nil {
			continue
		}
		if _, err := a.Outbox.Append(ctx, e); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// CreateFundingWithOutbox persists a funding request and appends outbox
// events. Best-effort sequential (in-memory).
func (a *All) CreateFundingWithOutbox(ctx context.Context, f *store.FundingRequest, events []*store.OutboxEntry) (*store.FundingRequest, error) {
	out, err := a.Funding.CreateFunding(ctx, f)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if e == nil {
			continue
		}
		if _, err := a.Outbox.Append(ctx, e); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// --- BatchStore ---

type BatchStore struct {
	mu   sync.Mutex
	rows []*store.Batch
}

func NewBatchStore() *BatchStore { return &BatchStore{} }

func (s *BatchStore) OpenBatch(_ context.Context, assetPair string) (*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.AssetPair == assetPair && b.Status == store.BatchOpen {
			c := *b
			return &c, nil
		}
	}
	id, _ := uuid.NewV7()
	b := &store.Batch{
		ID:        id,
		AssetPair: assetPair,
		Status:    store.BatchOpen,
		OpenedAt:  time.Now().UTC(),
	}
	s.rows = append(s.rows, b)
	c := *b
	return &c, nil
}

func (s *BatchStore) GetBatch(_ context.Context, id uuid.UUID) (*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.ID == id {
			c := *b
			return &c, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *BatchStore) ListBatches(_ context.Context, from, to time.Time) ([]*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Batch
	for _, b := range s.rows {
		if !from.IsZero() && b.OpenedAt.Before(from) {
			continue
		}
		if !to.IsZero() && b.OpenedAt.After(to) {
			continue
		}
		c := *b
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt.Before(out[j].OpenedAt) })
	return out, nil
}

func (s *BatchStore) ListOpenBatches(_ context.Context) ([]*store.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Batch
	for _, b := range s.rows {
		if b.Status == store.BatchOpen {
			c := *b
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

func (s *BatchStore) UpdateBatchStatus(_ context.Context, id uuid.UUID, from, to store.BatchStatus, mutator func(*store.Batch)) (*store.Batch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.ID == id {
			if b.Status != from {
				return nil, false, nil
			}
			if !from.CanTransitionTo(to) {
				return nil, false, store.ErrConflict
			}
			b.Status = to
			if to == store.BatchClosed && b.ClosedAt.IsZero() {
				b.ClosedAt = time.Now().UTC()
			}
			if mutator != nil {
				mutator(b)
			}
			c := *b
			return &c, true, nil
		}
	}
	return nil, false, store.ErrNotFound
}

func (s *BatchStore) SetBatchNotional(_ context.Context, id uuid.UUID, notional decimal.Decimal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.rows {
		if b.ID == id {
			b.NotionalUSD = notional
			return nil
		}
	}
	return store.ErrNotFound
}

// --- MembershipStore ---

type MembershipStore struct {
	mu   sync.Mutex
	rows []*store.Membership
	byTx map[string]bool
}

func NewMembershipStore() *MembershipStore {
	return &MembershipStore{byTx: map[string]bool{}}
}

func (s *MembershipStore) AddMembership(_ context.Context, m *store.Membership) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTx[m.TxID] {
		return false, nil
	}
	s.byTx[m.TxID] = true
	id, _ := uuid.NewV7()
	c := *m
	c.ID = id
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return true, nil
}

func (s *MembershipStore) ListMemberships(_ context.Context, batchID uuid.UUID) ([]*store.Membership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Membership
	for _, m := range s.rows {
		if m.BatchID == batchID {
			c := *m
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

func (s *MembershipStore) SumNotional(_ context.Context, batchID uuid.UUID) (decimal.Decimal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := decimal.Decimal{}
	for _, m := range s.rows {
		if m.BatchID == batchID {
			sum = sum.Add(m.NotionalUSD)
		}
	}
	return sum, nil
}

func (s *MembershipStore) ExistsByTxID(_ context.Context, txID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byTx[txID], nil
}

// --- AggregateOrderStore ---

type AggregateOrderStore struct {
	mu   sync.Mutex
	rows []*store.AggregateOrder
}

func NewAggregateOrderStore() *AggregateOrderStore { return &AggregateOrderStore{} }

func (s *AggregateOrderStore) CreateOrder(_ context.Context, o *store.AggregateOrder) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == o.BatchID {
			c := *r
			return &c, nil
		}
	}
	id, _ := uuid.NewV7()
	c := *o
	c.ID = id
	if c.Side == "" {
		c.Side = "BUY"
	}
	if c.Status == "" {
		c.Status = store.AggregateExecuting
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *AggregateOrderStore) GetOrderByBatch(_ context.Context, batchID uuid.UUID) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == batchID {
			c := *r
			return &c, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *AggregateOrderStore) ListOrders(_ context.Context, status string) ([]*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.AggregateOrder
	for _, r := range s.rows {
		if status != "" && string(r.Status) != status {
			continue
		}
		c := *r
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

func (s *AggregateOrderStore) UpdateOrderFill(_ context.Context, batchID uuid.UUID, fillPrice, totalFilled decimal.Decimal, venueRoutes []store.VenueRoute) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == batchID {
			r.FillPrice = fillPrice
			r.TotalFilled = totalFilled
			r.VenueRoutes = venueRoutes
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *AggregateOrderStore) SettleOrder(_ context.Context, batchID uuid.UUID, hedgedNotional decimal.Decimal) (*store.AggregateOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.BatchID == batchID {
			r.Status = store.AggregateSettled
			r.HedgedNotional = hedgedNotional
			r.SettledAt = time.Now().UTC()
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

// --- FundingStore ---

type FundingStore struct {
	mu   sync.Mutex
	rows []*store.FundingRequest
}

func NewFundingStore() *FundingStore { return &FundingStore{} }

func (s *FundingStore) CreateFunding(_ context.Context, f *store.FundingRequest) (*store.FundingRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, _ := uuid.NewV7()
	c := *f
	c.ID = id
	if c.Status == "" {
		c.Status = store.FundingPending
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *FundingStore) GetFunding(_ context.Context, id uuid.UUID) (*store.FundingRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *FundingStore) UpdateFundingStatus(_ context.Context, id uuid.UUID, status store.FundingStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.Status = status
			if status == store.FundingCompleted || status == store.FundingRejected {
				r.CompletedAt = time.Now().UTC()
			}
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *FundingStore) ListFunding(_ context.Context, status string) ([]*store.FundingRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.FundingRequest
	for _, r := range s.rows {
		if status == "" || string(r.Status) == status {
			c := *r
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

// --- FloatStore ---

type FloatStore struct {
	mu   sync.Mutex
	rows []*store.FloatPosition
}

func NewFloatStore() *FloatStore { return &FloatStore{} }

func (s *FloatStore) AddFloat(_ context.Context, p *store.FloatPosition) (*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Accumulate into the latest unsettled row for the currency.
	for _, r := range s.rows {
		if r.FiatCurrency == p.FiatCurrency && !r.Settled {
			r.ShortFiatAmount = r.ShortFiatAmount.Add(p.ShortFiatAmount)
			r.LongCryptoAmount = r.LongCryptoAmount.Add(p.LongCryptoAmount)
			if p.LongCryptoAsset != "" {
				r.LongCryptoAsset = p.LongCryptoAsset
			}
			if !p.SettlementDueAt.IsZero() {
				r.SettlementDueAt = p.SettlementDueAt
			}
			r.UpdatedAt = time.Now().UTC()
			return &*r, nil
		}
	}
	id, _ := uuid.NewV7()
	c := *p
	c.ID = id
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	c.UpdatedAt = time.Now().UTC()
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *FloatStore) GetFloat(_ context.Context, fiatCurrency string) (*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var agg *store.FloatPosition
	for _, r := range s.rows {
		if r.FiatCurrency != fiatCurrency {
			continue
		}
		if agg == nil {
			agg = &store.FloatPosition{FiatCurrency: fiatCurrency, LongCryptoAsset: r.LongCryptoAsset}
		}
		agg.ShortFiatAmount = agg.ShortFiatAmount.Add(r.ShortFiatAmount)
		agg.LongCryptoAmount = agg.LongCryptoAmount.Add(r.LongCryptoAmount)
		if r.SettlementDueAt.After(agg.SettlementDueAt) {
			agg.SettlementDueAt = r.SettlementDueAt
		}
	}
	if agg == nil {
		return &store.FloatPosition{FiatCurrency: fiatCurrency}, nil
	}
	return agg, nil
}

func (s *FloatStore) ListFloat(_ context.Context) ([]*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byCcy := map[string]*store.FloatPosition{}
	var order []string
	for _, r := range s.rows {
		agg, ok := byCcy[r.FiatCurrency]
		if !ok {
			agg = &store.FloatPosition{FiatCurrency: r.FiatCurrency, LongCryptoAsset: r.LongCryptoAsset}
			byCcy[r.FiatCurrency] = agg
			order = append(order, r.FiatCurrency)
		}
		agg.ShortFiatAmount = agg.ShortFiatAmount.Add(r.ShortFiatAmount)
		agg.LongCryptoAmount = agg.LongCryptoAmount.Add(r.LongCryptoAmount)
		if r.LongCryptoAsset != "" {
			agg.LongCryptoAsset = r.LongCryptoAsset
		}
		if r.SettlementDueAt.After(agg.SettlementDueAt) {
			agg.SettlementDueAt = r.SettlementDueAt
		}
	}
	sort.Strings(order)
	out := make([]*store.FloatPosition, 0, len(order))
	for _, c := range order {
		out = append(out, byCcy[c])
	}
	return out, nil
}

func (s *FloatStore) ListMaturedFloat(_ context.Context, before time.Time) ([]*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.FloatPosition
	for _, r := range s.rows {
		if r.Settled {
			continue
		}
		if r.SettlementDueAt.IsZero() {
			continue
		}
		if !r.SettlementDueAt.After(before) {
			c := *r
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

func (s *FloatStore) SettleFloat(_ context.Context, id uuid.UUID) (*store.FloatPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.Settled = true
			r.UpdatedAt = time.Now().UTC()
			return &*r, nil
		}
	}
	return nil, store.ErrNotFound
}

// --- RebalancingStore ---

type RebalancingStore struct {
	mu   sync.Mutex
	rows []*store.RebalancingJob
}

func NewRebalancingStore() *RebalancingStore { return &RebalancingStore{} }

func (s *RebalancingStore) CreateJob(_ context.Context, j *store.RebalancingJob) (*store.RebalancingJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, _ := uuid.NewV7()
	c := *j
	c.ID = id
	if c.Status == "" {
		c.Status = store.RebalancePending
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return &c, nil
}

func (s *RebalancingStore) ListJobs(_ context.Context, status string) ([]*store.RebalancingJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.RebalancingJob
	for _, r := range s.rows {
		if status == "" || string(r.Status) == status {
			c := *r
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out, nil
}

func (s *RebalancingStore) UpdateJobStatus(_ context.Context, id uuid.UUID, status store.RebalanceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.Status = status
			if status == store.RebalanceCompleted || status == store.RebalanceRejected {
				r.CompletedAt = time.Now().UTC()
			}
			return nil
		}
	}
	return store.ErrNotFound
}

// --- OutboxStore ---

type OutboxStore struct {
	mu   sync.Mutex
	rows []*store.OutboxEntry
	seen map[string]bool
}

func NewOutboxStore() *OutboxStore { return &OutboxStore{seen: map[string]bool{}} }

func (s *OutboxStore) Append(_ context.Context, e *store.OutboxEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[e.DedupKey] {
		return false, nil
	}
	s.seen[e.DedupKey] = true
	id, _ := uuid.NewV7()
	c := *e
	c.ID = id
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	s.rows = append(s.rows, &c)
	return true, nil
}

func (s *OutboxStore) ListPending(_ context.Context, limit int) ([]*store.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.OutboxEntry
	for _, r := range s.rows {
		if r.EmittedAt.IsZero() {
			c := *r
			out = append(out, &c)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *OutboxStore) MarkEmitted(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.ID == id {
			r.EmittedAt = time.Now().UTC()
			return nil
		}
	}
	return store.ErrNotFound
}

// Snapshot returns a copy of all outbox entries (test helper).
func (s *OutboxStore) Snapshot() []*store.OutboxEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*store.OutboxEntry, len(s.rows))
	for i, r := range s.rows {
		c := *r
		out[i] = &c
	}
	return out
}
