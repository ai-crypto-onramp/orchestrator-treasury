// Package postgres implements the store interfaces against a real
// PostgreSQL database using pgx. It is exercised by the integration suite
// via docker-compose; unit tests use internal/store/memstore.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/migrations"
)

// DB wraps a pgxpool.Pool and exposes the store implementations.
type DB struct {
	pool       *pgxpool.Pool
	batch      *BatchStore
	membership *MembershipStore
	order      *AggregateOrderStore
	funding    *FundingStore
	float      *FloatStore
	rebalance  *RebalancingStore
	outbox     *OutboxStore
}

// Open connects to dsn, pings, runs migrations, and returns a wired DB.
func Open(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	runner := migrations.NewRunnerWithQuery(
		func(c context.Context, q string, args ...any) error {
			_, err := pool.Exec(c, q, args...)
			return err
		},
		func(c context.Context, version string) (bool, error) {
			var exists bool
			err := pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
			return exists, err
		},
	)
	if err := runner.Up(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	d := &DB{pool: pool}
	d.batch = &BatchStore{db: pool}
	d.membership = &MembershipStore{db: pool}
	d.order = &AggregateOrderStore{db: pool}
	d.funding = &FundingStore{db: pool}
	d.float = &FloatStore{db: pool}
	d.rebalance = &RebalancingStore{db: pool}
	d.outbox = &OutboxStore{db: pool}
	return d, nil
}

// Close releases the pool.
func (d *DB) Close() error {
	d.pool.Close()
	return nil
}

// Batch returns the BatchStore.
func (d *DB) Batch() store.BatchStore { return d.batch }

// Membership returns the MembershipStore.
func (d *DB) Membership() store.MembershipStore { return d.membership }

// Order returns the AggregateOrderStore.
func (d *DB) Order() store.AggregateOrderStore { return d.order }

// Funding returns the FundingStore.
func (d *DB) Funding() store.FundingStore { return d.funding }

// Float returns the FloatStore.
func (d *DB) Float() store.FloatStore { return d.float }

// Rebalance returns the RebalancingStore.
func (d *DB) Rebalance() store.RebalancingStore { return d.rebalance }

// Outbox returns the OutboxStore.
func (d *DB) Outbox() store.OutboxStore { return d.outbox }

// Ping performs a round-trip to the database.
func (d *DB) Ping(ctx context.Context) error { return d.pool.Ping(ctx) }

// Pool returns the underlying pgxpool pool (used by integration tests to
// run raw SQL via the migration runner).
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

type poolDB = *pgxpool.Pool

// --- BatchStore ---

type BatchStore struct{ db poolDB }

func (s *BatchStore) OpenBatch(ctx context.Context, assetPair string) (*store.Batch, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	b, _, err := openBatchInTx(ctx, tx, assetPair)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

// OpenBatchWithOutbox opens (or reuses) a batch and appends outbox events
// inside the same DB transaction. buildEvents is called with the freshly
// created batch (only when a new row is inserted) and its returned entries
// are appended in the same tx. If an existing open batch is reused,
// buildEvents is not called and no events are appended.
func (d *DB) OpenBatchWithOutbox(ctx context.Context, assetPair string, buildEvents func(*store.Batch) []*store.OutboxEntry) (*store.Batch, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	b, created, err := openBatchInTx(ctx, tx, assetPair)
	if err != nil {
		return nil, err
	}
	if created && buildEvents != nil {
		for _, e := range buildEvents(b) {
			if e == nil {
				continue
			}
			if _, err := appendOutboxInTx(ctx, tx, e); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

// openBatchInTx returns the open batch for assetPair, creating one if none
// exists. The second return value reports whether a new row was inserted.
func openBatchInTx(ctx context.Context, tx pgx.Tx, assetPair string) (*store.Batch, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT id, asset_pair, status, notional_usd, opened_at, COALESCE(closed_at, now())
		 FROM batches WHERE asset_pair=$1 AND status='OPEN' LIMIT 1`, assetPair)
	var b store.Batch
	var closedAt time.Time
	var notional string
	if err := row.Scan(&b.ID, &b.AssetPair, &b.Status, &notional, &b.OpenedAt, &closedAt); err == nil {
		b.ClosedAt = closedAt
		b.NotionalUSD, _ = decimal.NewFromString(notional)
		return &b, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	id, _ := uuid.NewV7()
	if err := tx.QueryRow(ctx,
		`INSERT INTO batches (id, asset_pair, status, notional_usd, opened_at)
		 VALUES ($1, $2, 'OPEN', 0, now()) RETURNING opened_at`,
		id, assetPair).Scan(&b.OpenedAt); err != nil {
		return nil, false, err
	}
	b.ID = id
	b.AssetPair = assetPair
	b.Status = store.BatchOpen
	return &b, true, nil
}

func (s *BatchStore) GetBatch(ctx context.Context, id uuid.UUID) (*store.Batch, error) {
	row := s.db.QueryRow(ctx,
		`SELECT id, asset_pair, status, notional_usd, opened_at, COALESCE(closed_at, now())
		 FROM batches WHERE id=$1`, id)
	var b store.Batch
	var closedAt time.Time
	var notional string
	if err := row.Scan(&b.ID, &b.AssetPair, &b.Status, &notional, &b.OpenedAt, &closedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	b.ClosedAt = closedAt
	b.NotionalUSD, _ = decimal.NewFromString(notional)
	return &b, nil
}

func (s *BatchStore) ListBatches(ctx context.Context, from, to time.Time) ([]*store.Batch, error) {
	q := `SELECT id, asset_pair, status, notional_usd, opened_at, COALESCE(closed_at, now()) FROM batches`
	args := []any{}
	if !from.IsZero() || !to.IsZero() {
		conds := []string{}
		if !from.IsZero() {
			conds = append(conds, fmt.Sprintf("opened_at >= $%d", len(args)+1))
			args = append(args, from)
		}
		if !to.IsZero() {
			conds = append(conds, fmt.Sprintf("opened_at <= $%d", len(args)+1))
			args = append(args, to)
		}
		q += " WHERE " + joinStrings(conds, " AND ")
	}
	q += " ORDER BY opened_at ASC"
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Batch
	for rows.Next() {
		var b store.Batch
		var closedAt time.Time
		var notional string
		if err := rows.Scan(&b.ID, &b.AssetPair, &b.Status, &notional, &b.OpenedAt, &closedAt); err != nil {
			return nil, err
		}
		b.ClosedAt = closedAt
		b.NotionalUSD, _ = decimal.NewFromString(notional)
		out = append(out, &b)
	}
	return out, rows.Err()
}

func (s *BatchStore) ListOpenBatches(ctx context.Context) ([]*store.Batch, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, asset_pair, status, notional_usd, opened_at, COALESCE(closed_at, now())
		 FROM batches WHERE status='OPEN' ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Batch
	for rows.Next() {
		var b store.Batch
		var closedAt time.Time
		var notional string
		if err := rows.Scan(&b.ID, &b.AssetPair, &b.Status, &notional, &b.OpenedAt, &closedAt); err != nil {
			return nil, err
		}
		b.ClosedAt = closedAt
		b.NotionalUSD, _ = decimal.NewFromString(notional)
		out = append(out, &b)
	}
	return out, rows.Err()
}

func (s *BatchStore) UpdateBatchStatus(ctx context.Context, id uuid.UUID, from, to store.BatchStatus, mutator func(*store.Batch)) (*store.Batch, bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	b, ok, err := updateBatchStatusInTx(ctx, tx, id, from, to, mutator)
	if err != nil || !ok {
		return b, ok, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// UpdateBatchStatusWithOutbox transitions a batch and appends outbox
// events inside the same DB transaction. If any outbox insert fails the
// transaction is rolled back, so the state change and its audit/ledger
// event are persisted atomically.
func (d *DB) UpdateBatchStatusWithOutbox(ctx context.Context, id uuid.UUID, from, to store.BatchStatus, mutator func(*store.Batch), events []*store.OutboxEntry) (*store.Batch, bool, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	b, ok, err := updateBatchStatusInTx(ctx, tx, id, from, to, mutator)
	if err != nil || !ok {
		return b, ok, err
	}
	for _, e := range events {
		if e == nil {
			continue
		}
		if _, err := appendOutboxInTx(ctx, tx, e); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func updateBatchStatusInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, from, to store.BatchStatus, mutator func(*store.Batch)) (*store.Batch, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT id, asset_pair, status, notional_usd, opened_at, COALESCE(closed_at, now())
		 FROM batches WHERE id=$1 FOR UPDATE`, id)
	var b store.Batch
	var notional string
	if err := row.Scan(&b.ID, &b.AssetPair, &b.Status, &notional, &b.OpenedAt, &b.ClosedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, store.ErrNotFound
		}
		return nil, false, err
	}
	b.NotionalUSD, _ = decimal.NewFromString(notional)
	if b.Status != from {
		return nil, false, nil
	}
	if !from.CanTransitionTo(to) {
		return nil, false, store.ErrConflict
	}
	b.Status = to
	closedAt := b.ClosedAt
	if to == store.BatchClosed && closedAt.IsZero() {
		closedAt = time.Now().UTC()
	}
	if mutator != nil {
		mutator(&b)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE batches SET status=$2, notional_usd=$3, closed_at=$4, updated_at=now() WHERE id=$1`,
		id, string(b.Status), b.NotionalUSD.String(), closedAt); err != nil {
		return nil, false, err
	}
	b.ClosedAt = closedAt
	return &b, true, nil
}

func appendOutboxInTx(ctx context.Context, tx pgx.Tx, e *store.OutboxEntry) (bool, error) {
	id, _ := uuid.NewV7()
	tag, err := tx.Exec(ctx,
		`INSERT INTO outbox (id, aggregate, event_type, dedup_key, payload, created_at)
		 VALUES ($1,$2,$3,$4,$5,COALESCE($6,now())) ON CONFLICT (dedup_key) DO NOTHING`,
		id, e.Aggregate, e.EventType, e.DedupKey, e.Payload, e.CreatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *BatchStore) SetBatchNotional(ctx context.Context, id uuid.UUID, notional decimal.Decimal) error {
	_, err := s.db.Exec(ctx, `UPDATE batches SET notional_usd=$2, updated_at=now() WHERE id=$1`, id, notional.String())
	return err
}

// --- MembershipStore ---

type MembershipStore struct{ db poolDB }

func (s *MembershipStore) AddMembership(ctx context.Context, m *store.Membership) (bool, error) {
	id, _ := uuid.NewV7()
	tag, err := s.db.Exec(ctx,
		`INSERT INTO batch_memberships (id, batch_id, tx_id, amount, asset, fiat_currency, notional_usd, user_id, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9,now()))
		 ON CONFLICT (tx_id) DO NOTHING`,
		id, m.BatchID, m.TxID, m.Amount.String(), m.Asset, m.FiatCurrency, m.NotionalUSD.String(), m.UserID, m.CreatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *MembershipStore) ListMemberships(ctx context.Context, batchID uuid.UUID) ([]*store.Membership, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, batch_id, tx_id, amount, asset, fiat_currency, notional_usd, user_id, created_at
		 FROM batch_memberships WHERE batch_id=$1 ORDER BY id ASC`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Membership
	for rows.Next() {
		var m store.Membership
		var amount, notional string
		if err := rows.Scan(&m.ID, &m.BatchID, &m.TxID, &amount, &m.Asset, &m.FiatCurrency, &notional, &m.UserID, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.Amount, _ = decimal.NewFromString(amount)
		m.NotionalUSD, _ = decimal.NewFromString(notional)
		out = append(out, &m)
	}
	return out, rows.Err()
}

func (s *MembershipStore) SumNotional(ctx context.Context, batchID uuid.UUID) (decimal.Decimal, error) {
	var sum *string
	if err := s.db.QueryRow(ctx, `SELECT COALESCE(SUM(notional_usd),0)::text FROM batch_memberships WHERE batch_id=$1`, batchID).Scan(&sum); err != nil {
		return decimal.Decimal{}, err
	}
	if sum == nil {
		return decimal.Decimal{}, nil
	}
	return decimal.NewFromString(*sum)
}

func (s *MembershipStore) ExistsByTxID(ctx context.Context, txID string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM batch_memberships WHERE tx_id=$1)`, txID).Scan(&exists)
	return exists, err
}

// --- AggregateOrderStore ---

type AggregateOrderStore struct{ db poolDB }

func (s *AggregateOrderStore) CreateOrder(ctx context.Context, o *store.AggregateOrder) (*store.AggregateOrder, error) {
	side := o.Side
	if side == "" {
		side = "BUY"
	}
	status := string(o.Status)
	if status == "" {
		status = string(store.AggregateExecuting)
	}
	routes := o.VenueRoutes
	if routes == nil {
		routes = []store.VenueRoute{}
	}
	routesJSON, _ := json.Marshal(routes)
	if o.ID == uuid.Nil {
		o.ID, _ = uuid.NewV7()
	}
	_, err := s.db.Exec(ctx,
		`INSERT INTO aggregate_orders (id, batch_id, asset_pair, side, notional_usd, venue_routes, fill_price, total_filled, hedged_notional, status, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
		 ON CONFLICT (batch_id) DO NOTHING`,
		o.ID, o.BatchID, o.AssetPair, side, o.NotionalUSD.String(), routesJSON, o.FillPrice.String(), o.TotalFilled.String(), o.HedgedNotional.String(), status)
	if err != nil {
		return nil, err
	}
	return s.GetOrderByBatch(ctx, o.BatchID)
}

func (s *AggregateOrderStore) GetOrderByBatch(ctx context.Context, batchID uuid.UUID) (*store.AggregateOrder, error) {
	row := s.db.QueryRow(ctx,
		`SELECT id, batch_id, asset_pair, side, notional_usd, venue_routes, fill_price, total_filled, hedged_notional, status, created_at, COALESCE(settled_at, now())
		 FROM aggregate_orders WHERE batch_id=$1`, batchID)
	var o store.AggregateOrder
	var routesJSON []byte
	var settledAt time.Time
	var notional, fillPrice, totalFilled, hedgedNotional string
	if err := row.Scan(&o.ID, &o.BatchID, &o.AssetPair, &o.Side, &notional, &routesJSON, &fillPrice, &totalFilled, &hedgedNotional, &o.Status, &o.CreatedAt, &settledAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	o.NotionalUSD, _ = decimal.NewFromString(notional)
	o.FillPrice, _ = decimal.NewFromString(fillPrice)
	o.TotalFilled, _ = decimal.NewFromString(totalFilled)
	o.HedgedNotional, _ = decimal.NewFromString(hedgedNotional)
	_ = json.Unmarshal(routesJSON, &o.VenueRoutes)
	o.SettledAt = settledAt
	return &o, nil
}

func (s *AggregateOrderStore) ListOrders(ctx context.Context, status string) ([]*store.AggregateOrder, error) {
	q := `SELECT id, batch_id, asset_pair, side, notional_usd, venue_routes, fill_price, total_filled, hedged_notional, status, created_at, COALESCE(settled_at, now()) FROM aggregate_orders`
	var args []any
	if status != "" {
		q += " WHERE status=$1"
		args = []any{status}
	}
	q += " ORDER BY id ASC"
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.AggregateOrder
	for rows.Next() {
		var o store.AggregateOrder
		var routesJSON []byte
		var settledAt time.Time
		var notional, fillPrice, totalFilled, hedgedNotional string
		if err := rows.Scan(&o.ID, &o.BatchID, &o.AssetPair, &o.Side, &notional, &routesJSON, &fillPrice, &totalFilled, &hedgedNotional, &o.Status, &o.CreatedAt, &settledAt); err != nil {
			return nil, err
		}
		o.NotionalUSD, _ = decimal.NewFromString(notional)
		o.FillPrice, _ = decimal.NewFromString(fillPrice)
		o.TotalFilled, _ = decimal.NewFromString(totalFilled)
		o.HedgedNotional, _ = decimal.NewFromString(hedgedNotional)
		_ = json.Unmarshal(routesJSON, &o.VenueRoutes)
		o.SettledAt = settledAt
		out = append(out, &o)
	}
	return out, rows.Err()
}

func (s *AggregateOrderStore) UpdateOrderFill(ctx context.Context, batchID uuid.UUID, fillPrice, totalFilled decimal.Decimal, venueRoutes []store.VenueRoute) (*store.AggregateOrder, error) {
	routesJSON, _ := json.Marshal(venueRoutes)
	if _, err := s.db.Exec(ctx,
		`UPDATE aggregate_orders SET fill_price=$2, total_filled=$3, venue_routes=$4, updated_at=now() WHERE batch_id=$1`,
		batchID, fillPrice.String(), totalFilled.String(), routesJSON); err != nil {
		return nil, err
	}
	return s.GetOrderByBatch(ctx, batchID)
}

func (s *AggregateOrderStore) SettleOrder(ctx context.Context, batchID uuid.UUID, hedgedNotional decimal.Decimal) (*store.AggregateOrder, error) {
	if _, err := s.db.Exec(ctx,
		`UPDATE aggregate_orders SET status='SETTLED', hedged_notional=$2, settled_at=now(), updated_at=now() WHERE batch_id=$1`,
		batchID, hedgedNotional.String()); err != nil {
		return nil, err
	}
	return s.GetOrderByBatch(ctx, batchID)
}

// --- FundingStore ---

type FundingStore struct{ db poolDB }

func (s *FundingStore) CreateFunding(ctx context.Context, f *store.FundingRequest) (*store.FundingRequest, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out, err := createFundingInTx(ctx, tx, f)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// CreateFundingWithOutbox persists a funding request and appends outbox
// events inside the same DB transaction.
func (d *DB) CreateFundingWithOutbox(ctx context.Context, f *store.FundingRequest, events []*store.OutboxEntry) (*store.FundingRequest, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out, err := createFundingInTx(ctx, tx, f)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if e == nil {
			continue
		}
		if _, err := appendOutboxInTx(ctx, tx, e); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func createFundingInTx(ctx context.Context, tx pgx.Tx, f *store.FundingRequest) (*store.FundingRequest, error) {
	status := string(f.Status)
	if status == "" {
		status = string(store.FundingPending)
	}
	id, _ := uuid.NewV7()
	var createdAt time.Time
	if err := tx.QueryRow(ctx,
		`INSERT INTO funding_requests (id, wallet_id, asset, amount, status, source_venue, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,now()) RETURNING created_at`,
		id, f.WalletID, f.Asset, f.Amount.String(), status, f.SourceVenue).Scan(&createdAt); err != nil {
		return nil, err
	}
	c := *f
	c.ID = id
	c.CreatedAt = createdAt
	c.Status = store.FundingStatus(status)
	return &c, nil
}

func (s *FundingStore) GetFunding(ctx context.Context, id uuid.UUID) (*store.FundingRequest, error) {
	row := s.db.QueryRow(ctx,
		`SELECT id, wallet_id, asset, amount, status, source_venue, created_at, COALESCE(completed_at, now())
		 FROM funding_requests WHERE id=$1`, id)
	var f store.FundingRequest
	var amount string
	if err := row.Scan(&f.ID, &f.WalletID, &f.Asset, &amount, &f.Status, &f.SourceVenue, &f.CreatedAt, &f.CompletedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	f.Amount, _ = decimal.NewFromString(amount)
	return &f, nil
}

func (s *FundingStore) UpdateFundingStatus(ctx context.Context, id uuid.UUID, status store.FundingStatus) error {
	completed := ""
	if status == store.FundingCompleted || status == store.FundingRejected {
		completed = ", completed_at=now()"
	}
	_, err := s.db.Exec(ctx, fmt.Sprintf(`UPDATE funding_requests SET status=$2%s, updated_at=now() WHERE id=$1`, completed), id, string(status))
	return err
}

func (s *FundingStore) ListFunding(ctx context.Context, status string) ([]*store.FundingRequest, error) {
	q := `SELECT id, wallet_id, asset, amount, status, source_venue, created_at, COALESCE(completed_at, now()) FROM funding_requests`
	args := []any{}
	if status != "" {
		args = append(args, status)
		q += fmt.Sprintf(" WHERE status=$1")
	}
	q += " ORDER BY id ASC"
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.FundingRequest
	for rows.Next() {
		var f store.FundingRequest
		var amount string
		if err := rows.Scan(&f.ID, &f.WalletID, &f.Asset, &amount, &f.Status, &f.SourceVenue, &f.CreatedAt, &f.CompletedAt); err != nil {
			return nil, err
		}
		f.Amount, _ = decimal.NewFromString(amount)
		out = append(out, &f)
	}
	return out, rows.Err()
}

// --- FloatStore ---

type FloatStore struct{ db poolDB }

func (s *FloatStore) AddFloat(ctx context.Context, p *store.FloatPosition) (*store.FloatPosition, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out, err := addFloatInTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// AddFloatWithOutbox increments the float position and appends outbox
// events inside the same DB transaction.
func (d *DB) AddFloatWithOutbox(ctx context.Context, p *store.FloatPosition, events []*store.OutboxEntry) (*store.FloatPosition, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out, err := addFloatInTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if e == nil {
			continue
		}
		if _, err := appendOutboxInTx(ctx, tx, e); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func addFloatInTx(ctx context.Context, tx pgx.Tx, p *store.FloatPosition) (*store.FloatPosition, error) {
	row := tx.QueryRow(ctx,
		`SELECT id FROM float_positions WHERE fiat_currency=$1 AND settled=false FOR UPDATE`, p.FiatCurrency)
	var id uuid.UUID
	if err := row.Scan(&id); err == nil {
		if _, err := tx.Exec(ctx,
			`UPDATE float_positions SET short_fiat_amount=short_fiat_amount+$2, long_crypto_amount=long_crypto_amount+$3, long_crypto_asset=$4, settlement_due_at=$5, updated_at=now() WHERE id=$1`,
			id, p.ShortFiatAmount.String(), p.LongCryptoAmount.String(), p.LongCryptoAsset, p.SettlementDueAt); err != nil {
			return nil, err
		}
		return getFloatInTx(ctx, tx, p.FiatCurrency)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	newID, _ := uuid.NewV7()
	if _, err := tx.Exec(ctx,
		`INSERT INTO float_positions (id, fiat_currency, short_fiat_amount, long_crypto_amount, long_crypto_asset, settlement_due_at, settled, batch_id, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,false,$7,now(),now())`,
		newID, p.FiatCurrency, p.ShortFiatAmount.String(), p.LongCryptoAmount.String(), p.LongCryptoAsset, p.SettlementDueAt, p.BatchID); err != nil {
		return nil, err
	}
	c := *p
	c.ID = newID
	return &c, nil
}

func getFloatInTx(ctx context.Context, tx pgx.Tx, fiatCurrency string) (*store.FloatPosition, error) {
	row := tx.QueryRow(ctx,
		`SELECT COALESCE(SUM(short_fiat_amount),0)::text, COALESCE(SUM(long_crypto_amount),0)::text,
		        COALESCE(MAX(long_crypto_asset),''), COALESCE(MAX(settlement_due_at),now())
		 FROM float_positions WHERE fiat_currency=$1`, fiatCurrency)
	var p store.FloatPosition
	var shortFiat, longCrypto string
	p.FiatCurrency = fiatCurrency
	if err := row.Scan(&shortFiat, &longCrypto, &p.LongCryptoAsset, &p.SettlementDueAt); err != nil {
		return nil, err
	}
	p.ShortFiatAmount, _ = decimal.NewFromString(shortFiat)
	p.LongCryptoAmount, _ = decimal.NewFromString(longCrypto)
	return &p, nil
}

func (s *FloatStore) GetFloat(ctx context.Context, fiatCurrency string) (*store.FloatPosition, error) {
	row := s.db.QueryRow(ctx,
		`SELECT COALESCE(SUM(short_fiat_amount),0)::text, COALESCE(SUM(long_crypto_amount),0)::text,
		        COALESCE(MAX(long_crypto_asset),''), COALESCE(MAX(settlement_due_at),now())
		 FROM float_positions WHERE fiat_currency=$1`, fiatCurrency)
	var p store.FloatPosition
	var shortFiat, longCrypto string
	p.FiatCurrency = fiatCurrency
	if err := row.Scan(&shortFiat, &longCrypto, &p.LongCryptoAsset, &p.SettlementDueAt); err != nil {
		return nil, err
	}
	p.ShortFiatAmount, _ = decimal.NewFromString(shortFiat)
	p.LongCryptoAmount, _ = decimal.NewFromString(longCrypto)
	return &p, nil
}

func (s *FloatStore) ListFloat(ctx context.Context) ([]*store.FloatPosition, error) {
	rows, err := s.db.Query(ctx,
		`SELECT fiat_currency, COALESCE(SUM(short_fiat_amount),0)::text, COALESCE(SUM(long_crypto_amount),0)::text,
		        COALESCE(MAX(long_crypto_asset),''), COALESCE(MAX(settlement_due_at),now())
		 FROM float_positions GROUP BY fiat_currency ORDER BY fiat_currency ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.FloatPosition
	for rows.Next() {
		var p store.FloatPosition
		var shortFiat, longCrypto string
		if err := rows.Scan(&p.FiatCurrency, &shortFiat, &longCrypto, &p.LongCryptoAsset, &p.SettlementDueAt); err != nil {
			return nil, err
		}
		p.ShortFiatAmount, _ = decimal.NewFromString(shortFiat)
		p.LongCryptoAmount, _ = decimal.NewFromString(longCrypto)
		out = append(out, &p)
	}
	return out, rows.Err()
}

func (s *FloatStore) ListMaturedFloat(ctx context.Context, before time.Time) ([]*store.FloatPosition, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, fiat_currency, short_fiat_amount::text, long_crypto_amount::text, long_crypto_asset, settlement_due_at, settled, COALESCE(batch_id,uuid_nil()), created_at, updated_at
		 FROM float_positions WHERE settled=false AND settlement_due_at <= $1 ORDER BY id ASC`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.FloatPosition
	for rows.Next() {
		var p store.FloatPosition
		var shortFiat, longCrypto string
		if err := rows.Scan(&p.ID, &p.FiatCurrency, &shortFiat, &longCrypto, &p.LongCryptoAsset, &p.SettlementDueAt, &p.Settled, &p.BatchID, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		p.ShortFiatAmount, _ = decimal.NewFromString(shortFiat)
		p.LongCryptoAmount, _ = decimal.NewFromString(longCrypto)
		out = append(out, &p)
	}
	return out, rows.Err()
}

func (s *FloatStore) SettleFloat(ctx context.Context, id uuid.UUID) (*store.FloatPosition, error) {
	if _, err := s.db.Exec(ctx, `UPDATE float_positions SET settled=true, short_fiat_amount=0, long_crypto_amount=0, updated_at=now() WHERE id=$1`, id); err != nil {
		return nil, err
	}
	row := s.db.QueryRow(ctx, `SELECT id, fiat_currency, short_fiat_amount::text, long_crypto_amount::text, long_crypto_asset, settlement_due_at, settled, COALESCE(batch_id,uuid_nil()), created_at, updated_at FROM float_positions WHERE id=$1`, id)
	var p store.FloatPosition
	var shortFiat, longCrypto string
	if err := row.Scan(&p.ID, &p.FiatCurrency, &shortFiat, &longCrypto, &p.LongCryptoAsset, &p.SettlementDueAt, &p.Settled, &p.BatchID, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return nil, err
	}
	p.ShortFiatAmount, _ = decimal.NewFromString(shortFiat)
	p.LongCryptoAmount, _ = decimal.NewFromString(longCrypto)
	return &p, nil
}

// --- RebalancingStore ---

type RebalancingStore struct{ db poolDB }

func (s *RebalancingStore) CreateJob(ctx context.Context, j *store.RebalancingJob) (*store.RebalancingJob, error) {
	status := string(j.Status)
	if status == "" {
		status = string(store.RebalancePending)
	}
	id, _ := uuid.NewV7()
	var createdAt time.Time
	if err := s.db.QueryRow(ctx,
		`INSERT INTO rebalancing_jobs (id, from_ref, to_ref, asset, amount, status, reason, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,now()) RETURNING created_at`,
		id, j.FromRef, j.ToRef, j.Asset, j.Amount.String(), status, j.Reason).Scan(&createdAt); err != nil {
		return nil, err
	}
	c := *j
	c.ID = id
	c.CreatedAt = createdAt
	c.Status = store.RebalanceStatus(status)
	return &c, nil
}

func (s *RebalancingStore) ListJobs(ctx context.Context, status string) ([]*store.RebalancingJob, error) {
	q := `SELECT id, from_ref, to_ref, asset, amount, status, reason, created_at, COALESCE(completed_at, now()) FROM rebalancing_jobs`
	args := []any{}
	if status != "" {
		args = append(args, status)
		q += " WHERE status=$1"
	}
	q += " ORDER BY id ASC"
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.RebalancingJob
	for rows.Next() {
		var j store.RebalancingJob
		var amount string
		if err := rows.Scan(&j.ID, &j.FromRef, &j.ToRef, &j.Asset, &amount, &j.Status, &j.Reason, &j.CreatedAt, &j.CompletedAt); err != nil {
			return nil, err
		}
		j.Amount, _ = decimal.NewFromString(amount)
		out = append(out, &j)
	}
	return out, rows.Err()
}

func (s *RebalancingStore) UpdateJobStatus(ctx context.Context, id uuid.UUID, status store.RebalanceStatus) error {
	completed := ""
	if status == store.RebalanceCompleted || status == store.RebalanceRejected {
		completed = ", completed_at=now()"
	}
	_, err := s.db.Exec(ctx, fmt.Sprintf(`UPDATE rebalancing_jobs SET status=$2%s, updated_at=now() WHERE id=$1`, completed), id, string(status))
	return err
}

// --- OutboxStore ---

type OutboxStore struct{ db poolDB }

func (s *OutboxStore) Append(ctx context.Context, e *store.OutboxEntry) (bool, error) {
	id, _ := uuid.NewV7()
	tag, err := s.db.Exec(ctx,
		`INSERT INTO outbox (id, aggregate, event_type, dedup_key, payload, created_at)
		 VALUES ($1,$2,$3,$4,$5,COALESCE($6,now())) ON CONFLICT (dedup_key) DO NOTHING`,
		id, e.Aggregate, e.EventType, e.DedupKey, e.Payload, e.CreatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *OutboxStore) ListPending(ctx context.Context, limit int) ([]*store.OutboxEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, aggregate, event_type, dedup_key, payload, created_at, COALESCE(emitted_at, now())
		 FROM outbox WHERE emitted_at IS NULL ORDER BY id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.OutboxEntry
	for rows.Next() {
		var e store.OutboxEntry
		var emittedAt time.Time
		if err := rows.Scan(&e.ID, &e.Aggregate, &e.EventType, &e.DedupKey, &e.Payload, &e.CreatedAt, &emittedAt); err != nil {
			return nil, err
		}
		e.EmittedAt = emittedAt
		out = append(out, &e)
	}
	return out, rows.Err()
}

func (s *OutboxStore) MarkEmitted(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `UPDATE outbox SET emitted_at=now(), updated_at=now() WHERE id=$1`, id)
	return err
}

// --- helpers ---

func joinStrings(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}
