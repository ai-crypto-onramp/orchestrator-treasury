-- Treasury Orchestration initial schema.
-- All durable batch / membership / aggregate / funding / float /
-- rebalancing state.
-- Conventions: UUID PKs (app-generated UUIDv7, no DB default), UPPER_CASE enum
-- TEXT (no CHECK), created_at + updated_at on every table, no DB triggers.

CREATE TABLE IF NOT EXISTS batches (
    id           UUID PRIMARY KEY,
    asset_pair   TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'OPEN',
    notional_usd NUMERIC NOT NULL DEFAULT 0,
    opened_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at    TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS batches_status_idx        ON batches(status);
CREATE INDEX IF NOT EXISTS batches_asset_pair_idx    ON batches(asset_pair);
CREATE INDEX IF NOT EXISTS batches_opened_at_idx     ON batches(opened_at);

CREATE TABLE IF NOT EXISTS batch_memberships (
    id            UUID PRIMARY KEY,
    batch_id      UUID NOT NULL REFERENCES batches(id),
    tx_id         TEXT NOT NULL UNIQUE,
    amount        NUMERIC NOT NULL,
    asset         TEXT NOT NULL,
    fiat_currency TEXT NOT NULL,
    notional_usd  NUMERIC NOT NULL DEFAULT 0,
    user_id       TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS memberships_batch_id_idx     ON batch_memberships(batch_id);
CREATE INDEX IF NOT EXISTS memberships_fiat_currency_idx ON batch_memberships(fiat_currency);

CREATE TABLE IF NOT EXISTS aggregate_orders (
    id              UUID PRIMARY KEY,
    batch_id        UUID NOT NULL UNIQUE REFERENCES batches(id),
    asset_pair      TEXT NOT NULL,
    side            TEXT NOT NULL DEFAULT 'BUY',
    notional_usd    NUMERIC NOT NULL DEFAULT 0,
    venue_routes    JSONB NOT NULL DEFAULT '[]',
    fill_price      NUMERIC NOT NULL DEFAULT 0,
    total_filled    NUMERIC NOT NULL DEFAULT 0,
    hedged_notional NUMERIC NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'EXECUTING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS aggregate_orders_status_idx ON aggregate_orders(status);
CREATE INDEX IF NOT EXISTS aggregate_orders_batch_id_idx ON aggregate_orders(batch_id);

CREATE TABLE IF NOT EXISTS funding_requests (
    id           UUID PRIMARY KEY,
    wallet_id    TEXT NOT NULL,
    asset        TEXT NOT NULL,
    amount       NUMERIC NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    source_venue TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS funding_requests_status_idx ON funding_requests(status);
CREATE INDEX IF NOT EXISTS funding_requests_asset_idx  ON funding_requests(asset);

CREATE TABLE IF NOT EXISTS float_positions (
    id                  UUID PRIMARY KEY,
    fiat_currency       TEXT NOT NULL,
    short_fiat_amount   NUMERIC NOT NULL DEFAULT 0,
    long_crypto_amount  NUMERIC NOT NULL DEFAULT 0,
    long_crypto_asset   TEXT NOT NULL DEFAULT '',
    settlement_due_at   TIMESTAMPTZ,
    settled             BOOLEAN NOT NULL DEFAULT false,
    batch_id            UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS float_positions_fiat_currency_idx ON float_positions(fiat_currency);
CREATE INDEX IF NOT EXISTS float_positions_settlement_due_at_idx ON float_positions(settlement_due_at);
CREATE INDEX IF NOT EXISTS float_positions_settled_idx ON float_positions(settled);

CREATE TABLE IF NOT EXISTS rebalancing_jobs (
    id           UUID PRIMARY KEY,
    from_ref     TEXT NOT NULL DEFAULT '',
    to_ref       TEXT NOT NULL DEFAULT '',
    asset        TEXT NOT NULL,
    amount       NUMERIC NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    reason       TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS rebalancing_jobs_status_idx ON rebalancing_jobs(status);
CREATE INDEX IF NOT EXISTS rebalancing_jobs_asset_idx  ON rebalancing_jobs(asset);

-- Outbox for at-least-once ledger / audit emission.
CREATE TABLE IF NOT EXISTS outbox (
    id         UUID PRIMARY KEY,
    aggregate  TEXT NOT NULL,
    event_type TEXT NOT NULL,
    dedup_key  TEXT NOT NULL,
    payload    JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    emitted_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS outbox_dedup_key_uniq ON outbox(dedup_key);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(emitted_at) WHERE emitted_at IS NULL;

-- Migration bookkeeping.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO schema_migrations(version, applied_at)
VALUES ('0001', now())
ON CONFLICT (version) DO NOTHING;