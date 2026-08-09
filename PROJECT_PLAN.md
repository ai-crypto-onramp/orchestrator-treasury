# Project Plan — Treasury Orchestration

This plan decomposes the Treasury Orchestration service into ordered implementation stages. The service batches user crypto buys into aggregate parent orders, manages the T+0 vs T+2/3 float created by fronting crypto at T+0 while fiat settles at T+2/3, pre-funds hot wallets ahead of projected demand, and rebalances capital across wallets and venues. Stages are sequenced so each builds on a durable, testable foundation: schema first, then event ingestion, batch policy, aggregate execution, float tracking, pre-funding/rebalancing, FX hedging, ledger/audit emission, and finally hardening (tests, coverage, Docker).

## Stage 1: Database Schema & Migrations

Goal: Establish the durable persistence layer for all batch, membership, aggregate-order, funding, float, and rebalancing state.

Tasks:
- [x] Add PostgreSQL connection bootstrap (`DB_URL`, `pgx`/`database/sql` pool).
- [x] Create migrations for `batches` (id, asset_pair, status, notional_usd, opened_at, closed_at).
- [x] Create migrations for `batch_memberships` (batch_id, tx_id, amount, asset, fiat_currency, created_at).
- [x] Create migrations for `aggregate_orders` (batch_id, venue_routes, fill_price, total_filled, status).
- [x] Create migrations for `funding_requests` (wallet_id, asset, amount, status, source_venue).
- [x] Create migrations for `float_positions` (fiat_currency, short_fiat_amount, long_crypto_amount, settlement_due_at, updated_at).
- [x] Create migrations for `rebalancing_jobs` (from, to, asset, amount, status, reason).
- [x] Add idempotency-key table backed by Redis for write-side replays.
- [x] Wire migration runner into `cmd/treasury` startup.

Acceptance criteria:
- `go test ./...` covers migration up/down idempotency.
- All six tables exist with correct columns and indexes on (batch_id, status, asset_pair, fiat_currency).
- Service boots against a clean Postgres + Redis and reaches ready state.

## Stage 2: Async Tx Completion Event Consumption

Goal: Consume tx completion events from `transaction-orchestrator` off the event bus and persist them as pending batch memberships.

Tasks:
- [x] Implement event subscriber for `TX_ORCH_EVENT_TOPIC` (`tx.completed`).
- [x] Define event payload schema (tx_id, amount, asset, fiat_currency, notional_usd, user_id, completed_at).
- [x] On receipt, create a `batch_membership` row linked to the currently-open batch for the asset pair (open one if none exists).
- [x] Handle idempotency: dedupe by `tx_id` via Redis idempotency key.
- [x] Emit structured log + Prometheus counter on every consumed event.
- [x] Add dead-letter handling for poison messages.

Acceptance criteria:
- Replaying the same event produces no duplicate membership.
- An open batch is auto-created per asset pair on first event.
- Consumer survives a broker restart and resumes without loss.

## Stage 3: Batch Formation Policy (Time / Size / Manual)

Goal: Close batches deterministically on a time cadence, a notional size threshold, or manual operator trigger.

Tasks:
- [x] Implement scheduler loop with `BATCH_INTERVAL_SECONDS` cadence tick.
- [x] Implement size-threshold check: `sum(memberships.notional_usd) >= BATCH_SIZE_THRESHOLD_USD`.
- [x] Implement `POST /v1/batches/:id/close` for manual close.
- [x] On close, transition batch `open -> closed` and persist `closed_at`.
- [x] Acquire Redis cadence lock per asset pair to prevent double-close under leader election.
- [x] Expose `GET /v1/batches` and `GET /v1/batches/:id` query endpoints.
- [x] Make thresholds per-asset-pair overridable via config.

Acceptance criteria:
- A batch closes within `BATCH_INTERVAL_SECONDS + epsilon` of `opened_at` under steady load.
- A batch closes immediately when cumulative notional crosses `BATCH_SIZE_THRESHOLD_USD`.
- Manual close forces aggregation even if thresholds are unmet.
- Concurrent schedulers never double-close the same batch.

## Stage 4: Aggregate Parent Order Submission to Liquidity Routing

Goal: Submit each closed batch as an aggregate parent order to `liquidity-routing` and persist the fill result.

Tasks:
- [x] Implement `liquidity-routing` client (`LIQUIDITY_ROUTING_URL`) with retry + circuit breaker.
- [x] On batch close, build parent order payload (asset_pair, side=buy, total_filled target, notional).
- [x] Persist `aggregate_orders` row with status `executing` before submission.
- [x] On fill response, update `fill_price`, `total_filled`, `venue_routes`, status `settled`.
- [x] Transition batch `closed -> executing -> settled` to mirror order lifecycle.
- [x] Add idempotency key on the liquidity-routing call (batch_id).
- [x] Emit Prometheus histogram for slippage vs expected price.

Acceptance criteria:
- A closed batch results in exactly one parent order to liquidity-routing.
- Crash between persist and submit recovers and re-submits safely (idempotent).
- Fill result is durably persisted before any downstream side effect.

## Stage 5: T+0 vs T+2/3 Float Tracking & Capital Efficiency

Goal: Track the short-fiat / long-crypto position created by T+0 delivery and keep it within policy bounds.

Tasks:
- [x] On aggregate fill, increment `float_positions` long_crypto_amount and short_fiat_amount for the fiat currency.
- [x] Record `settlement_due_at` as `now + T+n` for the fiat rail (T+2 or T+3 per currency config).
- [x] Implement `GET /v1/float/{fiat_currency}`.
- [x] Enforce `MIN_FLOAT_USD` / `MAX_FLOAT_USD` bounds; log + alert on breach.
- [x] On settlement-due date, mark fiat leg settled and decrement short_fiat_amount.
- [x] Sweep matured floats to minimize idle capital (capital efficiency policy).

Acceptance criteria:
- Float position is consistent with sum of delivered crypto minus settled fiat.
- A breach of `MAX_FLOAT_USD` triggers an alert and a forced rebalance/hedge signal.
- Matured floats are swept within bounded latency of `settlement_due_at`.

## Stage 6: Hot Wallet Pre-Funding & Rebalancing

Goal: Pre-fund hot wallets ahead of projected demand and rebalance crypto/fiat across wallets and venues.

Tasks:
- [x] Implement demand-projection model from inbound order velocity (rolling window).
- [x] Compute per-asset target balance from `HOT_WALLET_TARGET_BALANCE_<ASSET>`.
- [x] Create `funding_requests` (POST /v1/funding-requests) when projected balance < target.
- [x] Call `wallet-management` (`WALLET_MGMT_URL`) to execute funding moves.
- [x] Implement rebalancing loop: detect drift below target or venue excess; create `rebalancing_jobs`.
- [x] Enforce capital allocation policy: reject out-of-policy amounts, log violations.
- [x] Expose `GET /v1/rebalancing-jobs` with status filter.

Acceptance criteria:
- Hot wallet never falls below target by more than the configured tolerance during peak velocity.
- Every funding and rebalance move is persisted before execution.
- Policy violations are rejected, logged, and surfaced via metrics.

## Stage 7: FX Hedging Exposure Calls

Goal: Forward net aggregate FX exposure to `fx-hedging` for hedge execution.

Tasks:
- [x] Implement `fx-hedging` client (`FX_HEDGING_URL`) with retry + circuit breaker.
- [x] After each aggregate fill, compute net FX exposure per fiat currency (short_fiat delta).
- [x] Submit exposure payload to fx-hedging; persist hedged_notional on the aggregate_order.
- [x] Add idempotency key (batch_id) on hedge calls.
- [x] Emit Prometheus gauge for unhedged exposure per currency.

Acceptance criteria:
- Every aggregate fill results in exactly one FX exposure submission.
- Replays are safe and do not double-hedge.
- Unhedged exposure gauge stays within policy tolerance.

## Stage 8: Ledger Posting & Audit Emission

Goal: Post every capital movement to `ledger-accounting` and emit audit events for every aggregate action.

Tasks:
- [x] Implement `ledger-accounting` client (`LEDGER_URL`) with idempotent postings.
- [x] Post on: batch close, aggregate fill, funding request, float adjustment, rebalance.
- [x] Implement `audit-event-log` client (`AUDIT_LOG_URL`) append-only emitter.
- [x] Emit audit event for: batch open/close, aggregate execution, funding, float adjustment, rebalance.
- [x] Add outbox pattern to guarantee ledger/audit delivery after local commit.
- [x] Add Prometheus counters for posting successes/failures.

Acceptance criteria:
- Every batch close/fill/funding/float/rebalance has matching ledger posting and audit event.
- Outbox guarantees delivery even if downstream is temporarily unavailable.
- No duplicate postings on replay (idempotency keys enforced).

## Stage 9: Tests, Coverage, Docker & CI Hardening

Goal: Reach production-grade tests, containerization, and CI readiness.

Tasks:
- [x] Add unit tests for scheduler, policy, float math, projection model, clients.
- [ ] Add integration tests with testcontainers (Postgres + Redis).
- [x] Add contract tests for liquidity-routing, wallet-management, fx-hedging, ledger, audit mocks.
- [x] Wire `go test -race -coverprofile` into CI.
- [x] Finalize Dockerfile (multi-stage, scratch/distroless final).
- [x] Add Makefile targets: `test`, `test-integration`, `lint`, `cover`, `docker`.
- [x] Verify Codecov upload works on CI.

Acceptance criteria:
- `make test` and `make test-integration` pass locally and on CI.
- Coverage reported to Codecov; CI is green on main.
- `make docker` builds a runnable image that starts the service cleanly.