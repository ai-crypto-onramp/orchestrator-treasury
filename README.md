# Treasury Orchestration

![CI](https://github.com/ai-crypto-onramp/orchestrator-treasury/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/ai-crypto-onramp/orchestrator-treasury/branch/main/graph/badge.svg)](https://codecov.io/gh/ai-crypto-onramp/orchestrator-treasury)

Batches user crypto buys into aggregate parent orders, manages the T+0 vs T+2/3 float, and pre-funds hot wallets ahead of demand.

## Overview / Responsibilities

Treasury Orchestration is the async treasury layer of the crypto on-ramp. It runs **off the synchronous transaction path**: the Transaction Orchestrator delivers crypto to users at T+0 (settlement-grade delivery), while fiat from the user's payment rail settles at T+2/3. Treasury absorbs that float gap.

Core responsibilities:

- **Aggregate user orders** into parent buy orders on a cadence (every N seconds or upon hitting a notional size threshold), reducing per-order slippage and venue cost.
- **Manage the T+0 vs T+2/3 float** — the platform fronts crypto now and receives fiat later; treasury tracks the resulting short fiat / long crypto position and ensures it stays within limits.
- **Pre-fund hot wallets** ahead of projected demand so the Transaction Orchestrator can deliver instantly without waiting for venue settlement.
- **Rebalance** crypto and fiat across wallets and venues to keep hot wallets funded and idle capital minimal.
- **Hedge aggregate FX exposure** by forwarding net currency exposure to the FX & Hedging service.
- **Enforce capital efficiency and a delivery-time SLA** — minimize idle capital while meeting the user-facing delivery commitment.

## Language & Tech Stack

- **Language:** Go (goroutines for batch workers, channel-driven scheduling).
- **Batch scheduler:** time- and size-triggered dispatcher running a configurable cadence.
- **Float management:** per-currency position tracking with T+0/T+2/3 settlement-date awareness.
- **Pre-funding optimization:** demand-projection model feeding proactive hot-wallet top-ups.
- **Persistence:** PostgreSQL for durable batch state; Redis for cadence locks and idempotency keys.
- **Observability:** structured logging, Prometheus metrics, OpenTelemetry traces.

## System Requirements

1. **Batch aggregation:** Group individual user crypto buy orders (emitted by the Transaction Orchestrator on tx completion) into aggregate parent orders, triggered on:
   - A time cadence (e.g. every `BATCH_INTERVAL_SECONDS`), **or**
   - A notional size threshold per asset pair (e.g. `BATCH_SIZE_THRESHOLD_USD`).
2. **Float management:** Track the position created by delivering crypto at T+0 while fiat settles at T+2/3. Maintain per-fiat-currency float positions and ensure the float stays within configured bounds (`MIN_FLOAT_USD`, max float).
3. **Pre-funding hot wallets:** Project forward demand from inbound order velocity and pre-fund hot wallets so that delivery SLA can be met without waiting for venue settlement.
4. **Rebalancing:** Rebalance crypto/fiat across wallets and venues when a wallet drifts below its target balance or a venue holds excess inventory.
5. **FX hedging:** Forward net aggregate FX exposure to the FX & Hedging service for hedge execution.
6. **Capital efficiency:** Minimize idle capital — only pre-fund to projected demand, never hold excess buffers beyond policy.
7. **Delivery SLA:** Honor the user-facing delivery-time SLA even when batch cadence or venue settlement introduces latency.

## Non-Functional Requirements

- **Batch decisions on cadence:** Batches close deterministically on time or size; never block the transaction path (fully async).
- **Durable batch state:** All batch, membership, aggregate-order, and float state is persisted before side effects; crash-recoverable to the last closed batch.
- **Audit every aggregate action:** Every batch open/close, aggregate execution, funding request, float adjustment, and rebalance is emitted to the Audit / Event Log.
- **Capital allocation policy enforcement:** All pre-funding and rebalancing amounts must be within policy-configured limits; violations are rejected and logged.
- **Idempotency:** All write-side endpoints accept an idempotency key; replays are safe.
- **Throughput / latency:** Batch worker loop must close batches within bounded latency; rebalancing and funding calls are non-blocking to the scheduler.

## Technical Specifications

### API Surface

REST HTTP/JSON for control and query, plus an internal scheduler process driving batch close, pre-funding, and rebalancing loops.

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/batches?from=&to=` | List batches in a time window. |
| `GET` | `/v1/batches/:id` | Get a single batch with memberships and aggregate order. |
| `POST` | `/v1/batches/:id/close` | Manually close an open batch (forces aggregation). |
| `POST` | `/v1/funding-requests` | Create a hot-wallet funding request. Body: `{ wallet_id, asset, amount }`. |
| `GET` | `/v1/float/{fiat_currency}` | Current float position for a fiat currency (T+0 long crypto / short fiat). |
| `GET` | `/v1/rebalancing-jobs` | List rebalancing jobs (optionally filtered by status). |

### Data Model

- **batches** — one aggregate parent order; `id`, `asset_pair`, `status` (open/closed/executing/settled), `notional_usd`, `opened_at`, `closed_at`.
- **batch_memberships** — links individual Transaction Orchestrator txs into a batch; `batch_id`, `tx_id`, `amount`, `asset`, `fiat_currency`, `created_at`.
- **aggregate_orders** — the executed parent order against Liquidity Routing; `batch_id`, `venue_routes`, `fill_price`, `total_filled`, `status`.
- **funding_requests** — hot-wallet funding instructions to Wallet Management; `wallet_id`, `asset`, `amount`, `status`, `source_venue`.
- **float_positions** — per-fiat-currency float; `fiat_currency`, `short_fiat_amount`, `long_crypto_amount`, `settlement_due_at`, `updated_at`.
- **rebalancing_jobs** — wallet/venue rebalance records; `from`, `to`, `asset`, `amount`, `status`, `reason`.

### Batch Policy

A batch closes when **any** of the following is true for a given asset pair:

- **Time threshold:** `now - batch.opened_at >= BATCH_INTERVAL_SECONDS`.
- **Size threshold:** `sum(memberships.notional_usd) >= BATCH_SIZE_THRESHOLD_USD`.
- **Manual close:** operator or scheduler forces `POST /v1/batches/:id/close`.

Policy is configurable per asset pair; defaults are global env vars.

### Integrations

- **Consumes** tx completion events from `transaction-orchestrator` asynchronously (event bus) — the trigger for batch membership.
- **Calls** `liquidity-routing` for aggregate execution of parent orders.
- **Calls** `wallet-management` for hot-wallet funding and rebalancing.
- **Calls** `fx-hedging` to hedge net aggregate FX exposure.
- **Posts** to `ledger-accounting` for every batch close, fill, funding, and float adjustment.
- **Emits** to `audit-event-log` for every aggregate action.

## Dependencies

| Dependency | Purpose |
|---|---|
| PostgreSQL | Durable batch / membership / float / rebalancing state. |
| Redis | Scheduler cadence locks, idempotency keys. |
| transaction-orchestrator | Event source — tx completion events drive batch membership. |
| liquidity-routing | Aggregate parent-order execution. |
| wallet-management | Hot-wallet funding and rebalancing. |
| fx-hedging | Hedge aggregate FX exposure. |
| ledger-accounting | Source-of-truth postings for all capital movements. |
| audit-event-log | Append-only audit trail of every aggregate action. |

## Configuration

| Env Var | Description | Example |
|---|---|---|
| `PORT` | HTTP listen port. | `8080` |
| `DB_URL` | PostgreSQL connection string. | `postgres://user:pass@host:5432/treasury` |
| `REDIS_URL` | Redis connection string. | `redis://host:6379/0` |
| `BATCH_INTERVAL_SECONDS` | Max time a batch stays open before forced close. | `30` |
| `BATCH_SIZE_THRESHOLD_USD` | Notional size that triggers an early batch close. | `50000` |
| `HOT_WALLET_TARGET_BALANCE_<ASSET>` | Target hot-wallet balance per asset (e.g. `HOT_WALLET_TARGET_BALANCE_USDC`). | `100000` |
| `MIN_FLOAT_USD` | Minimum float the service is allowed to carry. | `250000` |
| `MAX_FLOAT_USD` | Maximum float before a forced rebalance/hedge. | `5000000` |
| `LIQUIDITY_ROUTING_URL` | Base URL for the liquidity-routing service. | `http://liquidity-routing:8080` |
| `WALLET_MGMT_URL` | Base URL for wallet-management. | `http://wallet-management:8080` |
| `FX_HEDGING_URL` | Base URL for fx-hedging. | `http://fx-hedging:8080` |
| `LEDGER_URL` | Base URL for ledger-accounting. | `http://ledger-accounting:8080` |
| `AUDIT_LOG_URL` | Base URL for audit-event-log. | `http://audit-event-log:8080` |
| `TX_ORCH_EVENT_TOPIC` | Event bus topic for tx completion events. | `tx.completed` |
| `LOG_LEVEL` | Log level. | `info` |

## Local Development

```sh
# Build
go build -o bin/treasury ./cmd/treasury

# Run (requires PostgreSQL + Redis running)
go run ./cmd/treasury

# Test
go test ./...
```
