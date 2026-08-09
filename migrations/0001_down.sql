-- Treasury Orchestration initial schema (teardown).

DROP TABLE IF EXISTS rebalancing_jobs;
DROP TABLE IF EXISTS float_positions;
DROP TABLE IF EXISTS funding_requests;
DROP TABLE IF EXISTS aggregate_orders;
DROP TABLE IF EXISTS batch_memberships;
DROP TABLE IF EXISTS batches;
DROP TABLE IF EXISTS outbox;
DELETE FROM schema_migrations WHERE version = '0001';