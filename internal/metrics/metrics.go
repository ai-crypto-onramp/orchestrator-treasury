// Package metrics defines the Prometheus metric instruments used across
// the Treasury Orchestration service. Metrics are registered once at
// startup; the rest of the service updates them via the exported handles.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// EventsConsumed counts consumed tx completion events.
	EventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_events_consumed_total",
		Help: "Total consumed tx completion events.",
	}, []string{"asset_pair", "result"})

	// DeadLettered counts poison messages moved to the dead-letter queue.
	DeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_dead_lettered_total",
		Help: "Poison messages moved to the dead-letter queue.",
	}, []string{"topic"})

	// BatchesClosed counts batch close events by reason.
	BatchesClosed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_batches_closed_total",
		Help: "Batches closed by reason (time|size|manual).",
	}, []string{"asset_pair", "reason"})

	// SlippageSeconds observes batch close latency in seconds.
	CloseLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "treasury_batch_close_latency_seconds",
		Help:    "Time from batch open to close.",
		Buckets: prometheus.DefBuckets,
	}, []string{"asset_pair"})

	// SlippageUSD observes slippage of the fill price vs expected price.
	SlippageUSD = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "treasury_slippage_usd",
		Help:    "Slippage vs expected price (USD).",
		Buckets: prometheus.DefBuckets,
	}, []string{"asset_pair"})

	// UnhedgedExposure is a gauge of unhedged FX exposure per currency.
	UnhedgedExposure = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "treasury_unhedged_exposure_usd",
		Help: "Unhedged FX exposure per fiat currency (USD).",
	}, []string{"fiat_currency"})

	// FloatUSD is a gauge of the current float per currency.
	FloatUSD = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "treasury_float_usd",
		Help: "Current float (short fiat) per currency, USD.",
	}, []string{"fiat_currency"})

	// FloatBreach counts float bound breaches.
	FloatBreach = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_float_breach_total",
		Help: "Float bound breaches per currency.",
	}, []string{"fiat_currency", "bound"})

	// LedgerPost counts ledger posting outcomes.
	LedgerPost = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_ledger_post_total",
		Help: "Ledger posting outcomes.",
	}, []string{"aggregate", "result"})

	// AuditEmit counts audit emission outcomes.
	AuditEmit = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_audit_emit_total",
		Help: "Audit emission outcomes.",
	}, []string{"aggregate", "result"})

	// FundingRequests counts funding request outcomes.
	FundingRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_funding_requests_total",
		Help: "Funding request outcomes.",
	}, []string{"asset", "result"})

	// RebalanceJobs counts rebalancing job outcomes.
	RebalanceJobs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "treasury_rebalance_jobs_total",
		Help: "Rebalancing job outcomes.",
	}, []string{"asset", "result"})
)