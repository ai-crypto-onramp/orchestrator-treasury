// Package app is the composition root for the Treasury Orchestration
// service. It loads config, opens stores (in-memory by default, Postgres
// when DB_URL is set), constructs the consumer / scheduler / executor /
// float / funding / hedge / ledger subsystems, wires the REST handlers,
// and starts the HTTP server plus background loops.
package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/aggregate"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/api"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/authtoken"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/batch"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/consumer"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/eventbus"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/float"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/funding"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/hedge"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/ledger"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/projection"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/postgres"
)

// Server bundles the wired service.
type Server struct {
	cfg       config.Config
	http      *http.Server
	mux       http.Handler
	consumer  *consumer.Consumer
	scheduler *batch.Scheduler
	float     *float.Tracker
	emitter   *ledger.Emitter
	mu        sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	db        *postgres.DB
}

// Build constructs the server from config. When DB_URL is empty it uses
// in-memory stores (only allowed in DEV_MODE=1); when set it opens Postgres
// and runs migrations.
func Build(cfg config.Config) (*Server, error) {
	// Apply essential defaults when the caller constructed Config
	// directly (without going through config.Load).
	if cfg.TxOrchEventTopic == "" {
		cfg.TxOrchEventTopic = "tx.completed"
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if cfg.BatchIntervalSeconds <= 0 {
		cfg.BatchIntervalSeconds = 30
	}
	if cfg.BatchSizeThresholdUSD.LessThanOrEqual(decimal.Zero) {
		cfg.BatchSizeThresholdUSD = decimal.NewFromInt(50000)
	}

	ctx := context.Background()
	devMode := os.Getenv("DEV_MODE") == "1"
	if devMode {
		log.Printf("DEV_MODE=1: stub/fake clients in use — NOT FOR PRODUCTION")
	}

	var (
		batchStore      store.BatchStore
		membershipStore store.MembershipStore
		orderStore      store.AggregateOrderStore
		fundingStore    store.FundingStore
		floatStore      store.FloatStore
		rebalStore      store.RebalancingStore
		outboxStore     store.OutboxStore
		unit            store.Unit
		db              *postgres.DB
	)

	if cfg.DBURL != "" {
		var err error
		db, err = postgres.Open(ctx, cfg.DBURL)
		if err != nil {
			return nil, err
		}
		batchStore = db.Batch()
		membershipStore = db.Membership()
		orderStore = db.Order()
		fundingStore = db.Funding()
		floatStore = db.Float()
		rebalStore = db.Rebalance()
		outboxStore = db.Outbox()
		unit = db
	} else {
		if !devMode {
			return nil, errors.New("DB_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		all := memstore.NewAll()
		batchStore = all.Batch
		membershipStore = all.Membership
		orderStore = all.Order
		fundingStore = all.Funding
		floatStore = all.Float
		rebalStore = all.Rebalance
		outboxStore = all.Outbox
		unit = all
	}

	idem, err := idempotency.Open(ctx, cfg.RedisURL)
	if err != nil {
		return nil, err
	}
	cadence := idempotency.NewCadenceLock(idem, time.Duration(cfg.BatchIntervalSeconds)*time.Second)

	clients_, err := buildDownstreamClients(cfg, devMode)
	if err != nil {
		return nil, err
	}

	emitter := ledger.New(ledger.Deps{Outbox: outboxStore, Ledger: clients_.ledger, Audit: clients_.audit})

	proj := projection.New(5 * time.Minute)

	floatTracker := float.New(float.Deps{
		Cfg:    cfg,
		Floats: floatStore,
		Unit:   unit,
		BuildAdjustOutbox: func(ctx context.Context, fiat string, amount decimal.Decimal, batchID uuid.UUID) []*store.OutboxEntry {
			return []*store.OutboxEntry{ledger.BuildEntry(ledger.AggFloat, ledger.EvFloatAdjust, ledger.Key(ledger.AggFloat, ledger.EvFloatAdjust, batchID), ledger.Payload{
				BatchID:      batchID,
				NotionalUSD:  amount,
				FiatCurrency: fiat,
			})}
		},
	})

	hedger := hedge.New(hedge.Deps{FX: clients_.fx, Orders: orderStore, Idem: idem})

	executor := aggregate.New(aggregate.Deps{
		Batches:          batchStore,
		Orders:           orderStore,
		Liquidity:        clients_.liquidity,
		Idem:             idem,
		ExpectedPriceFor: func(assetPair string) decimal.Decimal { return decimal.NewFromInt(50000) },
		OnFill: func(ctx context.Context, b *store.Batch, o *store.AggregateOrder) {
			fiat := fiatOf(b.AssetPair)
			cryptoAsset := cryptoOf(b.AssetPair)
			_ = floatTracker.OnAggregateFill(ctx, b, o, fiat, cryptoAsset)
			_, _ = hedger.OnAggregateFill(ctx, b, o, fiat)
			aggEvent := ledger.BuildEntry(ledger.AggAggregate, ledger.EvAggregateExec, ledger.Key(ledger.AggAggregate, ledger.EvAggregateExec, b.ID), ledger.Payload{
				BatchID:      b.ID,
				NotionalUSD:  o.NotionalUSD,
				Asset:        cryptoAsset,
				FiatCurrency: fiat,
			})
			_, _, _ = unit.UpdateBatchStatusWithOutbox(ctx, b.ID, store.BatchExecuting, store.BatchSettled, nil, []*store.OutboxEntry{aggEvent})
		},
	})

	scheduler := batch.New(batch.Deps{
		Cfg:         cfg,
		Batches:     batchStore,
		Memberships: membershipStore,
		Lock:        cadence,
		Unit:        unit,
		BuildCloseOutbox: func(ctx context.Context, b *store.Batch, reason batch.CloseReason) []*store.OutboxEntry {
			return []*store.OutboxEntry{ledger.BuildEntry(ledger.AggBatch, ledger.EvBatchClose, ledger.Key(ledger.AggBatch, ledger.EvBatchClose, b.ID), ledger.Payload{
				BatchID:     b.ID,
				NotionalUSD: b.NotionalUSD,
			})}
		},
		OnClose: func(ctx context.Context, b *store.Batch, reason batch.CloseReason) {
			_, _ = executor.SubmitBatch(ctx, b.ID)
		},
	})

	fundingMgr := funding.New(funding.Deps{
		Cfg:        cfg,
		Funding:    fundingStore,
		Rebalance:  rebalStore,
		Wallet:     clients_.wallet,
		Idem:       idem,
		Projection: proj,
		Unit:       unit,
		BuildFundingOutbox: func(ctx context.Context, fr *store.FundingRequest) []*store.OutboxEntry {
			return []*store.OutboxEntry{ledger.BuildEntry(ledger.AggFunding, ledger.EvFunding, ledger.Key(ledger.AggFunding, ledger.EvFunding, fr.ID), ledger.Payload{
				Asset:       fr.Asset,
				NotionalUSD: fr.Amount,
			})}
		},
		OnRebalance: func(ctx context.Context, job *store.RebalancingJob) {
			_ = emitter.Append(ctx, ledger.AggRebalance, ledger.EvRebalance, ledger.Key(ledger.AggRebalance, ledger.EvRebalance, job.ID), ledger.Payload{
				Asset:       job.Asset,
				NotionalUSD: job.Amount,
			})
		},
	})

	httpPush := eventbus.NewHTTPPush()
	subscriber := eventbus.EventSubscriber(httpPush)
	if cfg.EventBusURL != "" {
		if ks, err := eventbus.NewKafkaSubscriberFromURL(cfg.EventBusURL, cfg.EventBusGroupID); err == nil {
			subscriber = ks
		} else {
			log.Printf("app: kafka subscriber init failed, using http-push: %v", err)
		}
	}

	cons := consumer.New(consumer.Deps{
		Topic:       cfg.TxOrchEventTopic,
		Batches:     batchStore,
		Memberships: membershipStore,
		Idem:        idem,
		Subscriber:  subscriber,
		Unit:        unit,
		BuildBatchOpenOutbox: func(ctx context.Context, b *store.Batch) []*store.OutboxEntry {
			return []*store.OutboxEntry{ledger.BuildEntry(ledger.AggBatch, ledger.EvBatchOpen, ledger.Key(ledger.AggBatch, ledger.EvBatchOpen, b.ID), ledger.Payload{
				BatchID: b.ID,
			})}
		},
	})

	mux := api.NewRouter(&api.Deps{
		Batches:   batchStore,
		Members:   membershipStore,
		Orders:    orderStore,
		Scheduler: scheduler,
		Float:     floatTracker,
		Funding:   fundingMgr,
		Readiness: buildReadinessChecks(db, cfg),
	})

	root := http.NewServeMux()
	root.Handle("/", mux)
	root.Handle("/v1/events/", httpPush.HTTPHandler())
	root.Handle("/metrics", promhttp.Handler())
	secret, bypass := authtoken.SecretFromEnv()
	authed := authtoken.Middleware(secret, bypass)(root)
	wrapped := otelhttp.NewHandler(authed, "treasury-orchestration")

	srv := &Server{
		cfg:       cfg,
		mux:       wrapped,
		consumer:  cons,
		scheduler: scheduler,
		float:     floatTracker,
		emitter:   emitter,
		db:        db,
		http: &http.Server{
			Addr:              ":" + cfg.Port,
			Handler:           wrapped,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
	return srv, nil
}

// Run starts the HTTP server and the background loops and blocks until
// SIGINT/SIGTERM.
func (s *Server) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	s.startLoops(ctx)
	log.Printf("treasury-orchestration listening on :%s", s.cfg.Port)
	errCh := make(chan error, 1)
	go func() { errCh <- s.http.ListenAndServe() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return err
	case <-sig:
		return s.Shutdown()
	}
}

func (s *Server) startLoops(ctx context.Context) {
	s.wg.Add(4)
	go func() { defer s.wg.Done(); _ = s.scheduler.Run(ctx) }()
	go func() { defer s.wg.Done(); _ = s.consumer.Run(ctx) }()
	go func() { defer s.wg.Done(); _ = s.emitter.RunDispatcherLoop(ctx, 5*time.Second) }()
	go func() { defer s.wg.Done(); _ = s.float.RunSweeperLoop(ctx, 30*time.Second) }()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if s.http != nil {
		err = s.http.Shutdown(ctx)
	}
	s.wg.Wait()
	if s.db != nil {
		_ = s.db.Close()
	}
	return err
}

// HTTPHandler returns the wired HTTP handler (test helper).
func (s *Server) HTTPHandler() http.Handler { return s.mux }

// ErrStopped is returned when the server has been stopped.
var ErrStopped = errors.New("app: stopped")

// fiatOf extracts the fiat side of an "ASSET/FIAT" pair, defaulting to
// USD.
func fiatOf(assetPair string) string {
	for i := 0; i < len(assetPair); i++ {
		if assetPair[i] == '/' {
			return assetPair[i+1:]
		}
	}
	return "USD"
}

// cryptoOf extracts the crypto side of an "ASSET/FIAT" pair.
func cryptoOf(assetPair string) string {
	for i := 0; i < len(assetPair); i++ {
		if assetPair[i] == '/' {
			return assetPair[:i]
		}
	}
	return assetPair
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

type downstreamClients struct {
	liquidity clients.LiquidityRouting
	fx        clients.FXHedging
	wallet    clients.WalletManagement
	ledger    clients.LedgerAccounting
	audit     clients.AuditLog
}

func buildDownstreamClients(cfg config.Config, devMode bool) (downstreamClients, error) {
	var dc downstreamClients
	liq, err := buildLiquidity(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.liquidity = liq
	fx, err := buildFX(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.fx = fx
	w, err := buildWallet(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.wallet = w
	lg, err := buildLedger(cfg, devMode)
	if err != nil {
		return dc, err
	}
	dc.ledger = lg
	au, err := buildAudit(devMode)
	if err != nil {
		return dc, err
	}
	dc.audit = au
	return dc, nil
}

// httpHealth probes the downstream service's /healthz endpoint. It is
// used by /readyz to verify that wired vendor/peer clients are reachable.
// A 2xx response is treated as healthy; any other status or transport
// error is treated as down.
func httpHealth(baseURL string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/healthz", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("upstream %s: status %d", baseURL, resp.StatusCode)
		}
		return nil
	}
}

// buildReadinessChecks assembles the /readyz dependency probes from the
// wired stores and configured downstream clients. In DEV_MODE with the
// in-memory store no DB pinger is available, so the DB check is omitted
// (the service is considered ready). Downstream HTTP clients are probed
// only when their URL is configured.
func buildReadinessChecks(db *postgres.DB, cfg config.Config) []api.ReadinessCheck {
	var checks []api.ReadinessCheck
	if db != nil {
		checks = append(checks, api.ReadinessCheck{Name: "db", Check: db.Ping})
	}
	if cfg.LiquidityRoutingURL != "" {
		checks = append(checks, api.ReadinessCheck{Name: "liquidity", Check: httpHealth(cfg.LiquidityRoutingURL)})
	}
	if cfg.FXHedgingURL != "" {
		checks = append(checks, api.ReadinessCheck{Name: "fx", Check: httpHealth(cfg.FXHedgingURL)})
	}
	if cfg.WalletMgmtURL != "" {
		checks = append(checks, api.ReadinessCheck{Name: "wallet", Check: httpHealth(cfg.WalletMgmtURL)})
	}
	if cfg.LedgerURL != "" {
		checks = append(checks, api.ReadinessCheck{Name: "ledger", Check: httpHealth(cfg.LedgerURL)})
	}
	return checks
}

func buildLiquidity(cfg config.Config, devMode bool) (clients.LiquidityRouting, error) {
	if cfg.LiquidityRoutingURL != "" {
		return clients.NewResilientLiquidity(
			clients.NewHTTPLiquidity(cfg.LiquidityRoutingURL),
			clients.DefaultRetry(),
			clients.DefaultCircuitBreaker(),
		), nil
	}
	if !devMode {
		return nil, errors.New("LIQUIDITY_ROUTING_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeLiquidity(clients.FillResult{FillPrice: decimal.NewFromInt(50000), TotalFilled: decimal.NewFromInt(1)}), nil
}

func buildFX(cfg config.Config, devMode bool) (clients.FXHedging, error) {
	if cfg.FXHedgingURL != "" {
		return clients.NewResilientFX(
			clients.NewHTTPFX(cfg.FXHedgingURL),
			clients.DefaultRetry(),
			clients.DefaultCircuitBreaker(),
		), nil
	}
	if !devMode {
		return nil, errors.New("FX_HEDGING_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeFX(clients.HedgeResult{HedgedNotional: decimal.Decimal{}}), nil
}

func buildWallet(cfg config.Config, devMode bool) (clients.WalletManagement, error) {
	if cfg.WalletMgmtURL != "" {
		return clients.NewHTTPWallet(cfg.WalletMgmtURL), nil
	}
	if !devMode {
		return nil, errors.New("WALLET_MGMT_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeWallet(clients.FundingMoveResult{Completed: true, TxID: "stub"}), nil
}

func buildLedger(cfg config.Config, devMode bool) (clients.LedgerAccounting, error) {
	if cfg.LedgerURL != "" {
		return clients.NewHTTPLedger(cfg.LedgerURL), nil
	}
	if !devMode {
		return nil, errors.New("LEDGER_URL not set and DEV_MODE!=1; refusing to start in production mode")
	}
	return clients.NewFakeLedger(), nil
}

func buildAudit(devMode bool) (clients.AuditLog, error) {
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		kc, err := clients.NewKafkaAudit(splitCSV(brokers))
		if err != nil {
			if devMode {
				log.Printf("warn: audit kafka init failed (DEV_MODE): %v; using fake audit", err)
				return clients.NewFakeAudit(), nil
			}
			return nil, fmt.Errorf("audit kafka init: %w", err)
		}
		return kc, nil
	}
	if devMode {
		log.Printf("warn: KAFKA_BROKERS unset and DEV_MODE=1; audit events recorded in-memory only")
		return clients.NewFakeAudit(), nil
	}
	return nil, errors.New("KAFKA_BROKERS unset and DEV_MODE!=1; refusing to start in production mode")
}
