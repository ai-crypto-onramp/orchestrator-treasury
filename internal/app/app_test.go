package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/eventbus"
)

func TestMain(m *testing.M) {
	os.Setenv("DEV_MODE", "1")
	os.Exit(m.Run())
}

// TestApp_FullFlow exercises the in-memory composition root: build the
// server, POST a tx completion event, observe a batch membership, close
// the batch (manual), and verify the aggregate order is persisted.
func TestApp_FullFlow(t *testing.T) {
	cfg := config.Config{
		Port:                  "0",
		BatchIntervalSeconds:  3600,
		BatchSizeThresholdUSD: decimal.NewFromInt(100000),
		SettlementDays:        map[string]int{"USD": 2},
	}
	srv, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start background loops.
	go func() { _ = srv.scheduler.Run(ctx) }()
	go func() { _ = srv.consumer.Run(ctx) }()
	go func() { _ = srv.emitter.RunDispatcherLoop(ctx, 200*time.Millisecond) }()
	go func() { _ = srv.float.RunSweeperLoop(ctx, 200*time.Millisecond) }()
	time.Sleep(50 * time.Millisecond) // allow loops to subscribe

	hs := httptest.NewServer(srv.HTTPHandler())
	defer hs.Close()

	// POST a tx completion event.
	ev := `{"tx_id":"tx1","asset":"BTC","fiat_currency":"USD","amount":1,"notional_usd":1000,"user_id":"u1"}`
	resp, err := http.Post(hs.URL+"/v1/events/tx.completed", "application/json", strings.NewReader(ev))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("event post code=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// Wait for the consumer to persist the membership + open batch.
	deadline := time.Now().Add(2 * time.Second)
	var batchID string
	for time.Now().Before(deadline) {
		listResp, _ := http.Get(hs.URL + "/v1/batches")
		if listResp != nil {
			var out struct {
				Batches []struct {
					ID        string `json:"id"`
					AssetPair string `json:"asset_pair"`
					Status    string `json:"status"`
				} `json:"batches"`
			}
			_ = json.NewDecoder(listResp.Body).Decode(&out)
			listResp.Body.Close()
			if len(out.Batches) >= 1 && out.Batches[0].Status == "OPEN" {
				batchID = out.Batches[0].ID
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if batchID == "" {
		t.Fatal("no open batch created")
	}

	// Manual close.
	closeResp, err := http.Post(hs.URL+"/v1/batches/"+batchID+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if closeResp.StatusCode != http.StatusOK {
		t.Fatalf("close code=%d", closeResp.StatusCode)
	}
	closeResp.Body.Close()

	// The OnClose callback submits the aggregate order. Verify the batch
	// transitioned to settled (or executing) and an order exists.
	deadline = time.Now().Add(2 * time.Second)
	var orderStatus string
	for time.Now().Before(deadline) {
		getResp, _ := http.Get(hs.URL + "/v1/batches/" + batchID)
		if getResp != nil {
			var out struct {
				Batch struct {
					Status string `json:"status"`
				} `json:"batch"`
				Order *struct {
					Status string `json:"status"`
				} `json:"order"`
			}
			_ = json.NewDecoder(getResp.Body).Decode(&out)
			getResp.Body.Close()
			if out.Order != nil {
				orderStatus = out.Order.Status
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if orderStatus == "" {
		t.Fatal("expected aggregate order after close")
	}

	// Float position should reflect the fill.
	floatResp, err := http.Get(hs.URL + "/v1/float/USD")
	if err != nil {
		t.Fatal(err)
	}
	if floatResp.StatusCode != http.StatusOK {
		t.Fatalf("float code=%d", floatResp.StatusCode)
	}
	var pos struct {
		ShortFiatAmount decimal.Decimal `json:"short_fiat_amount"`
	}
	_ = json.NewDecoder(floatResp.Body).Decode(&pos)
	floatResp.Body.Close()
	if !pos.ShortFiatAmount.GreaterThan(decimal.Zero) {
		t.Fatalf("float short=%s want >0", pos.ShortFiatAmount.String())
	}

	// Metrics endpoint should be served.
	metricsResp, err := http.Get(hs.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics code=%d", metricsResp.StatusCode)
	}
	metricsResp.Body.Close()

	// /v1/healthz and /readyz on the composed router.
	for _, p := range []string{"/healthz", "/readyz"} {
		r, _ := http.Get(hs.URL + p)
		if r == nil || r.StatusCode != http.StatusOK {
			t.Fatalf("%s code=%v", p, r)
		}
		r.Body.Close()
	}

	// Dedup: re-post the same event; should not create a second membership.
	dupResp, _ := http.Post(hs.URL+"/v1/events/tx.completed", "application/json", strings.NewReader(ev))
	if dupResp != nil {
		if dupResp.StatusCode != http.StatusAccepted {
			t.Fatalf("dup event code=%d", dupResp.StatusCode)
		}
		dupResp.Body.Close()
	}
	time.Sleep(100 * time.Millisecond)
	listResp, _ := http.Get(hs.URL + "/v1/batches/" + batchID)
	var out struct {
		Memberships []any `json:"memberships"`
	}
	_ = json.NewDecoder(listResp.Body).Decode(&out)
	listResp.Body.Close()
	if len(out.Memberships) != 1 {
		t.Fatalf("memberships=%d want 1 (dedup)", len(out.Memberships))
	}

	_ = eventbus.TxCompletedEvent{}
}

// TestBuild_DefaultsApplied exercises the defaulting branch of Build when
// the caller passes an empty Config directly (not via config.Load).
func TestBuild_DefaultsApplied(t *testing.T) {
	srv, err := Build(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	if srv.cfg.Port != "8080" {
		t.Fatalf("expected default port 8080, got %q", srv.cfg.Port)
	}
	if srv.cfg.BatchIntervalSeconds != 30 {
		t.Fatalf("expected default interval 30, got %d", srv.cfg.BatchIntervalSeconds)
	}
	if !srv.cfg.BatchSizeThresholdUSD.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("expected default threshold 50000, got %s", srv.cfg.BatchSizeThresholdUSD.String())
	}
	if srv.cfg.TxOrchEventTopic != "tx.completed" {
		t.Fatalf("expected default topic, got %q", srv.cfg.TxOrchEventTopic)
	}
}

// TestBuild_ExternalClientURLs exercises the Build branches that wire real
// HTTP clients (liquidity / fx / wallet / ledger / audit) when their URLs
// are set. The clients point at stub HTTP servers so no real downstream is
// required.
func TestBuild_ExternalClientURLs(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer stub.Close()

	cfg := config.Config{
		Port:                 "0",
		LiquidityRoutingURL:  stub.URL,
		FXHedgingURL:         stub.URL,
		WalletMgmtURL:        stub.URL,
		LedgerURL:            stub.URL,
		AuditLogURL:          stub.URL,
		BatchIntervalSeconds: 3600,
	}
	srv, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	if srv.http == nil {
		t.Fatal("expected http server wired")
	}
}

// TestBuild_DBURLErrorsOutWithoutPostgres exercises the DBURL branch of
// Build. With a non-empty DBURL pointing at an unreachable host, postgres
// Open should fail and Build should return that error.
func TestBuild_DBURLErrorsOutWithoutPostgres(t *testing.T) {
	cfg := config.Config{
		Port:  "0",
		DBURL: "postgres://nobody:nopw@127.0.0.1:1/db?sslmode=disable",
	}
	srv, err := Build(cfg)
	if err == nil {
		// If a server was returned (unexpected), shut it down.
		if srv != nil {
			srv.Shutdown()
		}
		t.Fatal("expected error when DBURL is unreachable")
	}
}

// TestBuild_RedisEmptyUsesMem exercises the RedisURL empty branch.
func TestBuild_RedisEmptyUsesMem(t *testing.T) {
	cfg := config.Config{Port: "0", RedisURL: ""}
	srv, err := Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
}

// TestServer_HTTPHandlerAndHealthz verifies HTTPHandler returns a
// functioning router serving healthz.
func TestServer_HTTPHandlerAndHealthz(t *testing.T) {
	srv, err := Build(config.Config{Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	hs := httptest.NewServer(srv.HTTPHandler())
	defer hs.Close()
	resp, err := http.Get(hs.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz code=%d", resp.StatusCode)
	}
}

// TestServer_ShutdownWithoutRun covers Shutdown when cancel is nil (Run
// never called) and no http server was started via Run.
func TestServer_ShutdownWithoutRun(t *testing.T) {
	srv, err := Build(config.Config{Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	// http field is set in Build; Shutdown should close it cleanly.
	if err := srv.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestServer_RunShutdown exercises the Run loop: it starts the HTTP server
// and background loops, then Shutdown from another goroutine forces Run to
// return via the errCh path (http.ErrServerClosed). This covers Run,
// startLoops, and the Run-internal Shutdown path.
func TestServer_RunShutdown(t *testing.T) {
	srv, err := Build(config.Config{Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Run() }()
	// Give the server a moment to bind and start loops.
	time.Sleep(150 * time.Millisecond)
	// Force shutdown from another goroutine; Run returns via errCh.
	go func() { _ = srv.Shutdown() }()
	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("run returned unexpected err: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}

// TestFiatOfAndCryptoOf exercises the asset-pair parser helpers including
// the no-slash default branches.
func TestFiatOfAndCryptoOf(t *testing.T) {
	cases := []struct {
		in, fiat, crypto string
	}{
		{"BTC/USD", "USD", "BTC"},
		{"ETH/EUR", "EUR", "ETH"},
		{"BTC", "USD", "BTC"}, // no slash -> fiat defaults USD, crypto is whole string
		{"", "USD", ""},       // empty -> USD, empty
	}
	for _, c := range cases {
		if got := fiatOf(c.in); got != c.fiat {
			t.Errorf("fiatOf(%q)=%q want %q", c.in, got, c.fiat)
		}
		if got := cryptoOf(c.in); got != c.crypto {
			t.Errorf("cryptoOf(%q)=%q want %q", c.in, got, c.crypto)
		}
	}
}
