package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/batch"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/float"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/funding"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/projection"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func newDeps(t *testing.T) (*Deps, *memstore.All, *batch.Scheduler) {
	t.Helper()
	all := memstore.NewAll()
	cfg := config.Config{BatchIntervalSeconds: 3600, BatchSizeThresholdUSD: decimal.NewFromInt(50000), SettlementDays: map[string]int{"USD": 2}}
	lock := idempotency.NewCadenceLock(idempotency.NewMem(), time.Duration(cfg.BatchIntervalSeconds)*time.Second)
	sched := batch.New(batch.Deps{Cfg: cfg, Batches: all.Batch, Memberships: all.Membership, Lock: lock})
	fMgr := funding.New(funding.Deps{
		Cfg:        cfg,
		Funding:    all.Funding,
		Rebalance:  all.Rebalance,
		Wallet:     clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:       idempotency.NewMem(),
		Projection: projection.New(time.Minute),
	})
	fTracker := float.New(float.Deps{Cfg: cfg, Floats: all.Float})
	return &Deps{
		Batches:   all.Batch,
		Members:   all.Membership,
		Orders:    all.Order,
		Scheduler: sched,
		Float:     fTracker,
		Funding:   fMgr,
	}, all, sched
}

func TestHealthz(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
}

func TestReadyz(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
}

func TestReadyz_AllChecksDownReturns503(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Readiness = []ReadinessCheck{
		{Name: "db", Check: func(context.Context) error { return errors.New("down") }},
	}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("code=%d want 503", resp.StatusCode)
	}
}

func TestReadyz_PartialDownReturnsDegraded(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Readiness = []ReadinessCheck{
		{Name: "db", Check: func(context.Context) error { return nil }},
		{Name: "fx", Check: func(context.Context) error { return errors.New("down") }},
	}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "degraded" {
		t.Fatalf("status=%q want degraded", body.Status)
	}
	if body.Checks["fx"] != "down" {
		t.Fatalf("fx=%q want down", body.Checks["fx"])
	}
}

func TestBatches_ListAndCreate(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", Asset: "BTC", FiatCurrency: "USD", NotionalUSD: decimal.NewFromInt(1000)})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		Batches []*store.Batch `json:"batches"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Batches) != 1 {
		t.Fatalf("batches=%d want 1", len(out.Batches))
	}
}

func TestBatchByID_GetWithMemberships(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", Asset: "BTC", FiatCurrency: "USD", NotionalUSD: decimal.NewFromInt(1000)})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + b.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		Batch       *store.Batch          `json:"batch"`
		Memberships []*store.Membership   `json:"memberships"`
		Order       *store.AggregateOrder `json:"order"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Batch == nil || out.Batch.ID != b.ID {
		t.Fatalf("batch=%+v", out.Batch)
	}
	if len(out.Memberships) != 1 {
		t.Fatalf("members=%d want 1", len(out.Memberships))
	}
}

func TestBatchByID_NotFound(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("code=%d want 404", resp.StatusCode)
	}
}

func TestBatchByID_Close(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+b.ID.String()+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	got, _ := all.Batch.GetBatch(ctx, b.ID)
	if got.Status != store.BatchClosed {
		t.Fatalf("status=%s want closed", got.Status)
	}
}

func TestFloat_Get(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1234), LongCryptoAmount: decimal.NewFromFloat(0.02), LongCryptoAsset: "BTC"})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/float/USD")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var pos store.FloatPosition
	_ = json.NewDecoder(resp.Body).Decode(&pos)
	if !pos.ShortFiatAmount.Equal(decimal.NewFromInt(1234)) {
		t.Fatalf("short=%s want 1234", pos.ShortFiatAmount.String())
	}
}

func TestFunding_Create(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	body := `{"wallet_id":"hot1","asset":"BTC","amount":5,"source_venue":"v"}`
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var fr store.FundingRequest
	_ = json.NewDecoder(resp.Body).Decode(&fr)
	if fr.Status != store.FundingCompleted {
		t.Fatalf("status=%s want completed", fr.Status)
	}
}

func TestFunding_Create_InvalidAmount(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	body := `{"wallet_id":"hot1","asset":"BTC","amount":0}`
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestFunding_List(t *testing.T) {
	ctx := context.Background()
	d, _, _ := newDeps(t)
	_, _ = d.Funding.CreateFundingRequest(ctx, "h1", "BTC", decimal.NewFromInt(1), "")
	_, _ = d.Funding.CreateFundingRequest(ctx, "h2", "ETH", decimal.NewFromInt(2), "")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/funding-requests?status=" + string(store.FundingCompleted))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		FundingRequests []*store.FundingRequest `json:"funding_requests"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.FundingRequests) != 2 {
		t.Fatalf("funding=%d want 2", len(out.FundingRequests))
	}
}

func TestRebalancingJobs_List(t *testing.T) {
	ctx := context.Background()
	d, _, _ := newDeps(t)
	_, _ = d.Funding.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(1), "drift")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/rebalancing-jobs")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		RebalancingJobs []*store.RebalancingJob `json:"rebalancing_jobs"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.RebalancingJobs) != 1 {
		t.Fatalf("jobs=%d want 1", len(out.RebalancingJobs))
	}
}

func TestFunding_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/funding-requests", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

// --- additional coverage ---

func TestBatches_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/batches", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatches_ListBatchesError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Batches = errBatchStore{err: errAPI}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestBatchByID_MissingID(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	// "/v1/batches/" with empty id.
	resp, err := http.Get(srv.URL + "/v1/batches/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestBatchByID_InvalidID(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/notanumber")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestBatchByID_CloseMethodNotAllowed(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/batches/"+b.ID.String()+"/close", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatchByID_CloseSchedulerNil(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	d.Scheduler = nil
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+b.ID.String()+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("code=%d want 503", resp.StatusCode)
	}
}

func TestBatchByID_CloseAlreadyClosedConflict(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	// Close it once first.
	if _, err := d.Scheduler.CloseBatch(context.Background(), b.ID); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+b.ID.String()+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("code=%d want 409", resp.StatusCode)
	}
}

func TestBatchByID_CloseNotFound(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/batches/"+uuid.New().String()+"/close", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("code=%d want 404", resp.StatusCode)
	}
}

func TestBatchByID_GetInternalError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Batches = errBatchStore{err: errAPI}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestBatchByID_GetWithoutOrdersOrMembers(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	d.Orders = nil
	d.Members = nil
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + b.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}

func TestFloat_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/float/USD", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestFloat_MissingCurrency(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	// "/v1/float/" with empty currency.
	resp, err := http.Get(srv.URL + "/v1/float/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestFunding_MalformedJSON(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", strings.NewReader("{bad"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("code=%d want 400", resp.StatusCode)
	}
}

func TestFunding_PolicyViolation(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	body := `{"wallet_id":"h","asset":"BTC","amount":` + strconv.FormatInt(11_000_000, 10) + `}`
	resp, err := http.Post(srv.URL+"/v1/funding-requests", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("code=%d want 403", resp.StatusCode)
	}
}

func TestFunding_ListError(t *testing.T) {
	d, _, _ := newDeps(t)
	// Replace Funding with one backed by an erroring store.
	d.Funding = funding.New(funding.Deps{
		Cfg:     config.Config{},
		Funding: errFundingStore{err: errAPI},
	})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/funding-requests")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestRebalancing_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/rebalancing-jobs", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestRebalancing_ListError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Funding = funding.New(funding.Deps{
		Cfg:       config.Config{},
		Rebalance: errRebalStore{err: errAPI},
	})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/rebalancing-jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

// --- additional coverage: API error / branch paths ---

func TestFloat_GetError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Float = float.New(float.Deps{
		Cfg:    config.Config{},
		Floats: errFloatStore{err: errAPI},
	})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/float/USD")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestFloatList_Error(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Float = float.New(float.Deps{
		Cfg:    config.Config{},
		Floats: errFloatStore{err: errAPI},
	})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/float")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestFloatList_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/float", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestAggregateOrders_ListError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Orders = errOrderStore{err: errAPI}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/aggregate-orders")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestAggregateOrders_MethodNotAllowed(t *testing.T) {
	d, _, _ := newDeps(t)
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/aggregate-orders", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatchMemberships_MethodNotAllowed(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/batches/"+b.ID.String()+"/memberships", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatchMemberships_NilMembersStore(t *testing.T) {
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(context.Background(), "BTC/USD")
	d.Members = nil
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + b.ID.String() + "/memberships")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("code=%d want 503", resp.StatusCode)
	}
}

func TestBatchMemberships_ListError(t *testing.T) {
	d, _, _ := newDeps(t)
	d.Members = errMembershipStore{err: errAPI}
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	id := uuid.New().String()
	resp, err := http.Get(srv.URL + "/v1/batches/" + id + "/memberships")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("code=%d want 500", resp.StatusCode)
	}
}

func TestBatchByID_UnknownSubPathFallsThroughToGet(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + b.ID.String() + "/unknown")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}

func TestBatchByID_GetMethodNotAllowed(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/batches/"+b.ID.String(), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("code=%d want 405", resp.StatusCode)
	}
}

func TestBatches_ListWithFromTo(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	_, _ = all.Batch.OpenBatch(ctx, "BTC/USD")
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	from := time.Now().Add(-time.Hour).Format(time.RFC3339)
	to := time.Now().Add(time.Hour).Format(time.RFC3339)
	resp, err := http.Get(srv.URL + "/v1/batches?from=" + from + "&to=" + to)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}

// errFloatStore implements store.FloatStore returning err for every method.
type errFloatStore struct{ err error }

func (e errFloatStore) AddFloat(context.Context, *store.FloatPosition) (*store.FloatPosition, error) {
	return nil, e.err
}
func (e errFloatStore) GetFloat(context.Context, string) (*store.FloatPosition, error) {
	return nil, e.err
}
func (e errFloatStore) ListFloat(context.Context) ([]*store.FloatPosition, error) {
	return nil, e.err
}
func (e errFloatStore) ListMaturedFloat(context.Context, time.Time) ([]*store.FloatPosition, error) {
	return nil, e.err
}
func (e errFloatStore) SettleFloat(context.Context, uuid.UUID) (*store.FloatPosition, error) {
	return nil, e.err
}

// errOrderStore implements store.AggregateOrderStore returning err.
type errOrderStore struct{ err error }

func (e errOrderStore) CreateOrder(context.Context, *store.AggregateOrder) (*store.AggregateOrder, error) {
	return nil, e.err
}
func (e errOrderStore) GetOrderByBatch(context.Context, uuid.UUID) (*store.AggregateOrder, error) {
	return nil, e.err
}
func (e errOrderStore) ListOrders(context.Context, string) ([]*store.AggregateOrder, error) {
	return nil, e.err
}
func (e errOrderStore) UpdateOrderFill(context.Context, uuid.UUID, decimal.Decimal, decimal.Decimal, []store.VenueRoute) (*store.AggregateOrder, error) {
	return nil, e.err
}
func (e errOrderStore) SettleOrder(context.Context, uuid.UUID, decimal.Decimal) (*store.AggregateOrder, error) {
	return nil, e.err
}

// errMembershipStore implements store.MembershipStore returning err.
type errMembershipStore struct{ err error }

func (e errMembershipStore) AddMembership(context.Context, *store.Membership) (bool, error) {
	return false, e.err
}
func (e errMembershipStore) ListMemberships(context.Context, uuid.UUID) ([]*store.Membership, error) {
	return nil, e.err
}
func (e errMembershipStore) SumNotional(context.Context, uuid.UUID) (decimal.Decimal, error) {
	return decimal.Decimal{}, e.err
}
func (e errMembershipStore) ExistsByTxID(context.Context, string) (bool, error) {
	return false, e.err
}

func TestNewRouter_NilDepsOmitsRoutes(t *testing.T) {
	// A Deps with no Batches/Float/Funding should still serve healthz.
	srv := httptest.NewServer(NewRouter(&Deps{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d want 200", resp.StatusCode)
	}
}

// --- fakes ---

func TestBatchMemberships(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	b, _ := all.Batch.OpenBatch(ctx, "BTC/USD")
	_, _ = all.Membership.AddMembership(ctx, &store.Membership{BatchID: b.ID, TxID: "t1", Asset: "BTC", FiatCurrency: "USD", NotionalUSD: decimal.NewFromInt(1000)})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/batches/" + b.ID.String() + "/memberships")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		Memberships []*store.Membership `json:"memberships"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Memberships) != 1 {
		t.Fatalf("members=%d want 1", len(out.Memberships))
	}
}

func TestFloatList(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "USD", ShortFiatAmount: decimal.NewFromInt(1000), LongCryptoAmount: decimal.NewFromFloat(0.01), LongCryptoAsset: "BTC"})
	_, _ = all.Float.AddFloat(ctx, &store.FloatPosition{FiatCurrency: "EUR", ShortFiatAmount: decimal.NewFromInt(500), LongCryptoAmount: decimal.NewFromFloat(0.005), LongCryptoAsset: "BTC"})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/float")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		FloatPositions []*store.FloatPosition `json:"float_positions"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.FloatPositions) != 2 {
		t.Fatalf("positions=%d want 2", len(out.FloatPositions))
	}
}

func TestAggregateOrdersList(t *testing.T) {
	ctx := context.Background()
	d, all, _ := newDeps(t)
	batchID1, _ := uuid.NewV7()
	batchID2, _ := uuid.NewV7()
	_, _ = all.Order.CreateOrder(ctx, &store.AggregateOrder{BatchID: batchID1, AssetPair: "BTC/USD", NotionalUSD: decimal.NewFromInt(50000), Status: store.AggregateExecuting})
	_, _ = all.Order.CreateOrder(ctx, &store.AggregateOrder{BatchID: batchID2, AssetPair: "ETH/USD", NotionalUSD: decimal.NewFromInt(30000), Status: store.AggregateSettled})
	srv := httptest.NewServer(NewRouter(d))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/aggregate-orders")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("code=%d", resp.StatusCode)
	}
	var out struct {
		AggregateOrders []*store.AggregateOrder `json:"aggregate_orders"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if len(out.AggregateOrders) != 2 {
		t.Fatalf("orders=%d want 2", len(out.AggregateOrders))
	}
	resp2, err := http.Get(srv.URL + "/v1/aggregate-orders?status=" + string(store.AggregateSettled))
	if err != nil {
		t.Fatal(err)
	}
	_ = json.NewDecoder(resp2.Body).Decode(&out)
	if len(out.AggregateOrders) != 1 || out.AggregateOrders[0].Status != store.AggregateSettled {
		t.Fatalf("settled filter: got %+v", out.AggregateOrders)
	}
}

var errAPI = errors.New("api store boom")

type errBatchStore struct{ err error }

func (e errBatchStore) OpenBatch(context.Context, string) (*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) GetBatch(context.Context, uuid.UUID) (*store.Batch, error) { return nil, e.err }
func (e errBatchStore) ListBatches(context.Context, time.Time, time.Time) ([]*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) ListOpenBatches(context.Context) ([]*store.Batch, error) {
	return nil, e.err
}
func (e errBatchStore) UpdateBatchStatus(context.Context, uuid.UUID, store.BatchStatus, store.BatchStatus, func(*store.Batch)) (*store.Batch, bool, error) {
	return nil, false, e.err
}
func (e errBatchStore) SetBatchNotional(context.Context, uuid.UUID, decimal.Decimal) error {
	return e.err
}

type errFundingStore struct{ err error }

func (e errFundingStore) CreateFunding(context.Context, *store.FundingRequest) (*store.FundingRequest, error) {
	return nil, e.err
}
func (e errFundingStore) GetFunding(context.Context, uuid.UUID) (*store.FundingRequest, error) {
	return nil, e.err
}
func (e errFundingStore) UpdateFundingStatus(context.Context, uuid.UUID, store.FundingStatus) error {
	return e.err
}
func (e errFundingStore) ListFunding(context.Context, string) ([]*store.FundingRequest, error) {
	return nil, e.err
}

type errRebalStore struct{ err error }

func (e errRebalStore) CreateJob(context.Context, *store.RebalancingJob) (*store.RebalancingJob, error) {
	return nil, e.err
}
func (e errRebalStore) ListJobs(context.Context, string) ([]*store.RebalancingJob, error) {
	return nil, e.err
}
func (e errRebalStore) UpdateJobStatus(context.Context, uuid.UUID, store.RebalanceStatus) error {
	return e.err
}
