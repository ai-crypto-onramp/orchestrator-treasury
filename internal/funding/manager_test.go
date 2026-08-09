package funding

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/idempotency"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/projection"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func newMgr(t *testing.T) (*Manager, *memstore.All, *clients.FakeWallet, *projection.Model) {
	t.Helper()
	all := memstore.NewAll()
	wallet := clients.NewFakeWallet(clients.FundingMoveResult{Completed: true, TxID: "wtx"})
	proj := projection.New(60 * 1000_000_000) // 1m
	mgr := New(Deps{
		Cfg:        config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding:    all.Funding,
		Rebalance:  all.Rebalance,
		Wallet:     wallet,
		Idem:       idempotency.NewMem(),
		Projection: proj,
	})
	return mgr, all, wallet, proj
}

func TestManager_CreateFundingRequest_PersistsAndExecutes(t *testing.T) {
	ctx := context.Background()
	mgr, all, wallet, _ := newMgr(t)
	fr, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", decimal.NewFromInt(5), "venueA")
	if err != nil {
		t.Fatal(err)
	}
	if fr.Status != store.FundingCompleted {
		t.Fatalf("status=%s want completed", fr.Status)
	}
	calls := wallet.Calls()
	if len(calls) != 1 {
		t.Fatalf("wallet calls=%d want 1", len(calls))
	}
	if calls[0].WalletID != "hot1" || calls[0].Asset != "BTC" || !calls[0].Amount.Equal(decimal.NewFromInt(5)) {
		t.Fatalf("call=%+v", calls[0])
	}
	got, _ := all.Funding.GetFunding(ctx, fr.ID)
	if got.Status != store.FundingCompleted {
		t.Fatalf("persisted status=%s want completed", got.Status)
	}
}

func TestManager_CreateFundingRequest_RejectsInvalidAmount(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", decimal.Decimal{}, ""); err != ErrInvalidAmount {
		t.Fatalf("err=%v want ErrInvalidAmount", err)
	}
	if _, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", decimal.NewFromInt(-1), ""); err != ErrInvalidAmount {
		t.Fatalf("err=%v want ErrInvalidAmount", err)
	}
}

func TestManager_CreateFundingRequest_RejectsPolicyViolation(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.CreateFundingRequest(ctx, "hot1", "BTC", decimal.NewFromInt(int64(MaxFundingAmount)+1), ""); err != ErrPolicyViolation {
		t.Fatalf("err=%v want ErrPolicyViolation", err)
	}
}

func TestManager_Rebalance_PersistsAndExecutes(t *testing.T) {
	ctx := context.Background()
	mgr, all, wallet, _ := newMgr(t)
	job, err := mgr.Rebalance(ctx, "venueA", "hot1", "BTC", decimal.NewFromInt(2), "drift")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != store.RebalanceCompleted {
		t.Fatalf("status=%s want completed", job.Status)
	}
	calls := wallet.Calls()
	if len(calls) != 1 {
		t.Fatalf("wallet calls=%d want 1", len(calls))
	}
	jobs, _ := all.Rebalance.ListJobs(ctx, "")
	if len(jobs) != 1 {
		t.Fatalf("persisted jobs=%d want 1", len(jobs))
	}
}

func TestManager_Rebalance_RejectsPolicyViolation(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(int64(MaxFundingAmount)+1), ""); err != ErrPolicyViolation {
		t.Fatalf("err=%v want ErrPolicyViolation", err)
	}
}

func TestManager_ListFunding_StatusFilter(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	_, _ = mgr.CreateFundingRequest(ctx, "h1", "BTC", decimal.NewFromInt(1), "")
	_, _ = mgr.CreateFundingRequest(ctx, "h2", "ETH", decimal.NewFromInt(1), "")
	all, _ := mgr.ListFunding(ctx, "")
	if len(all) != 2 {
		t.Fatalf("all=%d want 2", len(all))
	}
	completed, _ := mgr.ListFunding(ctx, string(store.FundingCompleted))
	if len(completed) != 2 {
		t.Fatalf("completed=%d want 2", len(completed))
	}
	pending, _ := mgr.ListFunding(ctx, string(store.FundingPending))
	if len(pending) != 0 {
		t.Fatalf("pending=%d want 0", len(pending))
	}
}

func TestManager_ListJobs_StatusFilter(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	_, _ = mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(1), "x")
	_, _ = mgr.Rebalance(ctx, "c", "d", "ETH", decimal.NewFromInt(2), "y")
	all, _ := mgr.ListJobs(ctx, "")
	if len(all) != 2 {
		t.Fatalf("all=%d want 2", len(all))
	}
}

// --- additional coverage ---

func TestManager_CreateFundingRequest_WalletErrorRejects(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	wallet := clients.NewFakeWallet(clients.FundingMoveResult{Completed: true})
	wallet.SetError(clients.ErrUnavailable)
	mgr := New(Deps{
		Cfg:     config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding: all.Funding,
		Wallet:  wallet,
		Idem:    idempotency.NewMem(),
	})
	fr, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), "")
	if err != nil {
		t.Fatalf("expected nil err (request persisted as rejected), got %v", err)
	}
	got, _ := all.Funding.GetFunding(ctx, fr.ID)
	if got.Status != store.FundingRejected {
		t.Fatalf("status=%s want rejected", got.Status)
	}
}

func TestManager_CreateFundingRequest_UpdateFundingStatusError(t *testing.T) {
	ctx := context.Background()
	mgr := New(Deps{
		Cfg:     config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding: errFundingStore{}, // CreateFunding ok, UpdateFundingStatus errors
		Wallet:  clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:    idempotency.NewMem(),
	})
	_, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), "")
	if !errors.Is(err, errFundingErr) {
		t.Fatalf("err=%v want errFundingErr", err)
	}
}

func TestManager_CreateFundingRequest_OnFundingHook(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	called := make(chan uuid.UUID, 4)
	mgr := New(Deps{
		Cfg:       config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding:   all.Funding,
		Wallet:    clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:      idempotency.NewMem(),
		OnFunding: func(_ context.Context, fr *store.FundingRequest) { called <- fr.ID },
	})
	if _, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), ""); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-called:
		if id == uuid.Nil {
			t.Fatalf("expected non-nil funding id, got %s", id)
		}
	default:
		t.Fatal("expected OnFunding to fire")
	}
}

// TestManager_CreateFundingRequest_UnitAppendsOutbox verifies that when a
// Unit is configured the funding insert and the BuildFundingOutbox event
// are persisted together via Unit.CreateFundingWithOutbox.
func TestManager_CreateFundingRequest_UnitAppendsOutbox(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	mgr := New(Deps{
		Cfg:     config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding: all.Funding,
		Wallet:  clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:    idempotency.NewMem(),
		Unit:    all,
		BuildFundingOutbox: func(_ context.Context, fr *store.FundingRequest) []*store.OutboxEntry {
			return []*store.OutboxEntry{{Aggregate: "funding", EventType: "funding.create", DedupKey: "fc:" + fr.ID.String(), Payload: []byte("{}")}}
		},
	})
	fr, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), "")
	if err != nil {
		t.Fatal(err)
	}
	if fr.Status != store.FundingCompleted {
		t.Fatalf("status=%s want completed", fr.Status)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(pending))
	}
}

func TestManager_CreateFundingRequest_IdemErrorLogs(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	mgr := New(Deps{
		Cfg:     config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding: all.Funding,
		Wallet:  clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:    errIdemStore{},
	})
	// Should still complete (idem error is logged, not fatal).
	fr, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), "")
	if err != nil {
		t.Fatal(err)
	}
	if fr.Status != store.FundingCompleted {
		t.Fatalf("status=%s want completed", fr.Status)
	}
}

func TestManager_CreateFundingRequest_NilWalletSkipsDispatch(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	mgr := New(Deps{
		Cfg:     config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding: all.Funding,
		Wallet:  nil, // no wallet configured
		Idem:    idempotency.NewMem(),
	})
	fr, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), "")
	if err != nil {
		t.Fatal(err)
	}
	if fr.Status != store.FundingCompleted {
		t.Fatalf("status=%s want completed", fr.Status)
	}
}

func TestManager_Rebalance_RejectsInvalidAmount(t *testing.T) {
	ctx := context.Background()
	mgr, _, _, _ := newMgr(t)
	if _, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.Decimal{}, ""); err != ErrInvalidAmount {
		t.Fatalf("err=%v want ErrInvalidAmount", err)
	}
	if _, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(-1), ""); err != ErrInvalidAmount {
		t.Fatalf("err=%v want ErrInvalidAmount", err)
	}
}

func TestManager_Rebalance_WalletErrorReturnsErr(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	wallet := clients.NewFakeWallet(clients.FundingMoveResult{Completed: true})
	wallet.SetError(clients.ErrUnavailable)
	mgr := New(Deps{
		Cfg:       config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Rebalance: all.Rebalance,
		Wallet:    wallet,
		Idem:      idempotency.NewMem(),
	})
	job, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(5), "drift")
	if !errors.Is(err, clients.ErrUnavailable) {
		t.Fatalf("err=%v want ErrUnavailable", err)
	}
	// Job is persisted as rejected.
	got, _ := all.Rebalance.ListJobs(ctx, string(store.RebalanceRejected))
	if len(got) != 1 || got[0].ID != job.ID {
		t.Fatalf("expected 1 rejected job, got %+v", got)
	}
}

func TestManager_Rebalance_OnRebalanceHook(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	called := make(chan uuid.UUID, 4)
	mgr := New(Deps{
		Cfg:         config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Rebalance:   all.Rebalance,
		Wallet:      clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:        idempotency.NewMem(),
		OnRebalance: func(_ context.Context, job *store.RebalancingJob) { called <- job.ID },
	})
	if _, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(5), "drift"); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-called:
		if id == uuid.Nil {
			t.Fatalf("expected non-nil job id, got %s", id)
		}
	default:
		t.Fatal("expected OnRebalance to fire")
	}
}

func TestManager_Rebalance_NilWalletSkipsDispatch(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	mgr := New(Deps{
		Cfg:       config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Rebalance: all.Rebalance,
		Wallet:    nil,
		Idem:      idempotency.NewMem(),
	})
	job, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(5), "drift")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != store.RebalanceCompleted {
		t.Fatalf("status=%s want completed", job.Status)
	}
}

func TestManager_Rebalance_UpdateJobStatusError(t *testing.T) {
	ctx := context.Background()
	mgr := New(Deps{
		Cfg:       config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Rebalance: errRebalStore{}, // CreateJob ok, UpdateJobStatus errors
		Wallet:    clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:      idempotency.NewMem(),
	})
	_, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(5), "drift")
	if !errors.Is(err, errRebalErr) {
		t.Fatalf("err=%v want errRebalErr", err)
	}
}

func TestManager_Rebalance_CreateJobError(t *testing.T) {
	ctx := context.Background()
	mgr := New(Deps{
		Cfg:       config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Rebalance: errCreateJobStore{},
		Wallet:    clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:      idempotency.NewMem(),
	})
	if _, err := mgr.Rebalance(ctx, "a", "b", "BTC", decimal.NewFromInt(5), "drift"); !errors.Is(err, errRebalErr) {
		t.Fatalf("err=%v want errRebalErr", err)
	}
}

func TestManager_CreateFundingRequest_CreateFundingError(t *testing.T) {
	ctx := context.Background()
	mgr := New(Deps{
		Cfg:     config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding: errCreateFundingStore{},
		Wallet:  clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:    idempotency.NewMem(),
	})
	if _, err := mgr.CreateFundingRequest(ctx, "h", "BTC", decimal.NewFromInt(5), ""); !errors.Is(err, errFundingErr) {
		t.Fatalf("err=%v want errFundingErr", err)
	}
}

func TestManager_checkAndRebalance_NoTargetReturns(t *testing.T) {
	ctx := context.Background()
	f := newMgrWithCfg(t, config.Config{}) // no HotWalletTargets
	// No target configured -> early return, no funding created.
	f.mgr.checkAndRebalance(ctx, "BTC", "h")
	frs, _ := f.mgr.ListFunding(ctx, "")
	if len(frs) != 0 {
		t.Fatalf("expected 0 funding, got %d", len(frs))
	}
}

func TestManager_checkAndRebalance_NilProjection(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	mgr := New(Deps{
		Cfg:        config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}},
		Funding:    all.Funding,
		Rebalance:  all.Rebalance,
		Wallet:     clients.NewFakeWallet(clients.FundingMoveResult{Completed: true}),
		Idem:       idempotency.NewMem(),
		Projection: nil, // no projection -> demand 0 -> no shortfall
	})
	mgr.checkAndRebalance(ctx, "BTC", "h")
	frs, _ := mgr.ListFunding(ctx, "")
	if len(frs) != 0 {
		t.Fatalf("expected 0 funding (no projection), got %d", len(frs))
	}
}

func TestManager_RunRebalanceLoop_CancelImmediately(t *testing.T) {
	f := newMgrWithCfg(t, config.Config{HotWalletTargets: map[string]decimal.Decimal{"BTC": decimal.NewFromInt(10)}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.mgr.RunRebalanceLoop(ctx, 0, "BTC", "h")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
}

// --- fakes ---

var errFundingErr = errors.New("funding store boom")
var errRebalErr = errors.New("rebal store boom")

type errFundingStore struct{}

func (errFundingStore) CreateFunding(_ context.Context, f *store.FundingRequest) (*store.FundingRequest, error) {
	// Return a cloned row with an ID so the manager proceeds, then fail on
	// UpdateFundingStatus.
	c := *f
	c.ID = uuid.New()
	return &c, nil
}
func (errFundingStore) GetFunding(context.Context, uuid.UUID) (*store.FundingRequest, error) {
	return nil, errFundingErr
}
func (errFundingStore) UpdateFundingStatus(context.Context, uuid.UUID, store.FundingStatus) error {
	return errFundingErr
}
func (errFundingStore) ListFunding(context.Context, string) ([]*store.FundingRequest, error) {
	return nil, errFundingErr
}

// errCreateFundingStore fails on CreateFunding.
type errCreateFundingStore struct{ errFundingStore }

func (errCreateFundingStore) CreateFunding(context.Context, *store.FundingRequest) (*store.FundingRequest, error) {
	return nil, errFundingErr
}

type errRebalStore struct{}

func (errRebalStore) CreateJob(_ context.Context, j *store.RebalancingJob) (*store.RebalancingJob, error) {
	c := *j
	c.ID = uuid.New()
	return &c, nil
}
func (errRebalStore) ListJobs(context.Context, string) ([]*store.RebalancingJob, error) {
	return nil, nil
}
func (errRebalStore) UpdateJobStatus(context.Context, uuid.UUID, store.RebalanceStatus) error {
	return errRebalErr
}

// errCreateJobStore fails on CreateJob.
type errCreateJobStore struct{}

func (errCreateJobStore) CreateJob(context.Context, *store.RebalancingJob) (*store.RebalancingJob, error) {
	return nil, errRebalErr
}
func (errCreateJobStore) ListJobs(context.Context, string) ([]*store.RebalancingJob, error) {
	return nil, nil
}
func (errCreateJobStore) UpdateJobStatus(context.Context, uuid.UUID, store.RebalanceStatus) error {
	return nil
}

type errIdemStore struct{}

func (errIdemStore) CheckAndMark(context.Context, string, time.Duration) (bool, error) {
	return false, errFundingErr
}
func (errIdemStore) Delete(context.Context, string) error { return nil }
