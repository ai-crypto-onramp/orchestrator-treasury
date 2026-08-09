package ledger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/clients"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store/memstore"
)

func TestEmitter_AppendAndDispatch(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	ledgerCli := clients.NewFakeLedger()
	auditCli := clients.NewFakeAudit()
	e := New(Deps{Outbox: all.Outbox, Ledger: ledgerCli, Audit: auditCli})

	if err := e.Append(ctx, AggBatch, EvBatchClose, "batch.close:1", Payload{BatchID: uuid.New(), NotionalUSD: decimal.NewFromInt(50000)}); err != nil {
		t.Fatal(err)
	}
	n, err := e.Dispatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("dispatched=%d want 1", n)
	}
	ledgerCalls := ledgerCli.Calls()
	if len(ledgerCalls) != 1 {
		t.Fatalf("ledger calls=%d want 1", len(ledgerCalls))
	}
	if ledgerCalls[0].EventType != "batch.close" {
		t.Fatalf("event_type=%s want batch.close", ledgerCalls[0].EventType)
	}
	auditCalls := auditCli.Calls()
	if len(auditCalls) != 1 {
		t.Fatalf("audit calls=%d want 1", len(auditCalls))
	}
	if auditCalls[0].EventType != "batch.close" {
		t.Fatalf("event_type=%s want batch.close", auditCalls[0].EventType)
	}
	// Outbox entry marked emitted.
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("pending=%d want 0", len(pending))
	}
}

func TestEmitter_DedupByKey(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	e := New(Deps{Outbox: all.Outbox, Ledger: clients.NewFakeLedger(), Audit: clients.NewFakeAudit()})
	if err := e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	// Same dedup key -> outbox Append returns false, no new entry.
	if err := e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()}); err != nil {
		t.Fatal(err)
	}
	snap := all.Outbox.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("outbox entries=%d want 1", len(snap))
	}
}

func TestEmitter_DispatchRetriesOnLedgerError(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	ledgerCli := clients.NewFakeLedger()
	ledgerCli.SetError(clients.ErrUnavailable)
	e := New(Deps{Outbox: all.Outbox, Ledger: ledgerCli, Audit: clients.NewFakeAudit()})
	_ = e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()})
	n, _ := e.Dispatch(ctx, 10)
	if n != 0 {
		t.Fatalf("dispatched=%d want 0 (ledger failed)", n)
	}
	// Entry remains pending.
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("pending=%d want 1 (retry)", len(pending))
	}
	// Clear the error and dispatch again.
	ledgerCli.SetError(nil)
	n2, _ := e.Dispatch(ctx, 10)
	if n2 != 1 {
		t.Fatalf("dispatched=%d want 1 on retry", n2)
	}
}

func TestEmitter_Key(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-00000000002a")
	if got := Key(AggBatch, EvBatchClose, id); got != "batch.batch.close:00000000-0000-0000-0000-00000000002a" {
		t.Fatalf("key=%q want batch.batch.close:00000000-0000-0000-0000-00000000002a", got)
	}
}

// --- additional coverage ---

func TestEmitter_DispatchAuditErrorKeepsPending(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	audit := clients.NewFakeAudit()
	audit.SetError(clients.ErrUnavailable)
	e := New(Deps{Outbox: all.Outbox, Ledger: clients.NewFakeLedger(), Audit: audit})
	_ = e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()})
	n, _ := e.Dispatch(ctx, 10)
	if n != 0 {
		t.Fatalf("dispatched=%d want 0 (audit failed)", n)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("pending=%d want 1 (retry)", len(pending))
	}
}

func TestEmitter_DispatchNilClientsSkipsPosting(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	// No Ledger, no Audit configured.
	e := New(Deps{Outbox: all.Outbox})
	_ = e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()})
	n, err := e.Dispatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("dispatched=%d want 1 (mark emitted only)", n)
	}
	pending, _ := all.Outbox.ListPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("pending=%d want 0", len(pending))
	}
}

func TestEmitter_DispatchListPendingError(t *testing.T) {
	ctx := context.Background()
	e := New(Deps{Outbox: errOutboxStore{}})
	n, err := e.Dispatch(ctx, 10)
	if err != errOutbox {
		t.Fatalf("err=%v want errOutbox", err)
	}
	if n != 0 {
		t.Fatalf("n=%d want 0", n)
	}
}

func TestEmitter_DispatchMarkEmittedError(t *testing.T) {
	ctx := context.Background()
	e := New(Deps{
		Outbox: &markErrOutbox{},
		Ledger: clients.NewFakeLedger(),
	})
	_ = e.Append(ctx, AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()})
	n, _ := e.Dispatch(ctx, 10)
	if n != 0 {
		t.Fatalf("dispatched=%d want 0 (mark emitted failed)", n)
	}
}

func TestEmitter_RunDispatcherLoopDefaultInterval(t *testing.T) {
	// interval <= 0 should default to 5s. Start, cancel immediately, and
	// verify Run returns ctx.Err() without panicking.
	all := memstore.NewAll()
	e := New(Deps{Outbox: all.Outbox, Ledger: clients.NewFakeLedger(), Audit: clients.NewFakeAudit()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.RunDispatcherLoop(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
}

func TestEmitter_RunDispatcherLoopDispatches(t *testing.T) {
	all := memstore.NewAll()
	ledgerCli := clients.NewFakeLedger()
	auditCli := clients.NewFakeAudit()
	e := New(Deps{Outbox: all.Outbox, Ledger: ledgerCli, Audit: auditCli})
	_ = e.Append(context.Background(), AggBatch, EvBatchClose, "k1", Payload{BatchID: uuid.New()})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.RunDispatcherLoop(ctx, 50*time.Millisecond) }()
	// Wait for at least one tick to dispatch.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(ledgerCli.Calls()) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunDispatcherLoop did not return after cancel")
	}
	if len(ledgerCli.Calls()) < 1 {
		t.Fatalf("ledger calls=%d want >=1", len(ledgerCli.Calls()))
	}
}

func TestEmitter_AppendOverwritesAggregateAndEventType(t *testing.T) {
	ctx := context.Background()
	all := memstore.NewAll()
	e := New(Deps{Outbox: all.Outbox, Ledger: clients.NewFakeLedger(), Audit: clients.NewFakeAudit()})
	// Pass a Payload with conflicting Aggregate/EventType; Append should
	// overwrite them with the explicit args.
	_ = e.Append(ctx, AggFunding, EvFunding, "k1", Payload{Aggregate: AggBatch, EventType: EvBatchOpen, BatchID: uuid.New()})
	n, _ := e.Dispatch(ctx, 10)
	if n != 1 {
		t.Fatalf("dispatched=%d want 1", n)
	}
}

// --- fakes ---

var errOutbox = errors.New("outbox boom")

type errOutboxStore struct{}

func (errOutboxStore) Append(context.Context, *store.OutboxEntry) (bool, error) {
	return false, errOutbox
}
func (errOutboxStore) ListPending(context.Context, int) ([]*store.OutboxEntry, error) {
	return nil, errOutbox
}
func (errOutboxStore) MarkEmitted(context.Context, uuid.UUID) error { return errOutbox }

// markErrOutbox succeeds for Append/ListPending but errors on MarkEmitted.
type markErrOutbox struct {
	rows []*store.OutboxEntry
}

func (m *markErrOutbox) Append(_ context.Context, e *store.OutboxEntry) (bool, error) {
	m.rows = append(m.rows, e)
	return true, nil
}
func (m *markErrOutbox) ListPending(_ context.Context, _ int) ([]*store.OutboxEntry, error) {
	return m.rows, nil
}
func (m *markErrOutbox) MarkEmitted(context.Context, uuid.UUID) error { return errOutbox }
