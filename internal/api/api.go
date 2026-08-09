// Package api exposes the Treasury Orchestration REST API. Handlers are
// thin and delegate to the composed services. The router uses the
// standard library net/http ServeMux to avoid external router deps.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/batch"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/float"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/funding"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/store"
)

// ReadinessCheck is a named dependency probe used by /readyz.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// Deps bundles the handler dependencies.
type Deps struct {
	Batches   store.BatchStore
	Members   store.MembershipStore
	Orders    store.AggregateOrderStore
	Scheduler *batch.Scheduler
	Float     *float.Tracker
	Funding   *funding.Manager
	Readiness []ReadinessCheck
}

// NewRouter returns an http.Handler wired with all REST routes.
func NewRouter(d *Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/readyz", readyz(d))
	if d.Batches != nil {
		mux.HandleFunc("/v1/batches", d.handleBatches)
		mux.HandleFunc("/v1/batches/", d.handleBatchByID)
	}
	if d.Float != nil {
		mux.HandleFunc("/v1/float", d.handleFloatList)
		mux.HandleFunc("/v1/float/", d.handleFloat)
	}
	if d.Funding != nil {
		mux.HandleFunc("/v1/funding-requests", d.handleFunding)
		mux.HandleFunc("/v1/rebalancing-jobs", d.handleRebalancing)
	}
	if d.Orders != nil {
		mux.HandleFunc("/v1/aggregate-orders", d.handleAggregateOrders)
	}
	return mux
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func readyz(d *Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		checks := d.Readiness
		results := make(map[string]string, len(checks))
		failed := 0
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		for _, c := range checks {
			if c.Check == nil {
				results[c.Name] = "ok"
				continue
			}
			if err := c.Check(ctx); err != nil {
				results[c.Name] = "down"
				failed++
			} else {
				results[c.Name] = "ok"
			}
		}
		status := "ready"
		code := http.StatusOK
		if failed == len(checks) && len(checks) > 0 {
			status = "not ready"
			code = http.StatusServiceUnavailable
		} else if failed > 0 {
			status = "degraded"
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": results})
	}
}

// GET /v1/batches?from=&to=
func (d *Deps) handleBatches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	from, _ := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, _ := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	bs, err := d.Batches.ListBatches(r.Context(), from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batches": bs})
}

// GET/POST /v1/batches/:id  and POST /v1/batches/:id/close
func (d *Deps) handleBatchByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/batches/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "missing batch id")
		return
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid batch id")
		return
	}
	if len(parts) >= 2 {
		switch parts[1] {
		case "close":
			d.handleBatchClose(w, r, id)
			return
		case "memberships":
			d.handleBatchMemberships(w, r, id)
			return
		}
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	b, err := d.Batches.GetBatch(r.Context(), id)
	if err != nil {
		if store.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "batch not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var order *store.AggregateOrder
	if d.Orders != nil {
		order, _ = d.Orders.GetOrderByBatch(r.Context(), id)
	}
	var members []*store.Membership
	if d.Members != nil {
		members, _ = d.Members.ListMemberships(r.Context(), id)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch":       b,
		"memberships": members,
		"order":       order,
	})
}

// POST /v1/batches/:id/close
func (d *Deps) handleBatchClose(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if d.Scheduler == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler not configured")
		return
	}
	b, err := d.Scheduler.CloseBatch(r.Context(), id)
	if err != nil {
		if err == batch.ErrNotOpen {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if store.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "batch not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// GET /v1/batches/:id/memberships
func (d *Deps) handleBatchMemberships(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if d.Members == nil {
		writeError(w, http.StatusServiceUnavailable, "membership store not configured")
		return
	}
	members, err := d.Members.ListMemberships(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"memberships": members})
}

// GET /v1/float (list all currencies)
func (d *Deps) handleFloatList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	positions, err := d.Float.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if positions == nil {
		positions = []*store.FloatPosition{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"float_positions": positions})
}

// GET /v1/float/{fiat_currency}
func (d *Deps) handleFloat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	fiat := strings.TrimPrefix(r.URL.Path, "/v1/float/")
	if fiat == "" {
		writeError(w, http.StatusBadRequest, "missing fiat currency")
		return
	}
	pos, err := d.Float.Get(r.Context(), fiat)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pos)
}

// GET /v1/aggregate-orders?status=
func (d *Deps) handleAggregateOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	status := r.URL.Query().Get("status")
	orders, err := d.Orders.ListOrders(r.Context(), status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if orders == nil {
		orders = []*store.AggregateOrder{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"aggregate_orders": orders})
}

// POST /v1/funding-requests, GET /v1/funding-requests?status=
// BREAKING CHANGE: amount is now accepted as a decimal string in JSON.
func (d *Deps) handleFunding(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		status := r.URL.Query().Get("status")
		out, err := d.Funding.ListFunding(r.Context(), status)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"funding_requests": out})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		WalletID    string          `json:"wallet_id"`
		Asset       string          `json:"asset"`
		Amount      decimal.Decimal `json:"amount"`
		SourceVenue string          `json:"source_venue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed json")
		return
	}
	fr, err := d.Funding.CreateFundingRequest(r.Context(), req.WalletID, req.Asset, req.Amount, req.SourceVenue)
	if err != nil {
		switch err.Error() {
		case funding.ErrInvalidAmount.Error():
			writeError(w, http.StatusBadRequest, err.Error())
		case funding.ErrPolicyViolation.Error():
			writeError(w, http.StatusForbidden, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, fr)
}

// GET /v1/rebalancing-jobs?status=
func (d *Deps) handleRebalancing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	status := r.URL.Query().Get("status")
	out, err := d.Funding.ListJobs(r.Context(), status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rebalancing_jobs": out})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
