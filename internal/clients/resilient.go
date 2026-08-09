package clients

import (
	"context"
	"time"
)

// ResilientLiquidity wraps a LiquidityRouting with retry + circuit
// breaker.
type ResilientLiquidity struct {
	inner LiquidityRouting
	opts  RetryOptions
	cb    *CircuitBreaker
}

// NewResilientLiquidity returns a liquidity-routing client with retry
// and circuit-breaker semantics.
func NewResilientLiquidity(inner LiquidityRouting, opts RetryOptions, cb *CircuitBreaker) *ResilientLiquidity {
	return &ResilientLiquidity{inner: inner, opts: opts, cb: cb}
}

// SubmitAggregate delegates with retry + circuit breaker.
func (r *ResilientLiquidity) SubmitAggregate(ctx context.Context, req AggregateOrderRequest, key string) (*FillResult, error) {
	var out *FillResult
	err := Do(ctx, r.opts, r.cb, func(ctx context.Context) error {
		res, err := r.inner.SubmitAggregate(ctx, req, key)
		if err != nil {
			return err
		}
		out = res
		return nil
	})
	return out, err
}

// ResilientFX wraps an FXHedging with retry + circuit breaker.
type ResilientFX struct {
	inner FXHedging
	opts  RetryOptions
	cb    *CircuitBreaker
}

// NewResilientFX returns an fx-hedging client with retry and
// circuit-breaker semantics.
func NewResilientFX(inner FXHedging, opts RetryOptions, cb *CircuitBreaker) *ResilientFX {
	return &ResilientFX{inner: inner, opts: opts, cb: cb}
}

// SubmitExposure delegates with retry + circuit breaker.
func (r *ResilientFX) SubmitExposure(ctx context.Context, req HedgeRequest, key string) (*HedgeResult, error) {
	var out *HedgeResult
	err := Do(ctx, r.opts, r.cb, func(ctx context.Context) error {
		res, err := r.inner.SubmitExposure(ctx, req, key)
		if err != nil {
			return err
		}
		out = res
		return nil
	})
	return out, err
}

// DefaultCircuitBreaker returns a circuit breaker that opens after 5
// consecutive failures and resets after 10 seconds.
func DefaultCircuitBreaker() *CircuitBreaker {
	return NewCircuitBreaker(5, 10*time.Second)
}