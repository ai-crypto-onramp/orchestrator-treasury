// Package idempotency provides dedup primitives for write-side replays.
// It is backed by Redis in production (go-redis) and falls back to an
// in-memory implementation when REDIS_URL is empty or unreachable, which
// keeps unit tests dependency-free.
package idempotency

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store is the idempotency-key store abstraction.
type Store interface {
	// CheckAndMark returns true if the key was claimed (first time), false
	// if it was already present. TTL is the duration the key is retained.
	CheckAndMark(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Delete removes the key (useful for tests).
	Delete(ctx context.Context, key string) error
}

// ErrUnavailable is returned when no store is configured.
var ErrUnavailable = errors.New("idempotency: store unavailable")

// MemStore is an in-memory idempotency store for tests.
type MemStore struct {
	mu sync.Mutex
	seen map[string]time.Time
}

// NewMem returns an in-memory idempotency store.
func NewMem() *MemStore { return &MemStore{seen: map[string]time.Time{}} }

// CheckAndMark claims the key if not present, with the given TTL.
func (s *MemStore) CheckAndMark(_ context.Context, key string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if exp, ok := s.seen[key]; ok && exp.After(now) {
		return false, nil
	}
	s.seen[key] = now.Add(ttl)
	return true, nil
}

// Delete removes the key.
func (s *MemStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seen, key)
	return nil
}

// RedisStore is a Redis-backed idempotency store using SET NX.
type RedisStore struct{ client *redis.Client }

// NewRedis returns a Redis-backed store.
func NewRedis(client *redis.Client) *RedisStore { return &RedisStore{client: client} }

// CheckAndMark atomically claims the key via SET NX with TTL.
func (s *RedisStore) CheckAndMark(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := s.client.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Delete removes the key.
func (s *RedisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

// Open returns a Redis-backed store if url is non-empty, otherwise a
// MemStore. When the URL is set but unreachable, Open falls back to Mem
// so the service still boots in dev / test without Redis.
func Open(ctx context.Context, url string) (Store, error) {
	if strings.TrimSpace(url) == "" {
		return NewMem(), nil
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return NewMem(), nil
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return NewMem(), nil
	}
	return NewRedis(client), nil
}

// CadenceLock is a Redis-backed leader/cadence lock that prevents double
// close of a batch under concurrent schedulers. It uses SET NX with TTL.
type CadenceLock struct {
	store Store
	ttl   time.Duration
}

// NewCadenceLock returns a cadence lock backed by store.
func NewCadenceLock(store Store, ttl time.Duration) *CadenceLock {
	return &CadenceLock{store: store, ttl: ttl}
}

// Acquire returns true if the lock for the given asset pair was acquired.
func (l *CadenceLock) Acquire(ctx context.Context, assetPair string) (bool, error) {
	return l.store.CheckAndMark(ctx, "cadence:"+assetPair, l.ttl)
}