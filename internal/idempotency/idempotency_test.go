package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMemStore_CheckAndMark(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	ok, err := s.CheckAndMark(ctx, "k1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first: %v ok=%v", err, ok)
	}
	ok, _ = s.CheckAndMark(ctx, "k1", time.Minute)
	if ok {
		t.Fatal("expected dup false")
	}
	// Different key claims.
	ok, _ = s.CheckAndMark(ctx, "k2", time.Minute)
	if !ok {
		t.Fatal("expected k2 ok")
	}
}

func TestMemStore_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	ok, _ := s.CheckAndMark(ctx, "exp", 50*time.Millisecond)
	if !ok {
		t.Fatal("expected first ok")
	}
	time.Sleep(60 * time.Millisecond)
	ok, _ = s.CheckAndMark(ctx, "exp", 50*time.Millisecond)
	if !ok {
		t.Fatal("expected re-claim after expiry")
	}
}

func TestMemStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	_, _ = s.CheckAndMark(ctx, "d", time.Minute)
	if err := s.Delete(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	ok, _ := s.CheckAndMark(ctx, "d", time.Minute)
	if !ok {
		t.Fatal("expected re-claim after delete")
	}
}

func TestCadenceLock_Acquire(t *testing.T) {
	ctx := context.Background()
	lock := NewCadenceLock(NewMem(), time.Minute)
	ok, err := lock.Acquire(ctx, "BTC/USD")
	if err != nil || !ok {
		t.Fatalf("acquire: %v ok=%v", err, ok)
	}
	ok, _ = lock.Acquire(ctx, "BTC/USD")
	if ok {
		t.Fatal("expected second acquire to fail")
	}
	ok, _ = lock.Acquire(ctx, "ETH/USD")
	if !ok {
		t.Fatal("expected different pair to acquire")
	}
}

func TestOpen_EmptyURLReturnsMem(t *testing.T) {
	s, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*MemStore); !ok {
		t.Fatal("expected MemStore for empty url")
	}
}

func TestOpen_BadURLReturnsMem(t *testing.T) {
	s, err := Open(context.Background(), "redis://invalidurl:::bad")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*MemStore); !ok {
		t.Fatal("expected MemStore for unparseable url")
	}
}

func TestOpen_UnreachableReturnsMem(t *testing.T) {
	// Use a URL pointing at a closed port to exercise the Ping fallback.
	s, err := Open(context.Background(), "redis://127.0.0.1:1/0")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*MemStore); !ok {
		t.Fatal("expected MemStore fallback when redis unreachable")
	}
}

func TestOpen_RedisReachableReturnsRedisStore(t *testing.T) {
	mr := miniredis.RunT(t)
	s, err := Open(context.Background(), "redis://"+mr.Addr()+"/0")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*RedisStore); !ok {
		t.Fatalf("expected RedisStore, got %T", s)
	}
}

func newRedisStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedis(client), mr
}

func TestRedisStore_CheckAndMark(t *testing.T) {
	ctx := context.Background()
	s, _ := newRedisStore(t)

	ok, err := s.CheckAndMark(ctx, "rk1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first: err=%v ok=%v", err, ok)
	}
	ok, err = s.CheckAndMark(ctx, "rk1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected duplicate to return false")
	}
	// Different key claims.
	ok, err = s.CheckAndMark(ctx, "rk2", time.Minute)
	if err != nil || !ok {
		t.Fatalf("rk2: err=%v ok=%v", err, ok)
	}
}

func TestRedisStore_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	s, mr := newRedisStore(t)

	ok, _ := s.CheckAndMark(ctx, "exp", time.Second)
	if !ok {
		t.Fatal("expected first ok")
	}
	mr.FastForward(2 * time.Second)
	ok, _ = s.CheckAndMark(ctx, "exp", time.Second)
	if !ok {
		t.Fatal("expected re-claim after expiry")
	}
}

func TestRedisStore_Delete(t *testing.T) {
	ctx := context.Background()
	s, _ := newRedisStore(t)

	_, _ = s.CheckAndMark(ctx, "del", time.Minute)
	if err := s.Delete(ctx, "del"); err != nil {
		t.Fatal(err)
	}
	ok, _ := s.CheckAndMark(ctx, "del", time.Minute)
	if !ok {
		t.Fatal("expected re-claim after delete")
	}
}

func TestRedisStore_CadenceLock(t *testing.T) {
	ctx := context.Background()
	s, _ := newRedisStore(t)
	lock := NewCadenceLock(s, time.Minute)
	ok, err := lock.Acquire(ctx, "BTC/USD")
	if err != nil || !ok {
		t.Fatalf("acquire: %v ok=%v", err, ok)
	}
	ok, _ = lock.Acquire(ctx, "BTC/USD")
	if ok {
		t.Fatal("expected second acquire to fail")
	}
}

func TestMemStore_ConcurrentCheckAndMark(t *testing.T) {
	ctx := context.Background()
	s := NewMem()
	const n = 50
	wins := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func() { ok, _ := s.CheckAndMark(ctx, "race", time.Minute); wins <- ok }()
	}
	got := 0
	for i := 0; i < n; i++ {
		if <-wins {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", got)
	}
}

func TestRedisStore_ErrorOnClosedClient(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	_ = client.Close()
	s := NewRedis(client)
	_, err := s.CheckAndMark(context.Background(), "k", time.Minute)
	if err == nil {
		t.Fatal("expected error from closed client")
	}
	if err := s.Delete(context.Background(), "k"); err == nil {
		t.Fatal("expected error from closed client on Delete")
	}
}