package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	gocache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
)

type testItem struct {
	ID   uint64 `json:"id,string"`
	Name string `json:"name"`
}

func newTestCache(t *testing.T) (*miniredis.Miniredis, *gocache.Cache, *MultiLevel[testItem]) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.New(30*time.Second, time.Minute)
	c := NewMultiLevel[testItem](l1, rdb, Options{
		L1TTL:       30 * time.Second,
		L2TTL:       1800 * time.Second,
		NullL2TTL:   60 * time.Second,
		JitterRatio: 0.1,
	})
	return mr, l1, c
}

func TestMultiLevel_HitPathAndDelete(t *testing.T) {
	mr, l1, c := newTestCache(t)
	ctx, key := context.Background(), "user:profile:1"
	v1, v2 := testItem{ID: 1, Name: "v1"}, testItem{ID: 1, Name: "v2"}

	var calls atomic.Int32
	loader := func(context.Context) (testItem, error) {
		calls.Add(1)
		return v1, nil
	}

	// 冷启动回源 L3，回填 L2 + L1。
	if got, err := c.Get(ctx, key, loader); err != nil || got != v1 || calls.Load() != 1 {
		t.Fatalf("cold Get failed: got=%+v err=%v calls=%d", got, err, calls.Load())
	}
	rawL2, err := mr.Get(key)
	if err != nil {
		t.Fatalf("expected L2 populated: %v", err)
	}
	if ttl := mr.TTL(key); ttl < 1618*time.Second || ttl > 1982*time.Second {
		t.Fatalf("L2 TTL %v out of 1800s±10%%", ttl)
	}

	// 删 L2 验证命中 L1；恢复 L2 并清 L1 验证命中 L2 回填 L1。
	mr.Del(key)
	if got, err := c.Get(ctx, key, loader); err != nil || got != v1 || calls.Load() != 1 {
		t.Fatalf("L1 hit failed: got=%+v err=%v calls=%d", got, err, calls.Load())
	}
	_ = mr.Set(key, rawL2)
	l1.Flush()
	if got, err := c.Get(ctx, key, loader); err != nil || got != v1 || calls.Load() != 1 {
		t.Fatalf("L2 hit failed: got=%+v err=%v calls=%d", got, err, calls.Load())
	}

	// Delete 先删 L1 再删 L2，后续重新回源。
	if err := c.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	got, err := c.Get(ctx, key, func(context.Context) (testItem, error) { return v2, nil })
	if err != nil || got != v2 {
		t.Fatalf("reload after Delete failed: got=%+v err=%v", got, err)
	}
}

func TestMultiLevel_NullCache(t *testing.T) {
	mr, l1, c := newTestCache(t)
	ctx, key := context.Background(), "user:profile:404"

	var calls atomic.Int32
	loader := func(context.Context) (testItem, error) {
		calls.Add(1)
		return testItem{}, ErrNotFound
	}

	if _, err := c.Get(ctx, key, loader); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if raw, _ := mr.Get(key); raw != NilMarker {
		t.Fatalf("expected nil marker %q in L2, got %q", NilMarker, raw)
	}
	if ttl := mr.TTL(key); ttl < 53*time.Second || ttl > 67*time.Second {
		t.Fatalf("null L2 TTL %v out of 60s±10%%", ttl)
	}

	// L1 命中空值；清 L1 后 L2 命中空值，均不触发回源。
	if _, err := c.Get(ctx, key, loader); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected L1 ErrNotFound, got %v", err)
	}
	l1.Flush()
	if _, err := c.Get(ctx, key, loader); !errors.Is(err, ErrNotFound) || calls.Load() != 1 {
		t.Fatalf("expected L2 ErrNotFound without reload: err=%v calls=%d", err, calls.Load())
	}
}

func TestMultiLevel_Singleflight(t *testing.T) {
	_, _, c := newTestCache(t)
	ctx, key := context.Background(), "user:profile:hot"
	want := testItem{ID: 42, Name: "hot"}

	var calls atomic.Int32
	loader := func(context.Context) (testItem, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return want, nil
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := c.Get(ctx, key, loader); err != nil || got != want {
				t.Errorf("concurrent Get: got=%+v err=%v", got, err)
			}
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected singleflight calls=1, got %d", calls.Load())
	}
}

func TestMultiLevel_DegradeAndLoaderError(t *testing.T) {
	mr, _, c := newTestCache(t)
	ctx := context.Background()

	// 回源普通错误不写入缓存。
	dbErr := errors.New("db timeout")
	if _, err := c.Get(ctx, "user:profile:err", func(context.Context) (testItem, error) {
		return testItem{}, dbErr
	}); !errors.Is(err, dbErr) || mr.Exists("user:profile:err") {
		t.Fatalf("loader error should return directly without caching: err=%v", err)
	}

	// Redis 宕机时透明降级回源并正常返回。
	mr.Close()
	want := testItem{ID: 7, Name: "degraded"}
	got, err := c.Get(ctx, "user:profile:degrade", func(context.Context) (testItem, error) {
		return want, nil
	})
	if err != nil || got != want {
		t.Fatalf("expected degrade success: got=%+v err=%v", got, err)
	}
}
