package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
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

	l1 := gocache.NewCache(2 * 1024 * 1024)
	c := NewMultiLevel[testItem](l1, rdb, 30*time.Second, 1800*time.Second)
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
	l1.Clear()
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
	l1.Clear()
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

func TestMultiLevel_WithL2TTLFunc(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	c := NewMultiLevel(
		l1, rdb, 30*time.Second, 10*time.Minute,
		WithL2TTLFunc(func(v testItem) time.Duration {
			switch v.Name {
			case "hot":
				return time.Hour
			case "fallback":
				return 0
			default:
				return 10 * time.Minute
			}
		}),
	)

	ctx := context.Background()
	if _, err := c.Get(ctx, "wp:hot", func(context.Context) (testItem, error) {
		return testItem{ID: 1, Name: "hot"}, nil
	}); err != nil {
		t.Fatalf("hot Get failed: %v", err)
	}
	if ttl := mr.TTL("wp:hot"); ttl < 3238*time.Second || ttl > 3962*time.Second {
		t.Fatalf("hot L2 TTL %v out of 1h±10%%", ttl)
	}

	if _, err := c.Get(ctx, "wp:fallback", func(context.Context) (testItem, error) {
		return testItem{ID: 2, Name: "fallback"}, nil
	}); err != nil {
		t.Fatalf("fallback Get failed: %v", err)
	}
	if ttl := mr.TTL("wp:fallback"); ttl < 538*time.Second || ttl > 662*time.Second {
		t.Fatalf("fallback L2 TTL %v out of 10m±10%%", ttl)
	}
}

func TestMultiLevel_MGet(t *testing.T) {
	ctx := context.Background()

	t.Run("empty keys returns empty map without loader", func(t *testing.T) {
		_, _, c := newTestCache(t)
		called := false
		got, err := c.MGet(ctx, nil, func(context.Context, []string) (map[string]testItem, error) {
			called = true
			return nil, nil
		})
		if err != nil || len(got) != 0 || called {
			t.Fatalf("empty MGet: got=%v err=%v called=%v", got, err, called)
		}
	})

	t.Run("mixed L1 L2 L3 nil marker dedup and backfill", func(t *testing.T) {
		mr, l1, c := newTestCache(t)

		// k1 预热进 L1（同时删 L2 证明真走 L1）
		v1 := testItem{ID: 1, Name: "l1"}
		if _, err := c.Get(ctx, "k1", func(context.Context) (testItem, error) { return v1, nil }); err != nil {
			t.Fatalf("seed k1: %v", err)
		}
		mr.Del("k1")

		// k2 预热进 L2（清掉 L1 证明走 L2）
		v2 := testItem{ID: 2, Name: "l2"}
		if _, err := c.Get(ctx, "k2", func(context.Context) (testItem, error) { return v2, nil }); err != nil {
			t.Fatalf("seed k2: %v", err)
		}
		l1.Del([]byte("k2"))

		// k4 预置空值哨兵在 L2
		_ = mr.Set("k4", NilMarker)

		v3 := testItem{ID: 3, Name: "l3"}
		var loaderKeys []string
		got, err := c.MGet(ctx, []string{"k1", "k2", "k3", "k3", "k4", "k5"}, func(_ context.Context, miss []string) (map[string]testItem, error) {
			loaderKeys = append([]string(nil), miss...)
			return map[string]testItem{"k3": v3}, nil
		})
		if err != nil {
			t.Fatalf("MGet failed: %v", err)
		}
		if len(got) != 3 || got["k1"] != v1 || got["k2"] != v2 || got["k3"] != v3 {
			t.Fatalf("unexpected MGet result: %+v", got)
		}
		if len(loaderKeys) != 2 || loaderKeys[0] != "k3" || loaderKeys[1] != "k5" {
			t.Fatalf("unexpected loader miss keys: %v", loaderKeys)
		}
		if raw, _ := mr.Get("k5"); raw != NilMarker {
			t.Fatalf("expected k5 nil marker in L2, got %q", raw)
		}

		// 再次 MGet 全部应命中 L1（包括 k5 空值哨兵），不再回源。
		reloaded := false
		got2, err := c.MGet(ctx, []string{"k1", "k2", "k3", "k4", "k5"}, func(context.Context, []string) (map[string]testItem, error) {
			reloaded = true
			return nil, nil
		})
		if err != nil || reloaded || len(got2) != 3 {
			t.Fatalf("second MGet expected full L1 hit: got=%+v err=%v reloaded=%v", got2, err, reloaded)
		}
	})

	t.Run("loader error returns error without caching", func(t *testing.T) {
		mr, _, c := newTestCache(t)
		dbErr := errors.New("db failed")
		if _, err := c.MGet(ctx, []string{"ke"}, func(context.Context, []string) (map[string]testItem, error) {
			return nil, dbErr
		}); !errors.Is(err, dbErr) || mr.Exists("ke") {
			t.Fatalf("expected error without caching: err=%v exists=%v", err, mr.Exists("ke"))
		}
	})

	t.Run("redis failure degrades to batch loader", func(t *testing.T) {
		mr, _, c := newTestCache(t)
		mr.Close()
		want := testItem{ID: 9, Name: "degraded"}
		got, err := c.MGet(ctx, []string{"kd"}, func(context.Context, []string) (map[string]testItem, error) {
			return map[string]testItem{"kd": want}, nil
		})
		if err != nil || got["kd"] != want {
			t.Fatalf("expected degraded MGet success: got=%+v err=%v", got, err)
		}
	})
}
