package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fixture struct {
	mr    *miniredis.Miniredis
	rdb   *redis.Client
	local *gocache.Cache
	ml    *MultiLevel
	loads atomic.Int32
	db    map[uint64]string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	local := gocache.NewCache(1024 * 1024)
	f := &fixture{mr: mr, rdb: rdb, local: local, db: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	f.ml = NewMultiLevel(local, rdb, Options{Prefix: "t:", LocalTTL: 30 * time.Second, NullTTL: 60 * time.Second})
	return f
}

func (f *fixture) loader(ctx context.Context, ids []uint64) (map[uint64][]byte, error) {
	f.loads.Add(1)
	out := map[uint64][]byte{}
	for _, id := range ids {
		if v, ok := f.db[id]; ok {
			out[id] = []byte(v)
		}
	}
	return out, nil
}

func ttl(uint64) time.Duration { return time.Hour }

func TestGetManyLoadsAndBackfills(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	got, err := f.ml.GetMany(ctx, []uint64{1, 2, 99}, f.loader, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[1]) != "a" || string(got[2]) != "b" {
		t.Fatalf("unexpected result %v", got)
	}
	if f.loads.Load() != 1 {
		t.Fatalf("expected 1 load, got %d", f.loads.Load())
	}
	// L2 已回填，含空值标记
	if v, _ := f.mr.Get("t:1"); v != "a" {
		t.Fatalf("l2 not backfilled: %q", v)
	}
	if v, _ := f.mr.Get("t:99"); v != string(NullMarker) {
		t.Fatalf("null marker not written: %q", v)
	}
	l2TTL := f.mr.TTL("t:1")
	if l2TTL < 54*time.Minute || l2TTL > 66*time.Minute {
		t.Fatalf("ttl jitter out of range: %v", l2TTL)
	}

	// 第二次全部命中 L1，不回源
	got, _ = f.ml.GetMany(ctx, []uint64{1, 2, 99}, f.loader, ttl)
	if len(got) != 2 || f.loads.Load() != 1 {
		t.Fatalf("expected l1 hit, loads=%d got=%v", f.loads.Load(), got)
	}
}

func TestGetManyL2HitBackfillsL1(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_ = f.mr.Set("t:3", "from-redis")

	got, err := f.ml.GetMany(ctx, []uint64{3}, f.loader, ttl)
	if err != nil || string(got[3]) != "from-redis" || f.loads.Load() != 0 {
		t.Fatalf("expected l2 hit: got=%v err=%v loads=%d", got, err, f.loads.Load())
	}
	if v, err := f.local.Get([]byte("t:3")); err != nil || string(v) != "from-redis" {
		t.Fatalf("l1 not backfilled: %q %v", v, err)
	}
}

func TestGetManySingleflight(t *testing.T) {
	f := newFixture(t)
	release := make(chan struct{})
	slow := func(ctx context.Context, ids []uint64) (map[uint64][]byte, error) {
		<-release
		return f.loader(ctx, ids)
	}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := f.ml.GetMany(context.Background(), []uint64{2, 1}, slow, ttl)
			if err != nil || len(got) != 2 {
				t.Errorf("got=%v err=%v", got, err)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond) // 让所有协程进入 singleflight
	close(release)
	wg.Wait()
	if n := f.loads.Load(); n != 1 {
		t.Fatalf("expected 1 load with singleflight, got %d", n)
	}
}

func TestGetManyRedisDownFallsBack(t *testing.T) {
	f := newFixture(t)
	f.mr.Close()
	got, err := f.ml.GetMany(context.Background(), []uint64{1}, f.loader, ttl)
	if err != nil || string(got[1]) != "a" {
		t.Fatalf("expected fallback to l3: got=%v err=%v", got, err)
	}
}

func TestDelete(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_, _ = f.ml.GetMany(ctx, []uint64{1}, f.loader, ttl)
	if err := f.ml.Delete(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if f.mr.Exists("t:1") {
		t.Fatal("l2 not deleted")
	}
	if _, err := f.local.Get([]byte("t:1")); err == nil {
		t.Fatal("l1 not deleted")
	}
}

func TestJitter(t *testing.T) {
	for range 1000 {
		d := Jitter(100 * time.Second)
		if d < 90*time.Second || d > 110*time.Second {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
	if Jitter(0) != 0 {
		t.Fatal("zero should stay zero")
	}
}
