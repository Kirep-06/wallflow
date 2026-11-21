package service

import (
	"context"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/storage"
)

const testUIDBase uint64 = 900_000_000

type tinyStorage struct{ storage.Service }

func (tinyStorage) URL(k string) string { return "https://cdn/" + k }

type tinyUserStore struct {
	UserStore
	mu     sync.Mutex
	nicks  map[uint64]string
	calls  atomic.Int32
	onRead func([]uint64)
	onSave func(uint64)
}

func (s *tinyUserStore) GetPublicByID(ctx context.Context, id uint64) (*model.UserPublic, error) {
	list, err := s.GetPublicByIDs(ctx, []uint64{id})
	if err != nil || len(list) == 0 {
		return nil, repository.ErrNotFound
	}
	return list[0], nil
}

func (s *tinyUserStore) GetPublicByIDs(_ context.Context, ids []uint64) ([]*model.UserPublic, error) {
	s.calls.Add(1)
	s.mu.Lock()
	out := make([]*model.UserPublic, 0, len(ids))
	for _, id := range ids {
		if nick, ok := s.nicks[id]; ok {
			out = append(out, &model.UserPublic{ID: id, Username: "u" + strconv.FormatUint(id, 10), Nickname: nick})
		}
	}
	hook := s.onRead
	s.mu.Unlock()
	if hook != nil {
		hook(ids)
	}
	return out, nil
}

func (s *tinyUserStore) UpdateFields(_ context.Context, id uint64, fields map[string]any) error {
	s.mu.Lock()
	if v, ok := fields["nickname"].(string); ok {
		s.nicks[id] = v
	}
	hook := s.onSave
	s.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	return nil
}

type tinyFollowStore struct {
	FollowStore
	mu       sync.Mutex
	edges    map[[2]uint64]bool
	stats    map[uint64]model.FollowStats
	onCommit func(changed bool)
}

func (s *tinyFollowStore) mutate(a, b uint64, add bool) (bool, error) {
	s.mu.Lock()
	k := [2]uint64{a, b}
	changed := s.edges[k] != add
	if changed {
		fs, ts := s.stats[a], s.stats[b]
		if add {
			s.edges[k] = true
			fs.FollowingCount++
			ts.FollowerCount++
		} else {
			delete(s.edges, k)
			fs.FollowingCount--
			ts.FollowerCount--
		}
		s.stats[a], s.stats[b] = fs, ts
	}
	hook := s.onCommit
	s.mu.Unlock()
	if hook != nil {
		hook(changed)
	}
	return changed, nil
}

func (s *tinyFollowStore) Follow(_ context.Context, a, b uint64, _ int, _ time.Time) (bool, error) {
	return s.mutate(a, b, true)
}
func (s *tinyFollowStore) Unfollow(_ context.Context, a, b uint64, _ time.Time) (bool, error) {
	return s.mutate(a, b, false)
}
func (s *tinyFollowStore) Exists(_ context.Context, a, b uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.edges[[2]uint64{a, b}], nil
}
func (s *tinyFollowStore) GetStatsByUserIDs(_ context.Context, ids []uint64) (map[uint64]model.FollowStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]model.FollowStats, len(ids))
	for _, id := range ids {
		if st, ok := s.stats[id]; ok {
			out[id] = st
		}
	}
	return out, nil
}

func newTestEnv(t *testing.T, uids ...uint64) (*redis.Client, *gocache.Cache, *tinyUserStore, *tinyFollowStore, *UserService, *FollowService) {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	var keys []string
	nicks := make(map[uint64]string, len(uids))
	for _, id := range uids {
		s := strconv.FormatUint(id, 10)
		keys = append(keys, "user:profile:"+s, "follow:stats:"+s)
		nicks[id] = "Nick" + s
	}
	_ = rdb.Del(context.Background(), keys...).Err()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), keys...).Err(); _ = rdb.Close() })

	local := gocache.NewCache(1 << 20)
	us := &tinyUserStore{nicks: nicks}
	fs := &tinyFollowStore{edges: map[[2]uint64]bool{}, stats: map[uint64]model.FollowStats{}}
	pc := cache.NewMultiLevel(local, rdb, cache.Options{Prefix: "user:profile:", LocalTTL: 30 * time.Second, NullTTL: 60 * time.Second})
	sc := cache.NewMultiLevel(local, rdb, cache.Options{Prefix: "follow:stats:", LocalTTL: 15 * time.Second, NullTTL: 60 * time.Second})
	uSvc := NewUserService(us, nil, nil, tinyStorage{}, pc, config.UserConfig{DefaultAvatarKey: "d.png", ProfileLocalTTL: 30, ProfileRedisTTL: 1800})
	fSvc := NewFollowService(fs, uSvc, sc, config.FollowConfig{MaxFollowing: 2000, StatsLocalTTL: 15, StatsRedisTTL: 600})
	uSvc.SetFollowReader(fSvc)
	return rdb, local, us, fs, uSvc, fSvc
}

// TestM0301BatchProfileLockConsistency 回归 M03-01：批量读与 PATCH 并发不回填旧值，同槽/交错加锁无死锁且单次批量回源。
func TestM0301BatchProfileLockConsistency(t *testing.T) {
	ctx := context.Background()
	uA, uB, uSame := testUIDBase+1, testUIDBase+2, testUIDBase+1+userLockStripes
	rdb, local, us, _, uSvc, _ := newTestEnv(t, uA, uB, uSame)
	us.nicks[uA] = "OldNick"

	// 单次 MySQL 批量回源（含同锁槽 ID 不自锁）
	if got, err := uSvc.GetPublicProfiles(ctx, []uint64{uA, uSame, uB}); err != nil || len(got) != 3 || us.calls.Load() != 1 {
		t.Fatalf("batch load failed: len=%d calls=%d err=%v", len(got), us.calls.Load(), err)
	}
	uSvc.InvalidateProfile(ctx, uA)
	uSvc.InvalidateProfile(ctx, uB)

	// 同步点：批量读拿到旧值暂停 -> PATCH 提交 MySQL -> 放行批量读
	paused, release, saved := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	us.onRead = func(ids []uint64) {
		if slices.Contains(ids, uA) {
			once.Do(func() { close(paused); <-release })
		}
	}
	us.onSave = func(id uint64) {
		if id == uA {
			close(saved)
		}
	}
	errCh := make(chan error, 2)
	go func() { _, err := uSvc.GetPublicProfiles(ctx, []uint64{uA, uB}); errCh <- err }()
	<-paused
	go func() {
		_, err := uSvc.UpdateProfile(ctx, uA, &PatchProfileReq{Nickname: OptionalString{Set: true, Value: "NewNick"}})
		errCh <- err
	}()
	<-saved
	close(release)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	us.onRead, us.onSave = nil, nil

	keyA := "user:profile:" + strconv.FormatUint(uA, 10)
	if v, err := local.Get([]byte(keyA)); err == nil && strings.Contains(string(v), "OldNick") {
		t.Fatalf("stale L1: %s", v)
	}
	if v, err := rdb.Get(ctx, keyA).Result(); err == nil && strings.Contains(v, "OldNick") {
		t.Fatalf("stale Redis: %s", v)
	}
	if p, err := uSvc.GetProfile(ctx, uA, uB); err != nil || p.Nickname != "NewNick" {
		t.Fatalf("profile mismatch: %+v err=%v", p, err)
	}

	// 交错顺序并发批量读与失效无死锁
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids := []uint64{uA, uB, uSame}
			if i%2 == 1 {
				slices.Reverse(ids)
			}
			for range 10 {
				if _, err := uSvc.GetPublicProfiles(ctx, ids); err != nil {
					t.Errorf("batch read: %v", err)
				}
				uSvc.InvalidateProfile(ctx, ids[0])
			}
		}(i)
	}
	wg.Wait()
}

// TestM0302CanceledRequestInvalidatesStats 回归 M03-02：提交后取消原请求仍删净双方 L2 缓存，Redis 挂起时清理受短超时约束。
func TestM0302CanceledRequestInvalidatesStats(t *testing.T) {
	ctx := context.Background()
	uA, uB := testUIDBase+11, testUIDBase+12
	rdb, _, us, fs, uSvc, fSvc := newTestEnv(t, uA, uB)
	keyA, keyB := "follow:stats:"+strconv.FormatUint(uA, 10), "follow:stats:"+strconv.FormatUint(uB, 10)

	// 预热空值标记后验证关注，再在普通计数已预热状态下验证取关
	_, _ = uSvc.GetProfile(ctx, uA, uA)
	_, _ = uSvc.GetProfile(ctx, uB, uA)
	checkCanceled := func(op func(context.Context, uint64, uint64) error, want int64) {
		t.Helper()
		reqCtx, cancel := context.WithCancel(ctx)
		fs.onCommit = func(changed bool) {
			if changed {
				cancel()
			}
		}
		if err := op(reqCtx, uA, uB); err != nil {
			t.Fatal(err)
		}
		fs.onCommit = nil
		if n, err := rdb.Exists(ctx, keyA, keyB).Result(); err != nil || n != 0 {
			t.Fatalf("L2 keys not deleted: exists=%d err=%v", n, err)
		}
		if pA, _ := uSvc.GetProfile(ctx, uA, uA); pA.FollowingCount != want {
			t.Fatalf("uA following=%d want %d", pA.FollowingCount, want)
		}
		if pB, _ := uSvc.GetProfile(ctx, uB, uA); pB.FollowerCount != want || pB.IsFollowing != (want > 0) {
			t.Fatalf("uB profile=%+v want %d", pB, want)
		}
		if err := op(ctx, uA, uB); err != nil || fs.stats[uA].FollowingCount != want {
			t.Fatalf("idempotent op failed: err=%v stats=%+v", err, fs.stats[uA])
		}
	}
	checkCanceled(fSvc.Follow, 1)
	checkCanceled(fSvc.Unfollow, 0)

	// Redis 黑洞挂起：验证短超时有界、双方 L1 已清理且已提交操作仍返回成功
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); _ = ln.Close() })
	go func() {
		for {
			if c, err := ln.Accept(); err == nil {
				go func(c net.Conn) { <-stop; _ = c.Close() }(c)
			} else {
				return
			}
		}
	}()
	hangRDB := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), ReadTimeout: 5 * time.Second, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = hangRDB.Close() })
	hangLocal := gocache.NewCache(1 << 20)
	_ = hangLocal.Set([]byte(keyA), []byte(`{"following_count":0,"follower_count":0}`), 30)
	_ = hangLocal.Set([]byte(keyB), []byte(`{"following_count":0,"follower_count":0}`), 30)
	_ = hangLocal.Set([]byte("user:profile:"+strconv.FormatUint(uB, 10)), []byte(`{"id":900000012,"username":"uB","nickname":"B"}`), 30)
	sc := cache.NewMultiLevel(hangLocal, hangRDB, cache.Options{Prefix: "follow:stats:", LocalTTL: 15 * time.Second, NullTTL: 60 * time.Second})
	pc := cache.NewMultiLevel(hangLocal, hangRDB, cache.Options{Prefix: "user:profile:", LocalTTL: 30 * time.Second, NullTTL: 60 * time.Second})
	hangUSvc := NewUserService(us, nil, nil, tinyStorage{}, pc, config.UserConfig{DefaultAvatarKey: "d.png", ProfileLocalTTL: 30, ProfileRedisTTL: 1800})
	hangFSvc := NewFollowService(fs, hangUSvc, sc, config.FollowConfig{MaxFollowing: 2000, StatsLocalTTL: 15, StatsRedisTTL: 600})
	hangFSvc.invalidateTimeout = 100 * time.Millisecond

	start := time.Now()
	if err := hangFSvc.Follow(ctx, uA, uB); err != nil {
		t.Fatalf("follow on hung redis should succeed: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 800*time.Millisecond {
		t.Fatalf("invalidation exceeded bounded timeout: %v", elapsed)
	}
	if _, err := hangLocal.Get([]byte(keyA)); err == nil {
		t.Fatal("uA L1 not cleared")
	}
	if _, err := hangLocal.Get([]byte(keyB)); err == nil {
		t.Fatal("uB L1 not cleared")
	}
}
