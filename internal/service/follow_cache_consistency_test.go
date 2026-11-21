package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// 隔离测试使用的用户 ID 偏移，避免连接外部 TEST_REDIS_ADDR 时碰撞业务 key。
const testUIDBase uint64 = 900_000_000

type testStorageStub struct{}

func (testStorageStub) UploadToken(context.Context, storage.UploadScene, uint64) (*storage.UploadToken, error) {
	return nil, nil
}
func (testStorageStub) Stat(context.Context, string) (*storage.ObjectInfo, error) { return nil, nil }
func (testStorageStub) URL(key string) string                                     { return "https://cdn.example/" + key }
func (testStorageStub) ThumbURL(key string) string                                { return "https://cdn.example/" + key + "?thumb" }

type stubUserStore struct {
	mu               sync.Mutex
	users            map[uint64]*model.User
	getPublicIDCalls atomic.Int32
	lastBatchIDs     []uint64

	onAfterGetPublicByIDs func(ids []uint64, res []*model.UserPublic)
	onAfterUpdateFields   func(id uint64, fields map[string]any)
}

func newStubUserStore(ids ...uint64) *stubUserStore {
	s := &stubUserStore{users: make(map[uint64]*model.User, len(ids))}
	for _, id := range ids {
		s.users[id] = &model.User{
			ID:        id,
			Username:  fmt.Sprintf("u_%d", id),
			Nickname:  fmt.Sprintf("Nick_%d", id),
			Bio:       fmt.Sprintf("Bio_%d", id),
			CreatedAt: time.UnixMilli(1700000000000 + int64(id%1000)),
		}
	}
	return s
}

func (s *stubUserStore) Create(_ context.Context, u *model.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *u
	s.users[u.ID] = &cp
	return nil
}

func (s *stubUserStore) GetAuthByUsername(context.Context, string) (*model.UserAuthRecord, error) {
	return nil, repository.ErrNotFound
}

func (s *stubUserStore) GetStatusByID(context.Context, uint64) (*model.UserStatusRecord, error) {
	return nil, repository.ErrNotFound
}

func (s *stubUserStore) GetPublicByID(_ context.Context, id uint64) (*model.UserPublic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[id]
	if !ok {
		return nil, repository.ErrNotFound
	}
	return &model.UserPublic{
		ID:        u.ID,
		Username:  u.Username,
		Nickname:  u.Nickname,
		AvatarKey: u.AvatarKey,
		Bio:       u.Bio,
		CreatedAt: u.CreatedAt,
	}, nil
}

func (s *stubUserStore) GetPublicByIDs(_ context.Context, ids []uint64) ([]*model.UserPublic, error) {
	s.getPublicIDCalls.Add(1)
	s.mu.Lock()
	s.lastBatchIDs = slices.Clone(ids)
	out := make([]*model.UserPublic, 0, len(ids))
	for _, id := range ids {
		if u, ok := s.users[id]; ok {
			out = append(out, &model.UserPublic{
				ID:        u.ID,
				Username:  u.Username,
				Nickname:  u.Nickname,
				AvatarKey: u.AvatarKey,
				Bio:       u.Bio,
				CreatedAt: u.CreatedAt,
			})
		}
	}
	hook := s.onAfterGetPublicByIDs
	s.mu.Unlock()

	if hook != nil {
		hook(ids, out)
	}
	return out, nil
}

func (s *stubUserStore) UpdateFields(_ context.Context, id uint64, fields map[string]any) error {
	s.mu.Lock()
	u, ok := s.users[id]
	if ok {
		if v, ok := fields["nickname"].(string); ok {
			u.Nickname = v
		}
		if v, ok := fields["bio"].(string); ok {
			u.Bio = v
		}
	}
	hook := s.onAfterUpdateFields
	s.mu.Unlock()

	if hook != nil {
		hook(id, fields)
	}
	return nil
}

type stubFollowStore struct {
	mu     sync.Mutex
	nextID uint64
	edges  map[[2]uint64]uint64
	stats  map[uint64]model.FollowStats

	onAfterFollowCommit   func(followerID, followeeID uint64, changed bool)
	onAfterUnfollowCommit func(followerID, followeeID uint64, changed bool)
}

func newStubFollowStore() *stubFollowStore {
	return &stubFollowStore{
		edges: make(map[[2]uint64]uint64),
		stats: make(map[uint64]model.FollowStats),
	}
}

func (s *stubFollowStore) Follow(_ context.Context, followerID, followeeID uint64, maxFollowing int, _ time.Time) (bool, error) {
	s.mu.Lock()
	fs := s.stats[followerID]
	ts := s.stats[followeeID]
	s.stats[followerID] = fs
	s.stats[followeeID] = ts

	key := [2]uint64{followerID, followeeID}
	if fs.FollowingCount >= int64(maxFollowing) {
		_, exists := s.edges[key]
		s.mu.Unlock()
		if exists {
			return false, nil
		}
		return false, repository.ErrFollowLimitExceeded
	}
	changed := false
	if _, exists := s.edges[key]; !exists {
		s.nextID++
		s.edges[key] = s.nextID
		fs.FollowingCount++
		ts.FollowerCount++
		s.stats[followerID] = fs
		s.stats[followeeID] = ts
		changed = true
	}
	hook := s.onAfterFollowCommit
	s.mu.Unlock()

	if hook != nil {
		hook(followerID, followeeID, changed)
	}
	return changed, nil
}

func (s *stubFollowStore) Unfollow(_ context.Context, followerID, followeeID uint64, _ time.Time) (bool, error) {
	s.mu.Lock()
	key := [2]uint64{followerID, followeeID}
	changed := false
	if _, exists := s.edges[key]; exists {
		delete(s.edges, key)
		fs := s.stats[followerID]
		if fs.FollowingCount > 0 {
			fs.FollowingCount--
		}
		s.stats[followerID] = fs
		ts := s.stats[followeeID]
		if ts.FollowerCount > 0 {
			ts.FollowerCount--
		}
		s.stats[followeeID] = ts
		changed = true
	}
	hook := s.onAfterUnfollowCommit
	s.mu.Unlock()

	if hook != nil {
		hook(followerID, followeeID, changed)
	}
	return changed, nil
}

func (s *stubFollowStore) Exists(_ context.Context, followerID, followeeID uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.edges[[2]uint64{followerID, followeeID}]
	return ok, nil
}

func (s *stubFollowStore) BatchIsFollowing(_ context.Context, followerID uint64, followeeIDs []uint64) (map[uint64]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]bool)
	for _, id := range followeeIDs {
		if _, ok := s.edges[[2]uint64{followerID, id}]; ok {
			out[id] = true
		}
	}
	return out, nil
}

func (s *stubFollowStore) GetStatsByUserIDs(_ context.Context, userIDs []uint64) (map[uint64]model.FollowStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]model.FollowStats)
	for _, uid := range userIDs {
		if st, ok := s.stats[uid]; ok {
			out[uid] = st
		}
	}
	return out, nil
}

func (s *stubFollowStore) ListFollowingBefore(_ context.Context, followerID, beforeID uint64, limit int) ([]model.FollowEdge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var edges []model.FollowEdge
	for k, eid := range s.edges {
		if k[0] == followerID && (beforeID == 0 || eid < beforeID) {
			edges = append(edges, model.FollowEdge{ID: eid, UserID: k[1]})
		}
	}
	slices.SortFunc(edges, func(a, b model.FollowEdge) int {
		if a.ID > b.ID {
			return -1
		}
		if a.ID < b.ID {
			return 1
		}
		return 0
	})
	if len(edges) > limit {
		edges = edges[:limit]
	}
	return edges, nil
}

func (s *stubFollowStore) ListFollowersBefore(_ context.Context, followeeID, beforeID uint64, limit int) ([]model.FollowEdge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var edges []model.FollowEdge
	for k, eid := range s.edges {
		if k[1] == followeeID && (beforeID == 0 || eid < beforeID) {
			edges = append(edges, model.FollowEdge{ID: eid, UserID: k[0]})
		}
	}
	slices.SortFunc(edges, func(a, b model.FollowEdge) int {
		if a.ID > b.ID {
			return -1
		}
		if a.ID < b.ID {
			return 1
		}
		return 0
	})
	if len(edges) > limit {
		edges = edges[:limit]
	}
	return edges, nil
}

func (s *stubFollowStore) ListFollowerIDsBefore(ctx context.Context, followeeID, cursorID uint64, limit int) ([]uint64, uint64, error) {
	edges, err := s.ListFollowersBefore(ctx, followeeID, cursorID, limit)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]uint64, len(edges))
	for i, e := range edges {
		ids[i] = e.UserID
	}
	var last uint64
	if len(edges) > 0 {
		last = edges[len(edges)-1].ID
	}
	return ids, last, nil
}

func (s *stubFollowStore) ListAllFolloweeIDs(_ context.Context, followerID uint64) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []uint64
	for k := range s.edges {
		if k[0] == followerID {
			ids = append(ids, k[1])
		}
	}
	return ids, nil
}

// openTestRedis 优先使用 TEST_REDIS_ADDR 连接真实 Redis（并打印版本、隔离清理测试 key）；未设置时退回 miniredis。
func openTestRedis(t *testing.T, cleanupKeys []string) (addr string, rdb *redis.Client) {
	t.Helper()
	if envAddr := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR")); envAddr != "" {
		client := redis.NewClient(&redis.Options{Addr: envAddr, ContextTimeoutEnabled: true})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			t.Fatalf("connect TEST_REDIS_ADDR=%s: %v", envAddr, err)
		}
		if info, err := client.Info(ctx, "server").Result(); err == nil {
			for _, line := range strings.Split(info, "\n") {
				if strings.HasPrefix(line, "redis_version:") {
					t.Logf("using real Redis at %s (%s)", envAddr, strings.TrimSpace(line))
					break
				}
			}
		}
		if len(cleanupKeys) > 0 {
			_ = client.Del(ctx, cleanupKeys...).Err()
		}
		t.Cleanup(func() {
			if len(cleanupKeys) > 0 {
				c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = client.Del(c, cleanupKeys...).Err()
			}
			_ = client.Close()
		})
		return envAddr, client
	}

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	return mr.Addr(), client
}

type faultMode int32

const (
	faultPassThrough faultMode = iota
	faultDropConn
	faultHang
)

// tcpFaultProxy 在本地代理到目标 Redis（miniredis 或真实 Redis 7），可按需切换断连或黑洞挂起模式。
type tcpFaultProxy struct {
	ln       net.Listener
	upstream string
	mode     atomic.Int32
	mu       sync.Mutex
	conns    []net.Conn
	hangCh   chan struct{}
	closeOne sync.Once
}

func newTCPFaultProxy(t *testing.T, upstream string) *tcpFaultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	p := &tcpFaultProxy{
		ln:       ln,
		upstream: upstream,
		hangCh:   make(chan struct{}),
	}
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpFaultProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpFaultProxy) SetMode(m faultMode) {
	p.mode.Store(int32(m))
	if m != faultPassThrough {
		// 切断已建立的连接，促使 Redis 连接池立即走新连接进入故障模式。
		p.mu.Lock()
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
		p.mu.Unlock()
	}
}

func (p *tcpFaultProxy) Close() {
	p.closeOne.Do(func() {
		close(p.hangCh)
		_ = p.ln.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
		p.mu.Unlock()
	})
}

func (p *tcpFaultProxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, c)
	p.mu.Unlock()
}

func (p *tcpFaultProxy) acceptLoop() {
	for {
		clientConn, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.track(clientConn)
		go p.handleConn(clientConn)
	}
}

func (p *tcpFaultProxy) handleConn(clientConn net.Conn) {
	defer func() { _ = clientConn.Close() }()
	switch faultMode(p.mode.Load()) {
	case faultDropConn:
		return
	case faultHang:
		// 接受连接但不回任何响应，模拟 Redis 写/删超时挂起。
		<-p.hangCh
		return
	default:
		upConn, err := net.DialTimeout("tcp", p.upstream, 2*time.Second)
		if err != nil {
			return
		}
		p.track(upConn)
		defer func() { _ = upConn.Close() }()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(upConn, clientConn)
			_ = upConn.Close()
		}()
		go func() {
			defer wg.Done()
			_, _ = io.Copy(clientConn, upConn)
			_ = clientConn.Close()
		}()
		wg.Wait()
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testKeysForUsers(uids ...uint64) []string {
	keys := make([]string, 0, len(uids)*2)
	for _, uid := range uids {
		s := strconv.FormatUint(uid, 10)
		keys = append(keys, "user:profile:"+s, "follow:stats:"+s)
	}
	return keys
}

func newTestServices(local *gocache.Cache, rdb *redis.Client, uStore UserStore, fStore FollowStore) (*UserService, *FollowService) {
	profileCache := cache.NewMultiLevel(local, rdb, cache.Options{
		Prefix:   "user:profile:",
		LocalTTL: 30 * time.Second,
		NullTTL:  60 * time.Second,
	})
	statsCache := cache.NewMultiLevel(local, rdb, cache.Options{
		Prefix:   "follow:stats:",
		LocalTTL: 15 * time.Second,
		NullTTL:  60 * time.Second,
	})
	uCfg := config.UserConfig{
		DefaultAvatarKey: "avatars/default.png",
		ProfileLocalTTL:  30,
		ProfileRedisTTL:  1800,
	}
	fCfg := config.FollowConfig{
		MaxFollowing:  2000,
		StatsLocalTTL: 15,
		StatsRedisTTL: 600,
	}
	userSvc := NewUserService(uStore, nil, nil, testStorageStub{}, profileCache, uCfg)
	followSvc := NewFollowService(fStore, userSvc, statsCache, fCfg)
	userSvc.SetFollowReader(followSvc)
	return userSvc, followSvc
}

// TestM0301BatchProfileReadDoesNotOverwriteUpdatedProfile 复现并回归 M03-01：
// 列表批量回源拿到用户 A 旧昵称后暂停，期间 PATCH 更新提交并尝试失效缓存，放行后不得把旧昵称写回 L1/Redis。
func TestM0301BatchProfileReadDoesNotOverwriteUpdatedProfile(t *testing.T) {
	ctx := context.Background()
	viewer := testUIDBase + 101
	userA := testUIDBase + 102
	userB := testUIDBase + 103

	_, rdb := openTestRedis(t, testKeysForUsers(viewer, userA, userB))
	local := gocache.NewCache(2 << 20)
	uStore := newStubUserStore(viewer, userA, userB)
	uStore.users[userA].Nickname = "OldNick"
	fStore := newStubFollowStore()
	userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)

	if err := followSvc.Follow(ctx, viewer, userA); err != nil {
		t.Fatalf("setup follow A: %v", err)
	}
	if err := followSvc.Follow(ctx, viewer, userB); err != nil {
		t.Fatalf("setup follow B: %v", err)
	}

	// 清空 A、B 的资料两级缓存，确保列表读取必走 MySQL 批量回源。
	userSvc.InvalidateProfile(ctx, userA)
	userSvc.InvalidateProfile(ctx, userB)

	batchLoadedOld := make(chan struct{})
	releaseBatch := make(chan struct{})
	mysqlCommitted := make(chan struct{})

	var batchPaused atomic.Bool
	uStore.onAfterGetPublicByIDs = func(ids []uint64, _ []*model.UserPublic) {
		if slices.Contains(ids, userA) && len(ids) > 1 && batchPaused.CompareAndSwap(false, true) {
			close(batchLoadedOld)
			<-releaseBatch
		}
	}
	uStore.onAfterUpdateFields = func(id uint64, _ map[string]any) {
		if id == userA {
			close(mysqlCommitted)
		}
	}

	listErrCh := make(chan error, 1)
	go func() {
		_, err := followSvc.ListFollowing(ctx, viewer, viewer, "", 20)
		listErrCh <- err
	}()

	select {
	case <-batchLoadedOld:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for batch profile load")
	}

	patchErrCh := make(chan error, 1)
	go func() {
		_, err := userSvc.UpdateProfile(ctx, userA, &PatchProfileReq{
			Nickname: OptionalString{Set: true, Value: "NewNick"},
		})
		patchErrCh <- err
	}()

	// 确认 PATCH 已提交 MySQL（不等待 UpdateProfile 返回，因其 InvalidateProfile 可能正等待条纹锁）。
	select {
	case <-mysqlCommitted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for PATCH mysql commit")
	}

	// 放行批量读取，等待两边都完成。
	close(releaseBatch)

	if err := <-listErrCh; err != nil {
		t.Fatalf("list following failed: %v", err)
	}
	if err := <-patchErrCh; err != nil {
		t.Fatalf("patch profile failed: %v", err)
	}

	// 此时 L1 与 Redis 均不应残留旧昵称。
	keyA := "user:profile:" + strconv.FormatUint(userA, 10)
	if v, err := local.Get([]byte(keyA)); err == nil && strings.Contains(string(v), "OldNick") {
		t.Fatalf("stale nickname found in L1 after PATCH: %s", string(v))
	}
	if v, err := rdb.Get(ctx, keyA).Result(); err == nil && strings.Contains(v, "OldNick") {
		t.Fatalf("stale nickname found in Redis after PATCH: %s", v)
	}

	// 随后读取主页与列表，均必须返回新昵称。
	prof, err := userSvc.GetProfile(ctx, userA, viewer)
	if err != nil {
		t.Fatalf("get profile failed: %v", err)
	}
	if prof.Nickname != "NewNick" {
		t.Fatalf("profile nickname = %q, want %q", prof.Nickname, "NewNick")
	}

	page, err := followSvc.ListFollowing(ctx, viewer, viewer, "", 20)
	if err != nil {
		t.Fatalf("list following after PATCH failed: %v", err)
	}
	foundA := false
	for _, it := range page.Items {
		if it.ID == userA {
			foundA = true
			if it.Nickname != "NewNick" {
				t.Fatalf("list item nickname = %q, want %q", it.Nickname, "NewNick")
			}
		}
	}
	if !foundA {
		t.Fatalf("user %d missing from following list", userA)
	}

	// 再次核对已回填的 L1 与 Redis 中均为新昵称、无旧昵称。
	if v, err := local.Get([]byte(keyA)); err != nil || !strings.Contains(string(v), "NewNick") || strings.Contains(string(v), "OldNick") {
		t.Fatalf("unexpected L1 cache value after reload: val=%q err=%v", string(v), err)
	}
	if v, err := rdb.Get(ctx, keyA).Result(); err != nil || !strings.Contains(v, "NewNick") || strings.Contains(v, "OldNick") {
		t.Fatalf("unexpected Redis cache value after reload: val=%q err=%v", v, err)
	}
}

// TestM0301BatchProfileLockSameStripeAndOverlappingOrder 覆盖 M03-01 并发锁安全验收：
// 1. 同一批次包含映射到相同条纹锁槽的两个不同用户 ID 不自锁；
// 2. 多个并发批次以不同顺序传入重叠用户 ID 无死锁；
// 3. 列表返回顺序仍严格遵循关注关系游标倒序。
func TestM0301BatchProfileLockSameStripeAndOverlappingOrder(t *testing.T) {
	ctx := context.Background()
	viewer := testUIDBase + 200
	sameSlotA := testUIDBase + 201
	sameSlotB := sameSlotA + userLockStripes // 与 sameSlotA 命中同一条纹锁槽
	other1 := testUIDBase + 202
	other2 := testUIDBase + 203
	other3 := testUIDBase + 204
	allTargets := []uint64{sameSlotB, other1, sameSlotA, other3, other2}

	allUIDs := append([]uint64{viewer}, allTargets...)
	_, rdb := openTestRedis(t, testKeysForUsers(allUIDs...))
	local := gocache.NewCache(2 << 20)
	uStore := newStubUserStore(allUIDs...)
	fStore := newStubFollowStore()
	userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)

	// 按刻意打乱的顺序建立关注关系，验证列表严格按关系 ID 倒序返回。
	for _, target := range allTargets {
		if err := followSvc.Follow(ctx, viewer, target); err != nil {
			t.Fatalf("follow %d: %v", target, err)
		}
	}

	done := make(chan struct{})
	errCh := make(chan error, 64)
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		batches := [][]uint64{
			{sameSlotA, sameSlotB, other1, other2, other3},
			{other3, other2, sameSlotB, other1, sameSlotA},
			{sameSlotB, sameSlotA, other2},
			{other1, sameSlotA, other3, sameSlotB},
		}
		for i := 0; i < 16; i++ {
			batch := batches[i%len(batches)]
			wg.Add(1)
			go func(ids []uint64, idx int) {
				defer wg.Done()
				for j := 0; j < 15; j++ {
					profs, err := userSvc.GetPublicProfiles(ctx, ids)
					if err != nil {
						errCh <- err
						return
					}
					if len(profs) != len(ids) {
						errCh <- fmt.Errorf("got %d profiles, want %d", len(profs), len(ids))
						return
					}
					if (idx+j)%3 == 0 {
						userSvc.InvalidateProfile(ctx, ids[j%len(ids)])
					}
				}
			}(batch, i)
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock detected in concurrent GetPublicProfiles with overlapping/same-stripe IDs")
	}
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent batch read error: %v", err)
	}

	page, err := followSvc.ListFollowing(ctx, viewer, viewer, "", 20)
	if err != nil {
		t.Fatalf("list following failed: %v", err)
	}
	wantOrder := []uint64{other2, other3, sameSlotA, other1, sameSlotB}
	if len(page.Items) != len(wantOrder) {
		t.Fatalf("len(items)=%d want=%d", len(page.Items), len(wantOrder))
	}
	for i, want := range wantOrder {
		if page.Items[i].ID != want {
			t.Fatalf("item[%d].ID=%d, want=%d (cursor edge order broken)", i, page.Items[i].ID, want)
		}
	}
}

// TestM0301BatchProfileReadUsesSingleMySQLQuery 确认加条纹锁后仍通过一次 MySQL 批量查询加载多个缺失资料。
func TestM0301BatchProfileReadUsesSingleMySQLQuery(t *testing.T) {
	ctx := context.Background()
	u1 := testUIDBase + 301
	u2 := u1 + userLockStripes // 同槽
	u3 := testUIDBase + 302
	u4 := testUIDBase + 303
	ids := []uint64{u1, u2, u3, u4}

	_, rdb := openTestRedis(t, testKeysForUsers(ids...))
	local := gocache.NewCache(2 << 20)
	uStore := newStubUserStore(ids...)
	fStore := newStubFollowStore()
	userSvc, _ := newTestServices(local, rdb, uStore, fStore)

	profs, err := userSvc.GetPublicProfiles(ctx, ids)
	if err != nil {
		t.Fatalf("GetPublicProfiles failed: %v", err)
	}
	if len(profs) != len(ids) {
		t.Fatalf("got %d profiles, want %d", len(profs), len(ids))
	}
	if calls := uStore.getPublicIDCalls.Load(); calls != 1 {
		t.Fatalf("GetPublicByIDs called %d times, want 1 batched MySQL load", calls)
	}
}

// TestM0302CanceledRequestStillInvalidatesStatsCache 复现并回归 M03-02：
// 覆盖首次关注（空值标记缓存）、非首次关注（普通计数缓存）与取关（普通计数缓存）三种路径，
// 在仓储确认提交后取消原请求 ctx，验证双方 L2 缓存均被清理、新请求返回最新计数且重复请求仍幂等。
func TestM0302CanceledRequestStillInvalidatesStatsCache(t *testing.T) {
	t.Run("follow_after_null_marker_cache", func(t *testing.T) {
		ctx := context.Background()
		userA := testUIDBase + 401
		userB := testUIDBase + 402
		_, rdb := openTestRedis(t, testKeysForUsers(userA, userB))
		local := gocache.NewCache(2 << 20)
		uStore := newStubUserStore(userA, userB)
		fStore := newStubFollowStore()
		userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)

		// 预热双方空值标记缓存。
		if _, err := userSvc.GetProfile(ctx, userA, userA); err != nil {
			t.Fatal(err)
		}
		if _, err := userSvc.GetProfile(ctx, userB, userA); err != nil {
			t.Fatal(err)
		}
		keyA := "follow:stats:" + strconv.FormatUint(userA, 10)
		keyB := "follow:stats:" + strconv.FormatUint(userB, 10)
		if v, err := rdb.Get(ctx, keyA).Result(); err != nil || v != string(cache.NullMarker) {
			t.Fatalf("expected null marker in Redis for A, got %q err=%v", v, err)
		}
		if v, err := rdb.Get(ctx, keyB).Result(); err != nil || v != string(cache.NullMarker) {
			t.Fatalf("expected null marker in Redis for B, got %q err=%v", v, err)
		}

		reqCtx, cancel := context.WithCancel(ctx)
		fStore.onAfterFollowCommit = func(_, _ uint64, changed bool) {
			if changed {
				cancel()
			}
		}
		if err := followSvc.Follow(reqCtx, userA, userB); err != nil {
			t.Fatalf("follow failed: %v", err)
		}
		fStore.onAfterFollowCommit = nil

		// 验证双方 L2 Key 已彻底删除（无空值标记残留）。
		if exists, err := rdb.Exists(ctx, keyA, keyB).Result(); err != nil || exists != 0 {
			t.Fatalf("expected both L2 stats keys deleted after canceled follow, exists=%d err=%v", exists, err)
		}

		// 新请求读取双方主页均反映已提交结果。
		profA, err := userSvc.GetProfile(ctx, userA, userA)
		if err != nil || profA.FollowingCount != 1 || profA.FollowerCount != 0 {
			t.Fatalf("userA profile mismatch: %+v err=%v", profA, err)
		}
		profB, err := userSvc.GetProfile(ctx, userB, userA)
		if err != nil || profB.FollowingCount != 0 || profB.FollowerCount != 1 || !profB.IsFollowing {
			t.Fatalf("userB profile mismatch: %+v err=%v", profB, err)
		}

		// 重复相同关注仍幂等，数据库与返回计数保持一致。
		if err := followSvc.Follow(ctx, userA, userB); err != nil {
			t.Fatalf("idempotent follow failed: %v", err)
		}
		profA2, err := userSvc.GetProfile(ctx, userA, userA)
		if err != nil || profA2.FollowingCount != 1 {
			t.Fatalf("idempotent follow altered count: %+v err=%v", profA2, err)
		}
		if dbStats, _ := fStore.GetStatsByUserIDs(ctx, []uint64{userA, userB}); dbStats[userA].FollowingCount != 1 || dbStats[userB].FollowerCount != 1 {
			t.Fatalf("db stats mismatch after idempotent follow: %+v", dbStats)
		}
	})

	t.Run("follow_after_normal_stats_cache", func(t *testing.T) {
		ctx := context.Background()
		userA := testUIDBase + 411
		userB := testUIDBase + 412
		userC := testUIDBase + 413
		userD := testUIDBase + 414
		_, rdb := openTestRedis(t, testKeysForUsers(userA, userB, userC, userD))
		local := gocache.NewCache(2 << 20)
		uStore := newStubUserStore(userA, userB, userC, userD)
		fStore := newStubFollowStore()
		userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)

		if err := followSvc.Follow(ctx, userA, userB); err != nil {
			t.Fatal(err)
		}
		if err := followSvc.Follow(ctx, userC, userD); err != nil {
			t.Fatal(err)
		}

		// 预热 A（following=1）与 D（follower=1）的普通计数缓存。
		if _, err := userSvc.GetProfile(ctx, userA, userA); err != nil {
			t.Fatal(err)
		}
		if _, err := userSvc.GetProfile(ctx, userD, userA); err != nil {
			t.Fatal(err)
		}
		keyA := "follow:stats:" + strconv.FormatUint(userA, 10)
		keyD := "follow:stats:" + strconv.FormatUint(userD, 10)
		assertStatsRedisJSON(t, ctx, rdb, keyA, model.FollowStats{FollowingCount: 1, FollowerCount: 0})
		assertStatsRedisJSON(t, ctx, rdb, keyD, model.FollowStats{FollowingCount: 0, FollowerCount: 1})

		reqCtx, cancel := context.WithCancel(ctx)
		fStore.onAfterFollowCommit = func(_, _ uint64, changed bool) {
			if changed {
				cancel()
			}
		}
		if err := followSvc.Follow(reqCtx, userA, userD); err != nil {
			t.Fatalf("follow A->D failed: %v", err)
		}
		fStore.onAfterFollowCommit = nil

		if exists, err := rdb.Exists(ctx, keyA, keyD).Result(); err != nil || exists != 0 {
			t.Fatalf("expected both L2 keys deleted after canceled follow, exists=%d err=%v", exists, err)
		}

		profA, err := userSvc.GetProfile(ctx, userA, userA)
		if err != nil || profA.FollowingCount != 2 {
			t.Fatalf("userA following_count mismatch: %+v err=%v", profA, err)
		}
		profD, err := userSvc.GetProfile(ctx, userD, userA)
		if err != nil || profD.FollowerCount != 2 || !profD.IsFollowing {
			t.Fatalf("userD follower_count mismatch: %+v err=%v", profD, err)
		}

		// 重复关注幂等。
		if err := followSvc.Follow(ctx, userA, userD); err != nil {
			t.Fatalf("idempotent follow A->D failed: %v", err)
		}
		if dbStats, _ := fStore.GetStatsByUserIDs(ctx, []uint64{userA, userD}); dbStats[userA].FollowingCount != 2 || dbStats[userD].FollowerCount != 2 {
			t.Fatalf("db stats mismatch after idempotent follow: %+v", dbStats)
		}
	})

	t.Run("unfollow_after_normal_stats_cache", func(t *testing.T) {
		ctx := context.Background()
		userA := testUIDBase + 421
		userB := testUIDBase + 422
		_, rdb := openTestRedis(t, testKeysForUsers(userA, userB))
		local := gocache.NewCache(2 << 20)
		uStore := newStubUserStore(userA, userB)
		fStore := newStubFollowStore()
		userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)

		if err := followSvc.Follow(ctx, userA, userB); err != nil {
			t.Fatal(err)
		}
		// 预热普通计数缓存。
		if _, err := userSvc.GetProfile(ctx, userA, userA); err != nil {
			t.Fatal(err)
		}
		if _, err := userSvc.GetProfile(ctx, userB, userA); err != nil {
			t.Fatal(err)
		}
		keyA := "follow:stats:" + strconv.FormatUint(userA, 10)
		keyB := "follow:stats:" + strconv.FormatUint(userB, 10)
		assertStatsRedisJSON(t, ctx, rdb, keyA, model.FollowStats{FollowingCount: 1, FollowerCount: 0})
		assertStatsRedisJSON(t, ctx, rdb, keyB, model.FollowStats{FollowingCount: 0, FollowerCount: 1})

		reqCtx, cancel := context.WithCancel(ctx)
		fStore.onAfterUnfollowCommit = func(_, _ uint64, changed bool) {
			if changed {
				cancel()
			}
		}
		if err := followSvc.Unfollow(reqCtx, userA, userB); err != nil {
			t.Fatalf("unfollow failed: %v", err)
		}
		fStore.onAfterUnfollowCommit = nil

		if exists, err := rdb.Exists(ctx, keyA, keyB).Result(); err != nil || exists != 0 {
			t.Fatalf("expected both L2 keys deleted after canceled unfollow, exists=%d err=%v", exists, err)
		}

		profA, err := userSvc.GetProfile(ctx, userA, userA)
		if err != nil || profA.FollowingCount != 0 || profA.FollowerCount != 0 {
			t.Fatalf("userA profile after unfollow mismatch: %+v err=%v", profA, err)
		}
		profB, err := userSvc.GetProfile(ctx, userB, userA)
		if err != nil || profB.FollowingCount != 0 || profB.FollowerCount != 0 || profB.IsFollowing {
			t.Fatalf("userB profile after unfollow mismatch: %+v err=%v", profB, err)
		}

		// 重复取关幂等。
		if err := followSvc.Unfollow(ctx, userA, userB); err != nil {
			t.Fatalf("idempotent unfollow failed: %v", err)
		}
		if dbStats, _ := fStore.GetStatsByUserIDs(ctx, []uint64{userA, userB}); dbStats[userA].FollowingCount != 0 || dbStats[userB].FollowerCount != 0 {
			t.Fatalf("db stats mismatch after idempotent unfollow: %+v", dbStats)
		}
	})
}

// TestM0302RedisDeleteFailureAndTimeoutBounded 验证 M03-02 故障注入验收：
// 通过本地可控 TCP 代理连接底层 Redis（支持 TEST_REDIS_ADDR 真实 Redis 7 或 miniredis），
// 分别注入删除断连与删除超时，确认双方 L1 均清理、清理有时间上限、告警日志完整、且原请求有效时接口保留成功响应。
func TestM0302RedisDeleteFailureAndTimeoutBounded(t *testing.T) {
	t.Run("delete_connection_failure", func(t *testing.T) {
		ctx := context.Background()
		userA := testUIDBase + 501
		userB := testUIDBase + 502
		upstreamAddr, _ := openTestRedis(t, testKeysForUsers(userA, userB))

		proxy := newTCPFaultProxy(t, upstreamAddr)
		rdb := redis.NewClient(&redis.Options{
			Addr:                  proxy.Addr(),
			ContextTimeoutEnabled: true,
			MaxRetries:            0,
		})
		t.Cleanup(func() { _ = rdb.Close() })
		local := gocache.NewCache(2 << 20)
		uStore := newStubUserStore(userA, userB)
		fStore := newStubFollowStore()
		userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)

		// 预热双方资料与计数缓存（资料在 L1 命中，避免后续 Follow 校验用户存在时受 Redis 断连干扰）。
		if _, err := userSvc.GetProfile(ctx, userA, userA); err != nil {
			t.Fatal(err)
		}
		if _, err := userSvc.GetProfile(ctx, userB, userA); err != nil {
			t.Fatal(err)
		}

		keyA := "follow:stats:" + strconv.FormatUint(userA, 10)
		keyB := "follow:stats:" + strconv.FormatUint(userB, 10)
		if _, err := local.Get([]byte(keyA)); err != nil {
			t.Fatalf("expected userA stats cached in L1: %v", err)
		}
		if _, err := local.Get([]byte(keyB)); err != nil {
			t.Fatalf("expected userB stats cached in L1: %v", err)
		}

		logBuf := &syncBuffer{}
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		t.Cleanup(func() { slog.SetDefault(prevLogger) })

		// 注入 Redis 断连故障。
		proxy.SetMode(faultDropConn)

		// 原请求有效，关注仍应返回成功。
		if err := followSvc.Follow(ctx, userA, userB); err != nil {
			t.Fatalf("follow should preserve committed success on Redis delete failure, got %v", err)
		}
		if _, err := local.Get([]byte(keyA)); err == nil {
			t.Fatal("userA L1 stats cache was not cleared on Redis delete failure")
		}
		if _, err := local.Get([]byte(keyB)); err == nil {
			t.Fatal("userB L1 stats cache was not cleared on Redis delete failure")
		}
		logs := logBuf.String()
		if !strings.Contains(logs, "delete follow stats l2 cache failed") ||
			!strings.Contains(logs, "user_id="+strconv.FormatUint(userA, 10)) ||
			!strings.Contains(logs, "user_id="+strconv.FormatUint(userB, 10)) {
			t.Fatalf("expected warn logs for both users' L2 delete failure, got: %s", logs)
		}

		// 切回正常模式验证取关路径的断连故障同样清理双方 L1 并保留成功。
		proxy.SetMode(faultPassThrough)
		if _, err := userSvc.GetProfile(ctx, userA, userA); err != nil {
			t.Fatal(err)
		}
		if _, err := userSvc.GetProfile(ctx, userB, userA); err != nil {
			t.Fatal(err)
		}
		proxy.SetMode(faultDropConn)
		if err := followSvc.Unfollow(ctx, userA, userB); err != nil {
			t.Fatalf("unfollow should preserve committed success on Redis delete failure, got %v", err)
		}
		if _, err := local.Get([]byte(keyA)); err == nil {
			t.Fatal("userA L1 stats cache was not cleared on unfollow Redis delete failure")
		}
		if _, err := local.Get([]byte(keyB)); err == nil {
			t.Fatal("userB L1 stats cache was not cleared on unfollow Redis delete failure")
		}
	})

	t.Run("delete_timeout_bounded", func(t *testing.T) {
		ctx := context.Background()
		userA := testUIDBase + 511
		userB := testUIDBase + 512
		upstreamAddr, _ := openTestRedis(t, testKeysForUsers(userA, userB))

		proxy := newTCPFaultProxy(t, upstreamAddr)
		rdb := redis.NewClient(&redis.Options{
			Addr:                  proxy.Addr(),
			ReadTimeout:           5 * time.Second, // 远大于单用户清理超时，确保由清理 ctx 截断
			ContextTimeoutEnabled: true,
		})
		t.Cleanup(func() { _ = rdb.Close() })
		local := gocache.NewCache(2 << 20)
		uStore := newStubUserStore(userA, userB)
		fStore := newStubFollowStore()
		userSvc, followSvc := newTestServices(local, rdb, uStore, fStore)
		followSvc.invalidateTimeout = 150 * time.Millisecond

		// 预热双方资料和计数到 L1/L2。
		if _, err := userSvc.GetProfile(ctx, userA, userA); err != nil {
			t.Fatal(err)
		}
		if _, err := userSvc.GetProfile(ctx, userB, userA); err != nil {
			t.Fatal(err)
		}

		logBuf := &syncBuffer{}
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		t.Cleanup(func() { slog.SetDefault(prevLogger) })

		// 切换代理到黑洞挂起模式，模拟 Redis DEL 超时。
		proxy.SetMode(faultHang)

		start := time.Now()
		err := followSvc.Follow(ctx, userA, userB)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("follow should return success even when Redis delete times out, got %v", err)
		}
		// 双方各自 150ms 超时预算，总耗时应远小于客户端 ReadTimeout(5s)。
		if elapsed > 1200*time.Millisecond {
			t.Fatalf("cache invalidation exceeded bounded timeout: elapsed=%v", elapsed)
		}

		keyA := "follow:stats:" + strconv.FormatUint(userA, 10)
		keyB := "follow:stats:" + strconv.FormatUint(userB, 10)
		if _, err := local.Get([]byte(keyA)); err == nil {
			t.Fatal("userA L1 stats cache was not cleared on Redis delete timeout")
		}
		if _, err := local.Get([]byte(keyB)); err == nil {
			t.Fatal("userB L1 stats cache was not cleared on Redis delete timeout")
		}
		logs := logBuf.String()
		if !strings.Contains(logs, "deadline exceeded") ||
			!strings.Contains(logs, "user_id="+strconv.FormatUint(userA, 10)) ||
			!strings.Contains(logs, "user_id="+strconv.FormatUint(userB, 10)) {
			t.Fatalf("expected timeout warn logs for both users, got: %s", logs)
		}

		// 确认数据库已提交且未受清理超时影响。
		if ok, err := fStore.Exists(ctx, userA, userB); err != nil || !ok {
			t.Fatalf("expected committed follow relation in store: ok=%v err=%v", ok, err)
		}
	})
}

func assertStatsRedisJSON(t *testing.T, ctx context.Context, rdb *redis.Client, key string, want model.FollowStats) {
	t.Helper()
	raw, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			t.Fatalf("expected Redis key %s to exist", key)
		}
		t.Fatalf("get Redis key %s: %v", key, err)
	}
	var got model.FollowStats
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal Redis key %s (%q): %v", key, raw, err)
	}
	if got != want {
		t.Fatalf("Redis key %s = %+v, want %+v", key, got, want)
	}
}
