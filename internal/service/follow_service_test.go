package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/cursor"
	"wallpaper-feed/internal/pkg/errno"
)

func newTestFollowCaches(t *testing.T) (
	*miniredis.Miniredis,
	*cache.MultiLevel[model.UserProfileCache],
	*cache.MultiLevel[model.FollowStatsCache],
	*cache.MultiLevel[[]uint64],
) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	statsCache := cache.NewMultiLevel[model.FollowStatsCache](l1, rdb, 15*time.Second, 10*time.Minute)
	followingCache := cache.NewMultiLevel[[]uint64](l1, rdb, 15*time.Second, 10*time.Minute)
	return mr, profileCache, statsCache, followingCache
}

func TestContainsSortedID(t *testing.T) {
	ids := []uint64{10, 25, 100, 2000}
	tests := []struct {
		name   string
		list   []uint64
		target uint64
		want   bool
	}{
		{name: "empty list", list: nil, target: 10, want: false},
		{name: "zero target", list: ids, target: 0, want: false},
		{name: "first element", list: ids, target: 10, want: true},
		{name: "middle element", list: ids, target: 100, want: true},
		{name: "last element", list: ids, target: 2000, want: true},
		{name: "missing element", list: ids, target: 99, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := containsSortedID(tc.list, tc.target); got != tc.want {
				t.Fatalf("containsSortedID(%v, %d) = %v, want %v", tc.list, tc.target, got, tc.want)
			}
		})
	}
}

func TestPaginateFollowEdges(t *testing.T) {
	edges := []model.FollowEdge{
		{ID: 300, PeerID: 3},
		{ID: 200, PeerID: 2},
		{ID: 100, PeerID: 1},
	}

	page, next, hasMore, err := paginateFollowEdges(edges, 2)
	if err != nil || !hasMore || len(page) != 2 || page[1].ID != 200 {
		t.Fatalf("paginateFollowEdges(hasMore) = (%+v, %q, %v, %v)", page, next, hasMore, err)
	}
	decoded, err := cursor.Decode[cursor.IDCursor](next)
	if err != nil || decoded.ID != 200 {
		t.Fatalf("decoded cursor = (%+v, %v), want ID=200", decoded, err)
	}

	page, next, hasMore, err = paginateFollowEdges(edges[:2], 2)
	if err != nil || hasMore || next != "" || len(page) != 2 {
		t.Fatalf("paginateFollowEdges(exact) = (%+v, %q, %v, %v)", page, next, hasMore, err)
	}
}

func TestFollowServiceValidationAndSelfRules(t *testing.T) {
	mr, profileCache, statsCache, followingCache := newTestFollowCaches(t)
	userSvc := NewUserService(UserServiceDeps{
		ProfileCache:      profileCache,
		FollowStatsCache:  statsCache,
		FollowingIDsCache: followingCache,
		Store:             &stubStorage{},
	})
	svc := NewFollowService(nil, userSvc, statsCache, followingCache)
	ctx := context.Background()

	// 预灌不存在用户的空值哨兵与本人统计缓存。
	_ = mr.Set("user:profile:999", cache.NilMarker)
	selfStats, _ := json.Marshal(model.FollowStatsCache{FollowingCount: 5, FollowerCount: 42})
	_ = mr.Set("follow:stats:10", string(selfStats))

	if _, err := svc.Follow(ctx, 10, 10); !errors.Is(err, errno.ErrCannotFollowSelf) {
		t.Fatalf("Follow self err = %v, want ErrCannotFollowSelf", err)
	}
	if _, err := svc.Follow(ctx, 10, 999); !errors.Is(err, errno.ErrUserNotFound) {
		t.Fatalf("Follow missing target err = %v, want ErrUserNotFound", err)
	}

	// 取关本人直接幂等返回 200 并带回本人最新粉丝数。
	res, err := svc.Unfollow(ctx, 10, 10)
	if err != nil || res.IsFollowing || res.FollowerCount != 42 {
		t.Fatalf("Unfollow self = (%+v, %v), want is_following=false follower_count=42", res, err)
	}
	if _, err := svc.Unfollow(ctx, 10, 999); !errors.Is(err, errno.ErrUserNotFound) {
		t.Fatalf("Unfollow missing target err = %v, want ErrUserNotFound", err)
	}

	// 列表游标、limit 与目标用户存在性校验。
	zeroCursor, err := cursor.Encode(cursor.IDCursor{ID: 0})
	if err != nil {
		t.Fatalf("encode zero cursor: %v", err)
	}
	if _, err := svc.ListFollowing(ctx, 10, 0, "", 51); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("limit=51 err = %v, want ErrInvalidParam", err)
	}
	for _, bad := range []string{"bad!!!", zeroCursor} {
		if _, err := svc.ListFollowers(ctx, 10, 0, bad, 20); !errors.Is(err, errno.ErrInvalidCursor) {
			t.Fatalf("bad cursor %q err = %v, want ErrInvalidCursor", bad, err)
		}
	}
	if _, err := svc.ListFollowing(ctx, 999, 0, "", 20); !errors.Is(err, errno.ErrUserNotFound) {
		t.Fatalf("list missing user err = %v, want ErrUserNotFound", err)
	}
}

func TestAssembleFollowUserPage_MGetAndBinarySearchIsFollowing(t *testing.T) {
	mr, profileCache, statsCache, followingCache := newTestFollowCaches(t)
	userSvc := NewUserService(UserServiceDeps{
		ProfileCache:      profileCache,
		FollowStatsCache:  statsCache,
		FollowingIDsCache: followingCache,
		Store:             &stubStorage{},
	})
	svc := NewFollowService(nil, userSvc, statsCache, followingCache)
	ctx := context.Background()

	mustSetJSON := func(key string, v any) {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %s: %v", key, err)
		}
		_ = mr.Set(key, string(raw))
	}

	mustSetJSON("user:profile:1", model.UserProfileCache{ID: 1, Username: "alice", Nickname: "爱丽丝", Bio: "风景画师"})
	mustSetJSON("user:profile:2", model.UserProfileCache{ID: 2, Username: "bob", Nickname: "鲍勃", AvatarKey: "avatars/2/20261122/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.png"})
	mustSetJSON("user:profile:3", model.UserProfileCache{ID: 3, Username: "charlie", Nickname: "查理"})
	_ = mr.Set("user:profile:4", cache.NilMarker) // 异常缺失资料的幽灵用户

	mustSetJSON("follow:stats:1", model.FollowStatsCache{FollowerCount: 1200, IsBigV: true})
	mustSetJSON("follow:stats:2", model.FollowStatsCache{FollowerCount: 15})
	mustSetJSON("follow:stats:3", model.FollowStatsCache{FollowerCount: 0})
	mustSetJSON("follow:stats:4", model.FollowStatsCache{FollowerCount: 3})

	// 登录用户 2 关注了 1（即使脏数据包含自身 2，也应强制返回 false）。
	mustSetJSON("feed:following_ids:2", []uint64{1, 2})

	edges := []model.FollowEdge{
		{ID: 50, PeerID: 1},
		{ID: 49, PeerID: 4},
		{ID: 48, PeerID: 2},
		{ID: 47, PeerID: 3},
	}

	t.Run("guest viewer marks all is_following false", func(t *testing.T) {
		page, err := svc.assembleFollowUserPage(ctx, edges, 0, "cur-47", true)
		if err != nil {
			t.Fatalf("assembleFollowUserPage(guest) err = %v", err)
		}
		if !page.HasMore || page.NextCursor != "cur-47" || len(page.Items) != 3 {
			t.Fatalf("unexpected guest page: %+v", page)
		}
		for _, item := range page.Items {
			if item.IsFollowing {
				t.Fatalf("guest item %d should not be following", item.ID)
			}
		}
	})

	t.Run("authenticated viewer annotates is_following via sorted binary search", func(t *testing.T) {
		page, err := svc.assembleFollowUserPage(ctx, edges, 2, "cur-47", true)
		if err != nil {
			t.Fatalf("assembleFollowUserPage(auth) err = %v", err)
		}
		if len(page.Items) != 3 {
			t.Fatalf("expected 3 items skipping ghost 4, got %d", len(page.Items))
		}
		// 顺序保持 [1, 2, 3]。
		if page.Items[0].ID != 1 || !page.Items[0].IsFollowing || page.Items[0].FollowerCount != 1200 {
			t.Fatalf("item[0] mismatch: %+v", page.Items[0])
		}
		if page.Items[1].ID != 2 || page.Items[1].IsFollowing || !strings.Contains(page.Items[1].AvatarURL, "avatars/2/") {
			t.Fatalf("item[1] (self) mismatch: %+v", page.Items[1])
		}
		if page.Items[2].ID != 3 || page.Items[2].IsFollowing || !strings.Contains(page.Items[2].AvatarURL, defaultAvatarKey) {
			t.Fatalf("item[2] mismatch: %+v", page.Items[2])
		}
	})
}

func TestFollowServiceInvalidateAfterChange(t *testing.T) {
	mr, profileCache, statsCache, followingCache := newTestFollowCaches(t)
	svc := NewFollowService(nil, nil, statsCache, followingCache)
	ctx := context.Background()

	_ = mr.Set("follow:stats:10", `{"following_count":1,"follower_count":0,"is_bigv":false}`)
	_ = mr.Set("follow:stats:20", `{"following_count":0,"follower_count":1,"is_bigv":false}`)
	_ = mr.Set("feed:following_ids:10", `[20]`)
	_ = mr.Set("feed:following_ids:20", `[30]`)
	_ = mr.Set("user:profile:10", `{"id":"10","username":"a","nickname":"A","avatar_key":"","bio":""}`)

	// 先触发读填充 L1，确保失效同时清 L1 与 L2。
	if _, err := statsCache.Get(ctx, "follow:stats:10", nil); err != nil {
		t.Fatalf("warm L1 stats 10: %v", err)
	}
	if _, err := followingCache.Get(ctx, "feed:following_ids:10", nil); err != nil {
		t.Fatalf("warm L1 following 10: %v", err)
	}

	svc.invalidateAfterChange(ctx, 10, 20)

	for _, deleted := range []string{"follow:stats:10", "follow:stats:20", "feed:following_ids:10"} {
		if mr.Exists(deleted) {
			t.Fatalf("expected redis key %s to be deleted", deleted)
		}
	}
	// 不得误删目标用户的 following_ids 与用户资料缓存。
	for _, kept := range []string{"feed:following_ids:20", "user:profile:10"} {
		if !mr.Exists(kept) {
			t.Fatalf("expected redis key %s to be kept", kept)
		}
	}
	if _, err := profileCache.Get(ctx, "user:profile:10", nil); err != nil {
		t.Fatalf("profile cache should remain readable: %v", err)
	}
}

func TestUserServicePublicProfile_FollowStatsAndIsFollowing(t *testing.T) {
	mr, profileCache, statsCache, followingCache := newTestFollowCaches(t)
	userSvc := NewUserService(UserServiceDeps{
		ProfileCache:      profileCache,
		FollowStatsCache:  statsCache,
		FollowingIDsCache: followingCache,
		Store:             &stubStorage{},
	})
	ctx := context.Background()

	u20, _ := json.Marshal(model.UserProfileCache{ID: 20, Username: "creator", Nickname: "画师", CreatedAt: time.Now().UTC()})
	s20, _ := json.Marshal(model.FollowStatsCache{FollowingCount: 7, FollowerCount: 300, IsBigV: false})
	_ = mr.Set("user:profile:20", string(u20))
	_ = mr.Set("follow:stats:20", string(s20))
	_ = mr.Set("feed:following_ids:10", `[5,20,99]`)
	_ = mr.Set("feed:following_ids:11", `[5,99]`)

	// 已关注浏览者 10 -> is_following=true。
	pub, err := userSvc.GetPublicProfile(ctx, 20, 10)
	if err != nil || !pub.IsFollowing || pub.FollowingCount != 7 || pub.FollowerCount != 300 {
		t.Fatalf("GetPublicProfile(viewer=10) = (%+v, %v)", pub, err)
	}
	// 未关注浏览者 11 -> is_following=false。
	pub, err = userSvc.GetPublicProfile(ctx, 20, 11)
	if err != nil || pub.IsFollowing {
		t.Fatalf("GetPublicProfile(viewer=11) = (%+v, %v)", pub, err)
	}
	// 本人看自己或游客 -> is_following=false。
	for _, viewer := range []uint64{0, 20} {
		pub, err = userSvc.GetPublicProfile(ctx, 20, viewer)
		if err != nil || pub.IsFollowing {
			t.Fatalf("GetPublicProfile(viewer=%d) = (%+v, %v)", viewer, pub, err)
		}
	}
}
