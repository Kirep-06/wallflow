package service

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
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

func TestMergeDescendingKWay(t *testing.T) {
	t.Run("multi stream merge with bigv promotion duplicates deduplicated", func(t *testing.T) {
		// 模拟作者晋升大 V 过渡期：#90 与 #70 同时存在于 inbox 与 outbox
		inbox := []uint64{100, 90, 70, 50, 10}
		outbox1 := []uint64{95, 90, 80, 70, 60}
		outbox2 := []uint64{85, 70, 40}

		got := mergeDescendingKWay([][]uint64{inbox, nil, outbox1, outbox2}, 7)
		want := []uint64{100, 95, 90, 85, 80, 70, 60}
		if !slices.Equal(got, want) {
			t.Fatalf("got = %v, want %v", got, want)
		}
	})

	t.Run("single stream fast path deduplicates and caps", func(t *testing.T) {
		got := mergeDescendingKWay([][]uint64{nil, {50, 50, 40, 30}}, 2)
		want := []uint64{50, 40}
		if !slices.Equal(got, want) {
			t.Fatalf("got = %v, want %v", got, want)
		}
	})
}

func TestResolveFollowingFeedIDsWatermark(t *testing.T) {
	t.Run("hot zone above watermark makes zero cold db calls", func(t *testing.T) {
		coldCalls := 0
		streams := []feedStream{
			{ids: []uint64{100, 90, 80}, floorID: 50},
			{ids: []uint64{95, 85, 75}, floorID: 70},
		}
		got, err := resolveFollowingFeedIDs(streams, 0, 4, func(uint64, int) ([]uint64, error) {
			coldCalls++
			return nil, nil
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		want := []uint64{100, 95, 90, 85}
		if !slices.Equal(got, want) || coldCalls != 0 {
			t.Fatalf("got=%v coldCalls=%d, want=%v 0", got, coldCalls, want)
		}
	})

	t.Run("untruncated streams reaching bottom make zero cold db calls", func(t *testing.T) {
		coldCalls := 0
		streams := []feedStream{
			{ids: []uint64{30, 10}, floorID: 0},
			{ids: []uint64{20}, floorID: 0},
		}
		got, err := resolveFollowingFeedIDs(streams, 0, 10, func(uint64, int) ([]uint64, error) {
			coldCalls++
			return []uint64{5}, nil
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		want := []uint64{30, 20, 10}
		if !slices.Equal(got, want) || coldCalls != 0 {
			t.Fatalf("got=%v coldCalls=%d, want=%v 0", got, coldCalls, want)
		}
	})

	t.Run("prolific bigv truncation triggers cold backfill even when inbox alone has enough items", func(t *testing.T) {
		// 经典防漏帖用例：inbox 有充足元素 [4980, 4970, 4960, 4950] (floor=1000)，
		// 但高产大 V 的发件箱在 #5000 就已截断（floor=5000），在 DB 中还有 #4985。
		// 传 beforeID=4990, want=4：必须发现尾部跌破 W_safe=5000，回源 DB 补齐零漏 #4985！
		streams := []feedStream{
			{ids: []uint64{4980, 4970, 4960, 4950}, floorID: 1000},
			{ids: nil, floorID: 5000},
		}
		var gotBefore uint64
		var gotNeed int
		got, err := resolveFollowingFeedIDs(streams, 4990, 4, func(coldBefore uint64, count int) ([]uint64, error) {
			gotBefore = coldBefore
			gotNeed = count
			return []uint64{4985, 4980, 4970, 4960}, nil
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		want := []uint64{4985, 4980, 4970, 4960}
		if gotBefore != 4990 || gotNeed != 4 || !slices.Equal(got, want) {
			t.Fatalf("got=(%v, before=%d, need=%d), want=(%v, 4990, 4)", got, gotBefore, gotNeed, want)
		}
	})

	t.Run("continuous pagination across hot-cold boundary has zero duplicates and zero missing", func(t *testing.T) {
		// 全量真值数据集：普通作者 + 高产大 V（其中 #14 为大 V 晋升过渡期双写重叠）
		normalAll := []uint64{18, 14, 11, 9, 6, 4, 2}
		bigVAll := []uint64{20, 19, 17, 16, 15, 14, 13, 12, 10, 8, 7, 5, 3, 1}
		inboxHot := normalAll[:5] // 容量 5 截断：floor=6
		outboxHot := bigVAll[:4]  // 容量 4 截断：floor=16

		allTruth := []uint64{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

		const pageSize = 3
		var (
			collected []uint64
			beforeID  uint64
		)
		for step := range 20 {
			st1 := sliceRebuiltBox(inboxHot, beforeID, pageSize+1, 5)
			st2 := sliceRebuiltBox(outboxHot, beforeID, pageSize+1, 4)

			rawIDs, err := resolveFollowingFeedIDs(
				[]feedStream{st1, st2},
				beforeID,
				pageSize+1,
				func(coldBefore uint64, count int) ([]uint64, error) {
					res := make([]uint64, 0, count)
					for _, id := range allTruth {
						if coldBefore > 0 && id >= coldBefore {
							continue
						}
						res = append(res, id)
						if len(res) >= count {
							break
						}
					}
					return res, nil
				},
			)
			if err != nil {
				t.Fatalf("step %d failed: %v", step, err)
			}

			pageIDs, nextCursor, hasMore, err := paginateIDs(rawIDs, pageSize)
			if err != nil {
				t.Fatalf("paginate failed: %v", err)
			}
			collected = append(collected, pageIDs...)
			if !hasMore {
				break
			}
			beforeID, err = parseIDCursor(nextCursor)
			if err != nil {
				t.Fatalf("parse cursor failed: %v", err)
			}
		}

		if !slices.Equal(collected, allTruth) {
			t.Fatalf("paginated result mismatch!\ngot:  %v\nwant: %v", collected, allTruth)
		}
	})
}

func TestFeedServiceValidationEmptyAndReadTimeUnfollowFilter(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	wallpaperCache := cache.NewMultiLevel[model.WallpaperDetailCache](l1, rdb, 30*time.Second, 10*time.Minute)
	followingCache := cache.NewMultiLevel[[]uint64](l1, rdb, 15*time.Second, 10*time.Minute)
	bigVSet := cache.NewBigVSet(rdb, 30*time.Second)
	followingFeed := cache.NewFollowingFeed(rdb)

	store := &stubStorage{}
	userSvc := NewUserService(UserServiceDeps{
		ProfileCache:      profileCache,
		FollowingIDsCache: followingCache,
		Store:             store,
	})
	wpSvc := NewWallpaperService(nil, nil, userSvc, wallpaperCache, nil, store)
	feedSvc := NewFeedService(FeedServiceDeps{
		FollowingIDsCache: followingCache,
		BigVSet:           bigVSet,
		FollowingFeed:     followingFeed,
		WallpaperSvc:      wpSvc,
	})

	ctx := context.Background()

	// 1. 参数与游标校验
	if _, err := feedSvc.ListFollowingFeed(ctx, 1, "bad-cursor", 20); !errors.Is(err, cursor.ErrInvalid) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
	if _, err := feedSvc.ListFollowingFeed(ctx, 1, "", 99); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("expected ErrInvalidParam for limit=99, got %v", err)
	}

	// 2. 空关注用户快速返回空分页
	_ = rdb.Set(ctx, followingIDsKey(1), "[]", time.Minute).Err()
	emptyPage, err := feedSvc.ListFollowingFeed(ctx, 1, "", 20)
	if err != nil {
		t.Fatalf("empty following feed failed: %v", err)
	}
	if len(emptyPage.Items) != 0 || emptyPage.HasMore || emptyPage.NextCursor != "" {
		t.Fatalf("unexpected empty page: %+v", emptyPage)
	}

	// 3. 读时二分兜底过滤：用户 2 仅关注普通作者 10 和大 V 20；
	// 模拟并发乱序导致已取关作者 30 的壁纸 #102 残留在 inbox 中，读路径应精准剔除 #102。
	_ = rdb.Set(ctx, followingIDsKey(2), "[10,20]", time.Minute).Err()
	if err := bigVSet.Reconcile(ctx, []uint64{20}); err != nil {
		t.Fatalf("reconcile bigv failed: %v", err)
	}
	if err := followingFeed.RebuildInbox(ctx, 2, []uint64{102, 101}); err != nil {
		t.Fatalf("rebuild inbox failed: %v", err)
	}
	if err := followingFeed.RebuildOutbox(ctx, 20, []uint64{103}); err != nil {
		t.Fatalf("rebuild outbox failed: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	for _, wp := range []model.WallpaperDetailCache{
		{ID: 103, UserID: 20, Title: "大V新作", ObjectKey: "wallpapers/20/103.jpg", CreatedAt: now},
		{ID: 102, UserID: 30, Title: "已取关残余", ObjectKey: "wallpapers/30/102.jpg", CreatedAt: now},
		{ID: 101, UserID: 10, Title: "普通作者新作", ObjectKey: "wallpapers/10/101.jpg", CreatedAt: now},
	} {
		raw, _ := json.Marshal(wp)
		_ = rdb.Set(ctx, wallpaperDetailKey(wp.ID), raw, time.Minute).Err()
	}
	for _, p := range []model.UserProfileCache{
		{ID: 10, Username: "normal10", Nickname: "普通画师"},
		{ID: 20, Username: "bigv20", Nickname: "大V画师"},
		{ID: 30, Username: "unfollowed30", Nickname: "已取关画师"},
	} {
		raw, _ := json.Marshal(p)
		_ = rdb.Set(ctx, userProfileKey(p.ID), raw, time.Minute).Err()
	}

	page, err := feedSvc.ListFollowingFeed(ctx, 2, "", 10)
	if err != nil {
		t.Fatalf("ListFollowingFeed failed: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("expected 2 items after filtering unfollowed author 30, got %+v", page.Items)
	}
	if page.Items[0].ID != 103 || page.Items[1].ID != 101 {
		t.Fatalf("unexpected item order/ids: %+v", page.Items)
	}
}

func TestFeedService_ListHotFeed_RedisETagPaginationAndRemoveBust(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	followingIDsCache := cache.NewMultiLevel[[]uint64](l1, rdb, 15*time.Second, 10*time.Minute)
	wallpaperCache := cache.NewMultiLevel[model.WallpaperDetailCache](l1, rdb, 30*time.Second, 10*time.Minute)
	statsCache := cache.NewMultiLevel[model.WallpaperStatsCache](l1, rdb, 10*time.Second, 10*time.Minute)
	hotRank := cache.NewHotRank(rdb)

	store := &stubStorage{}
	userSvc := NewUserService(UserServiceDeps{
		ProfileCache:      profileCache,
		FollowingIDsCache: followingIDsCache,
		Store:             store,
	})
	wpSvc := NewWallpaperService(
		nil, nil, userSvc, wallpaperCache, nil, store,
		WithWallpaperInteraction(nil, statsCache),
		WithWallpaperHotRank(hotRank),
	)
	feedSvc := NewFeedService(FeedServiceDeps{
		HotRank:      hotRank,
		WallpaperSvc: wpSvc,
	})

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// 预热作者、壁纸详情与统计缓存，包含同分 ID=10 与 ID=9 验证字典序防错乱。
	rawAuthor, _ := json.Marshal(model.UserProfileCache{ID: 7, Username: "artist7", Nickname: "画师7"})
	_ = rdb.Set(ctx, userProfileKey(7), rawAuthor, time.Minute).Err()
	rawFollowing, _ := json.Marshal([]uint64{7})
	_ = rdb.Set(ctx, followingIDsKey(42), rawFollowing, time.Minute).Err()
	for _, id := range []uint64{100, 10, 9, 8} {
		rawWP, _ := json.Marshal(model.WallpaperDetailCache{
			ID: id, UserID: 7, Title: "热门图", ObjectKey: "wallpapers/7/img.jpg", CreatedAt: now,
		})
		_ = rdb.Set(ctx, wallpaperDetailKey(id), rawWP, time.Minute).Err()
		rawStats, _ := json.Marshal(model.WallpaperStatsCache{LikeCount: id, DownloadCount: id * 2})
		_ = rdb.Set(ctx, cache.WallpaperStatsKey(id), rawStats, time.Minute).Err()
	}

	ver, err := hotRank.Swap(ctx, []model.HotRankEntry{
		{WallpaperID: 100, Score: 10.0},
		{WallpaperID: 9, Score: 4.5},
		{WallpaperID: 10, Score: 4.5},
		{WallpaperID: 8, Score: 2.0},
	})
	if err != nil {
		t.Fatalf("hotRank.Swap failed: %v", err)
	}

	// 1. 非法 ScoreCursor（score<=0 / id=0 / 垃圾串）统一返回 50001
	zeroScoreCur, _ := cursor.Encode(cursor.ScoreCursor{Score: 0, ID: 10})
	negScoreCur, _ := cursor.Encode(cursor.ScoreCursor{Score: -1.5, ID: 10})
	zeroIDCur, _ := cursor.Encode(cursor.ScoreCursor{Score: 4.5, ID: 0})
	for _, bad := range []string{"bad-cursor", zeroScoreCur, negScoreCur, zeroIDCur} {
		if _, err := feedSvc.ListHotFeed(ctx, 0, bad, 20, ""); !errors.Is(err, cursor.ErrInvalid) {
			t.Fatalf("cursor %q: expected ErrInvalidCursor, got %v", bad, err)
		}
	}

	// 2. 游客首屏返回 200 + 弱 ETag，排序严格按 (score DESC, id DESC) -> [100, 10]
	p1, err := feedSvc.ListHotFeed(ctx, 0, "", 2, "")
	if err != nil {
		t.Fatalf("first page failed: %v", err)
	}
	wantETag := `W/"hot-` + ver + `-l2-u0"`
	if p1.NotModified || p1.ETag != wantETag {
		t.Fatalf("p1 ETag=%q NotModified=%v, want %q false", p1.ETag, p1.NotModified, wantETag)
	}
	if len(p1.Page.Items) != 2 || p1.Page.Items[0].ID != 100 || p1.Page.Items[1].ID != 10 || !p1.Page.HasMore {
		t.Fatalf("unexpected p1 items: %+v", p1.Page)
	}
	if p1.Page.Items[0].HotScore != 10.0 || p1.Page.Items[1].HotScore != 4.5 {
		t.Fatalf("expected hot_score [10.0, 4.5], got [%v, %v]", p1.Page.Items[0].HotScore, p1.Page.Items[1].HotScore)
	}
	if p1.Page.Items[0].Author.IsFollowing {
		t.Fatalf("guest viewer must see author.is_following=false")
	}

	// 3. 游客携带相同 ETag（单值与多值列表）均命中 304，不装配 Page
	hitSingle, err := feedSvc.ListHotFeed(ctx, 0, "", 2, wantETag)
	if err != nil || !hitSingle.NotModified || hitSingle.Page != nil || hitSingle.ETag != wantETag {
		t.Fatalf("expected 304 single hit, got %+v err=%v", hitSingle, err)
	}
	hitMulti, err := feedSvc.ListHotFeed(ctx, 0, "", 2, `W/"hot-1-l2-u0", `+wantETag)
	if err != nil || !hitMulti.NotModified || hitMulti.Page != nil {
		t.Fatalf("expected 304 multi hit, got %+v err=%v", hitMulti, err)
	}

	// 4. 登录态切换（viewerID=42 带着游客 u0 ETag）或换 limit 绝不误命中 304，且正确标注 author.is_following=true
	authRes, err := feedSvc.ListHotFeed(ctx, 42, "", 2, wantETag)
	if err != nil || authRes.NotModified || authRes.ETag != `W/"hot-`+ver+`-l2-u42"` {
		t.Fatalf("auth switch must miss 304 and issue u42 ETag, got %+v err=%v", authRes, err)
	}
	if len(authRes.Page.Items) != 2 || !authRes.Page.Items[0].Author.IsFollowing {
		t.Fatalf("logged-in viewer 42 follows author 7, expected author.is_following=true, got %+v", authRes.Page.Items)
	}
	limitRes, err := feedSvc.ListHotFeed(ctx, 0, "", 3, wantETag)
	if err != nil || limitRes.NotModified || limitRes.ETag != `W/"hot-`+ver+`-l3-u0"` {
		t.Fatalf("limit switch must miss 304 and issue l3 ETag, got %+v err=%v", limitRes, err)
	}

	// 5. 翻第 2 页携带 ScoreCursor（即使带了 If-None-Match 也不返 304 且不发 ETag），正确产出同分后续 [9, 8]
	p2, err := feedSvc.ListHotFeed(ctx, 0, p1.Page.NextCursor, 2, wantETag)
	if err != nil {
		t.Fatalf("page 2 failed: %v", err)
	}
	if p2.NotModified || p2.ETag != "" || p2.Page.HasMore || p2.Page.NextCursor != "" {
		t.Fatalf("unexpected page 2 meta: %+v", p2)
	}
	if len(p2.Page.Items) != 2 || p2.Page.Items[0].ID != 9 || p2.Page.Items[1].ID != 8 {
		t.Fatalf("unexpected page 2 items (tie-break): %+v", p2.Page.Items)
	}

	// 6. 软删榜首壁纸 #100 后，旧 ETag 立即失效不再返回 304，列表自动剔除 #100
	if err := hotRank.Remove(ctx, 100); err != nil {
		t.Fatalf("hotRank.Remove failed: %v", err)
	}
	afterDel, err := feedSvc.ListHotFeed(ctx, 0, "", 2, wantETag)
	if err != nil || afterDel.NotModified || afterDel.ETag == wantETag {
		t.Fatalf("expected ETag invalidated after remove, got %+v err=%v", afterDel, err)
	}
	if len(afterDel.Page.Items) != 2 || afterDel.Page.Items[0].ID != 10 || afterDel.Page.Items[1].ID != 9 {
		t.Fatalf("unexpected items after remove: %+v", afterDel.Page.Items)
	}
}

func TestFeedService_ListHotFeed_FallbackSingleflight(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	l1 := gocache.NewCache(2 * 1024 * 1024)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](l1, rdb, 30*time.Second, 30*time.Minute)
	wallpaperCache := cache.NewMultiLevel[model.WallpaperDetailCache](l1, rdb, 30*time.Second, 10*time.Minute)
	statsCache := cache.NewMultiLevel[model.WallpaperStatsCache](l1, rdb, 10*time.Second, 10*time.Minute)
	hotRank := cache.NewHotRank(rdb) // 未 Swap，模拟未就绪/降级场景

	store := &stubStorage{}
	userSvc := NewUserService(UserServiceDeps{ProfileCache: profileCache, Store: store})
	wpSvc := NewWallpaperService(nil, nil, userSvc, wallpaperCache, nil, store, WithWallpaperInteraction(nil, statsCache))
	feedSvc := NewFeedService(FeedServiceDeps{HotRank: hotRank, WallpaperSvc: wpSvc})

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	rawAuthor, _ := json.Marshal(model.UserProfileCache{ID: 1, Username: "u1", Nickname: "U1"})
	_ = rdb.Set(ctx, userProfileKey(1), rawAuthor, time.Minute).Err()
	for _, id := range []uint64{1, 2} {
		rawWP, _ := json.Marshal(model.WallpaperDetailCache{ID: id, UserID: 1, Title: "T", ObjectKey: "wallpapers/1/a.jpg", CreatedAt: now})
		_ = rdb.Set(ctx, wallpaperDetailKey(id), rawWP, time.Minute).Err()
		rawStats, _ := json.Marshal(model.WallpaperStatsCache{DownloadCount: id * 10})
		_ = rdb.Set(ctx, cache.WallpaperStatsKey(id), rawStats, time.Minute).Err()
	}

	var dbCalls atomic.Int32
	gate := make(chan struct{})
	feedSvc.hotBatchLoader = func(_ context.Context, beforeID uint64, limit int) ([]model.HotRankCandidate, error) {
		dbCalls.Add(1)
		<-gate
		if beforeID > 0 {
			return nil, nil
		}
		return []model.HotRankCandidate{
			{ID: 2, CreatedAt: now.Add(-time.Hour), DownloadCount: 20},
			{ID: 1, CreatedAt: now.Add(-time.Hour), DownloadCount: 10},
		}, nil
	}

	const concurrency = 16
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := feedSvc.ListHotFeed(ctx, 0, "", 10, `W/"hot-1-l10-u0"`)
			if err != nil {
				errs <- err
				return
			}
			if res.NotModified || res.ETag != "" || len(res.Page.Items) != 2 || res.Page.Items[0].ID != 2 {
				errs <- errors.New("unexpected fallback result")
			}
		}()
	}

	// 等待首个并发请求进入 singleflight 后统一放行
	time.Sleep(30 * time.Millisecond)
	close(gate)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent fallback failed: %v", err)
	}
	if calls := dbCalls.Load(); calls != 1 {
		t.Fatalf("expected singleflight coalesced into 1 DB call, got %d", calls)
	}
}
