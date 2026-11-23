package cache

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/model"
)

func newTestHotRank(t *testing.T, clock *time.Time) (*miniredis.Miniredis, *HotRank) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	hr := NewHotRank(rdb)
	if clock != nil {
		hr.nowFunc = func() time.Time { return *clock }
	}
	return mr, hr
}

func TestHotRank_NotReadyAndEmptySentinel(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	mr, hr := newTestHotRank(t, &now)
	ctx := context.Background()

	// 未预热前返回 Ready=false
	snap, err := hr.ReadSnapshot(ctx, "")
	if err != nil || snap.Ready {
		t.Fatalf("before swap: snap=%+v err=%v", snap, err)
	}

	// 空榜 Swap 写入哨兵 "0"，永不过期且防穿透
	ver, err := hr.Swap(ctx, nil)
	if err != nil || ver == "" {
		t.Fatalf("empty swap failed: ver=%q err=%v", ver, err)
	}
	if ttl := mr.TTL(hotRankKey); ttl != 0 {
		t.Fatalf("expected rank:hot:top persistent (ttl=0), got %v", ttl)
	}
	if ttl := mr.TTL(hotRankVerKey); ttl != 0 {
		t.Fatalf("expected rank:hot:version persistent (ttl=0), got %v", ttl)
	}

	snap, err = hr.ReadSnapshot(ctx, "")
	if err != nil || !snap.Ready || snap.NotModified || snap.Version != ver || len(snap.Entries) != 0 {
		t.Fatalf("empty ready snap = %+v err=%v", snap, err)
	}
}

func TestHotRank_SwapReadShortCircuitMonotonicVersionAndRemove(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	mr, hr := newTestHotRank(t, &now)
	ctx := context.Background()

	v1, err := hr.Swap(ctx, []model.HotRankEntry{
		{WallpaperID: 10, Score: 5.25},
		{WallpaperID: 9, Score: 5.25},
		{WallpaperID: 20, Score: 9.5},
	})
	if err != nil {
		t.Fatalf("Swap v1 failed: %v", err)
	}
	if want := strconv.FormatInt(now.UnixMilli(), 10); v1 != want {
		t.Fatalf("v1 = %q, want %q", v1, want)
	}
	if ttl := mr.TTL(hotRankKey); ttl != 0 {
		t.Fatalf("expected persistent rank:hot:top, got ttl=%v", ttl)
	}

	// 首屏命中版本与通配符 "*" 均零载荷短路返回 NotModified=true
	hit, err := hr.ReadSnapshot(ctx, v1)
	if err != nil || !hit.Ready || !hit.NotModified || hit.Version != v1 || len(hit.Entries) != 0 {
		t.Fatalf("short-circuit hit = %+v err=%v", hit, err)
	}
	wild, err := hr.ReadSnapshot(ctx, "*")
	if err != nil || !wild.Ready || !wild.NotModified || wild.Version != v1 {
		t.Fatalf("wildcard hit = %+v err=%v", wild, err)
	}

	// 未命中返回完整成员（已剔除哨兵 "0"）
	full, err := hr.ReadSnapshot(ctx, "stale-ver")
	if err != nil || !full.Ready || full.NotModified || len(full.Entries) != 3 {
		t.Fatalf("full read = %+v err=%v", full, err)
	}

	// 时钟倒流时 Swap 仍单调递增（cur + 1），防止版本号回退碰撞
	now = now.Add(-10 * time.Minute)
	v2, err := hr.Swap(ctx, []model.HotRankEntry{
		{WallpaperID: 30, Score: 8.0},
	})
	if err != nil {
		t.Fatalf("Swap v2 failed: %v", err)
	}
	v1Int, _ := strconv.ParseInt(v1, 10, 64)
	v2Int, _ := strconv.ParseInt(v2, 10, 64)
	if v2Int != v1Int+1 {
		t.Fatalf("expected monotonic v2=%d, got %d", v1Int+1, v2Int)
	}

	// 删除不在榜壁纸不扰动版本号
	if err := hr.Remove(ctx, 999); err != nil {
		t.Fatalf("Remove non-member failed: %v", err)
	}
	curVer, _ := mr.Get(hotRankVerKey)
	if curVer != v2 {
		t.Fatalf("version changed on non-member remove: got %q want %q", curVer, v2)
	}

	// 删除榜上最后 1 张壁纸：版本号递增击穿旧 304，且哨兵保活 rank:hot:top
	if err := hr.Remove(ctx, 30); err != nil {
		t.Fatalf("Remove member 30 failed: %v", err)
	}
	v3, _ := mr.Get(hotRankVerKey)
	v3Int, _ := strconv.ParseInt(v3, 10, 64)
	if v3Int != v2Int+1 {
		t.Fatalf("expected version incremented to %d after remove, got %d", v2Int+1, v3Int)
	}
	afterDel, err := hr.ReadSnapshot(ctx, v2)
	if err != nil || !afterDel.Ready || afterDel.NotModified || len(afterDel.Entries) != 0 {
		t.Fatalf("after remove last item: snap=%+v err=%v", afterDel, err)
	}
}
