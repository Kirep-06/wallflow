package cache

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLatestFeed(t *testing.T, maxSize int) (*miniredis.Miniredis, *LatestFeed) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	f := NewLatestFeed(rdb)
	if maxSize > 0 {
		f.maxSize = maxSize
	}
	return mr, f
}

func TestLatestFeed_NotReadySkipsPushAndReturnsFalse(t *testing.T) {
	mr, f := newTestLatestFeed(t, 10)
	ctx := context.Background()

	if err := f.PushIfReady(ctx, 101); err != nil {
		t.Fatalf("PushIfReady failed: %v", err)
	}
	if mr.Exists(latestFeedKey) {
		t.Fatalf("expected feed:latest not created when ready flag missing")
	}

	ids, ready, err := f.ReadPage(ctx, 0, 10)
	if err != nil || ready || len(ids) != 0 {
		t.Fatalf("ReadPage before ready: ids=%v ready=%v err=%v", ids, ready, err)
	}
}

func TestLatestFeed_ReconcileReadPushTrimAndRemove(t *testing.T) {
	mr, f := newTestLatestFeed(t, 5)
	ctx := context.Background()

	// 预热对账写入 1..5，确认无过期时间且 ready=1。
	if err := f.Reconcile(ctx, []uint64{5, 4, 3, 2, 1}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if ttl := mr.TTL(latestFeedKey); ttl != 0 {
		t.Fatalf("expected feed:latest persistent (ttl=0), got %v", ttl)
	}
	if raw, _ := mr.Get(latestFeedReadyKey); raw != "1" {
		t.Fatalf("expected ready flag = 1, got %q", raw)
	}

	// 首页与游标翻页严格按 id 倒序。
	page1, ready, err := f.ReadPage(ctx, 0, 3)
	if err != nil || !ready || !slices.Equal(page1, []uint64{5, 4, 3}) {
		t.Fatalf("page1 = (%v, %v, %v), want [5 4 3]", page1, ready, err)
	}
	page2, ready, err := f.ReadPage(ctx, 3, 3)
	if err != nil || !ready || !slices.Equal(page2, []uint64{2, 1}) {
		t.Fatalf("page2 = (%v, %v, %v), want [2 1]", page2, ready, err)
	}

	// 追加新帖超出 maxSize=5 自动裁剪最老的 ID=1。
	if err := f.PushIfReady(ctx, 6); err != nil {
		t.Fatalf("PushIfReady(6) failed: %v", err)
	}
	all, ready, err := f.ReadPage(ctx, 0, 10)
	if err != nil || !ready || !slices.Equal(all, []uint64{6, 5, 4, 3, 2}) {
		t.Fatalf("after push trim = %v, want [6 5 4 3 2]", all)
	}

	// 软删移除指定 ID。
	if err := f.Remove(ctx, 4); err != nil {
		t.Fatalf("Remove(4) failed: %v", err)
	}
	afterDel, _, err := f.ReadPage(ctx, 0, 10)
	if err != nil || !slices.Equal(afterDel, []uint64{6, 5, 3, 2}) {
		t.Fatalf("after remove = %v, want [6 5 3 2]", afterDel)
	}
}

func TestLatestFeed_ReconcileMergesConcurrentNewPostsAndCleansGhosts(t *testing.T) {
	_, f := newTestLatestFeed(t, 5)
	ctx := context.Background()

	// 初始热窗口含幽灵 ID=99，以及对账期间新发布的并发新帖 101、102。
	if err := f.Reconcile(ctx, []uint64{100, 99, 98, 97, 96}); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}
	if err := f.PushIfReady(ctx, 101); err != nil {
		t.Fatalf("push 101: %v", err)
	}
	if err := f.PushIfReady(ctx, 102); err != nil {
		t.Fatalf("push 102: %v", err)
	}

	// DB 快照最大 ID=100（且 99 已被软删不在 DB 快照中），对账应保留 101/102、剔除幽灵 99 并裁剪至 5 条。
	if err := f.Reconcile(ctx, []uint64{100, 98, 97, 96, 95}); err != nil {
		t.Fatalf("reconcile merge: %v", err)
	}
	got, ready, err := f.ReadPage(ctx, 0, 10)
	if err != nil || !ready || !slices.Equal(got, []uint64{102, 101, 100, 98, 97}) {
		t.Fatalf("after merge reconcile = %v, want [102 101 100 98 97]", got)
	}
}

func TestLatestFeed_ReconcileEmptyDBAndZeroToOneConcurrentPost(t *testing.T) {
	mr, f := newTestLatestFeed(t, 5)
	ctx := context.Background()

	// 空库首次对账：不留 feed:latest 脏键，ready=1，读返回空切片。
	if err := f.Reconcile(ctx, nil); err != nil {
		t.Fatalf("empty reconcile: %v", err)
	}
	if mr.Exists(latestFeedKey) {
		t.Fatalf("expected feed:latest absent when empty")
	}
	ids, ready, err := f.ReadPage(ctx, 0, 10)
	if err != nil || !ready || len(ids) != 0 {
		t.Fatalf("empty ReadPage = (%v, %v, %v)", ids, ready, err)
	}

	// 全站 0->1 并发新帖：对账拿到空 DB 快照，但期间已有 ID=1 写入 feed:latest，应保留不被清空。
	if err := f.PushIfReady(ctx, 1); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	if err := f.Reconcile(ctx, nil); err != nil {
		t.Fatalf("reconcile with concurrent first post: %v", err)
	}
	ids, ready, err = f.ReadPage(ctx, 0, 10)
	if err != nil || !ready || !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("expected concurrent first post [1] preserved, got (%v, %v, %v)", ids, ready, err)
	}
}
