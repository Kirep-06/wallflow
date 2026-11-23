package cache

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestBigVSetReconcileSentinelAndL1Snapshot(t *testing.T) {
	mr, rdb := newTestRedis(t)
	ctx := context.Background()

	now := time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)
	bvs := NewBigVSet(rdb, 30*time.Second)
	bvs.nowFunc = func() time.Time { return now }

	// 1. AddIfExists 在未对账就绪时不得造出残缺集合
	if err := bvs.AddIfExists(ctx, 88); err != nil {
		t.Fatalf("AddIfExists before ready failed: %v", err)
	}
	if mr.Exists(bigVKey) {
		t.Fatal("feed:bigvs must not be created by AddIfExists before reconcile")
	}

	// 2. 零大 V 对账必须保留哨兵 "0" 且永不过期，防空集合击穿
	if err := bvs.Reconcile(ctx, nil); err != nil {
		t.Fatalf("Reconcile empty failed: %v", err)
	}
	if !mr.Exists(bigVKey) {
		t.Fatal("feed:bigvs should exist with sentinel even when 0 bigVs")
	}
	if ttl := mr.TTL(bigVKey); ttl != 0 {
		t.Fatalf("feed:bigvs ttl = %v, want persistent (0)", ttl)
	}

	var dbCalls atomic.Int32
	snap, err := bvs.Snapshot(ctx, func(context.Context) ([]uint64, error) {
		dbCalls.Add(1)
		return []uint64{999}, nil
	})
	if err != nil {
		t.Fatalf("Snapshot empty bigVs failed: %v", err)
	}
	if len(snap) != 0 || dbCalls.Load() != 0 {
		t.Fatalf("expected empty hit from sentinel without db call, got snap=%v dbCalls=%d", snap, dbCalls.Load())
	}

	// 3. 对账写入真实大 V + AddIfExists 增量生效
	if err := bvs.Reconcile(ctx, []uint64{10, 20}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if err := bvs.AddIfExists(ctx, 30); err != nil {
		t.Fatalf("AddIfExists failed: %v", err)
	}
	snap, err = bvs.Snapshot(ctx, func(context.Context) ([]uint64, error) {
		dbCalls.Add(1)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Snapshot after reconcile+add failed: %v", err)
	}
	if len(snap) != 3 {
		t.Fatalf("snap len = %d, want 3 (%v)", len(snap), snap)
	}
	for _, wantID := range []uint64{10, 20, 30} {
		if _, ok := snap[wantID]; !ok {
			t.Fatalf("missing bigV %d in snapshot", wantID)
		}
	}

	// 4. L1 30s 内命中不打 Redis；过期后重读，且 Redis 丢 key 时回源 loader 并回填
	mr.Del(bigVKey)
	snap2, err := bvs.Snapshot(ctx, func(context.Context) ([]uint64, error) {
		dbCalls.Add(1)
		return []uint64{77}, nil
	})
	if err != nil || len(snap2) != 3 || dbCalls.Load() != 0 {
		t.Fatalf("expected L1 cache hit within 30s, got snap=%v err=%v dbCalls=%d", snap2, err, dbCalls.Load())
	}

	now = now.Add(31 * time.Second)
	snap3, err := bvs.Snapshot(ctx, func(context.Context) ([]uint64, error) {
		dbCalls.Add(1)
		return []uint64{77}, nil
	})
	if err != nil || dbCalls.Load() != 1 {
		t.Fatalf("expected fallback loader after L1 expire, err=%v dbCalls=%d", err, dbCalls.Load())
	}
	if _, ok := snap3[77]; !ok || len(snap3) != 1 {
		t.Fatalf("unexpected fallback snapshot: %v", snap3)
	}
	if !mr.Exists(bigVKey) {
		t.Fatal("expected fallback loader to backfill feed:bigvs")
	}
}

func TestFollowingFeedSentinelTrimWatermarkAndPipeline(t *testing.T) {
	mr, rdb := newTestRedis(t)
	ctx := context.Background()

	ff := &FollowingFeed{
		rdb:       rdb,
		inboxMax:  3,
		outboxMax: 2,
		ttl:       time.Hour,
	}

	// 1. 冷粉丝收件箱不存在：条件追加必须跳过，绝不造出僵尸残缺收件箱
	if err := ff.BatchPushInboxIfExists(ctx, []uint64{101, 102}, 900); err != nil {
		t.Fatalf("BatchPushInboxIfExists on cold users failed: %v", err)
	}
	if mr.Exists(inboxKey(101)) || mr.Exists(inboxKey(102)) {
		t.Fatal("cold inbox must not be created by fanout push")
	}

	// 2. 空重建（作者未发帖）也必须写入哨兵 "0"，避免穿透且 FloorID=0
	if err := ff.RebuildInbox(ctx, 101, nil); err != nil {
		t.Fatalf("RebuildInbox empty failed: %v", err)
	}
	gotEmpty, err := ff.ReadInbox(ctx, 101, 0, 10)
	if err != nil {
		t.Fatalf("ReadInbox empty failed: %v", err)
	}
	if !gotEmpty.Exists || len(gotEmpty.IDs) != 0 || gotEmpty.FloorID != 0 {
		t.Fatalf("unexpected empty inbox read: %+v", gotEmpty)
	}

	// 3. 追加 4 条超过 inboxMax=3：rank 0 哨兵必须完好保留，最老的 #10 被裁剪，FloorID=#20
	for _, wpID := range []uint64{10, 20, 30, 40} {
		if err := ff.BatchPushInboxIfExists(ctx, []uint64{101, 102}, wpID); err != nil {
			t.Fatalf("BatchPushInboxIfExists(%d) failed: %v", wpID, err)
		}
	}
	if mr.Exists(inboxKey(102)) {
		t.Fatal("inactive user 102 should still have no inbox")
	}

	// 验证哨兵 "0" 仍在 rank 0
	rank0, err := rdb.ZRange(ctx, inboxKey(101), 0, 0).Result()
	if err != nil || len(rank0) != 1 || rank0[0] != FeedSentinel {
		t.Fatalf("sentinel evicted! rank0 = %v, err = %v", rank0, err)
	}
	gotFull, err := ff.ReadInbox(ctx, 101, 0, 10)
	if err != nil {
		t.Fatalf("ReadInbox full failed: %v", err)
	}
	wantIDs := []uint64{40, 30, 20}
	if !gotFull.Exists || !slices.Equal(gotFull.IDs, wantIDs) || gotFull.FloorID != 20 {
		t.Fatalf("got %+v, want ids=%v floor=20", gotFull, wantIDs)
	}

	// 游标翻页 < 40，排除哨兵
	gotCursor, err := ff.ReadInbox(ctx, 101, 40, 2)
	if err != nil {
		t.Fatalf("ReadInbox cursor failed: %v", err)
	}
	if !slices.Equal(gotCursor.IDs, []uint64{30, 20}) || gotCursor.FloorID != 20 {
		t.Fatalf("cursor read mismatch: %+v", gotCursor)
	}

	// 4. 大 V 发件箱 Pipeline 读取：201 未满（FloorID=0）、202 已满裁剪（FloorID=500）、203 冷未命中
	if err := ff.RebuildOutbox(ctx, 201, []uint64{100}); err != nil {
		t.Fatalf("RebuildOutbox 201 failed: %v", err)
	}
	if err := ff.RebuildOutbox(ctx, 202, []uint64{400, 500, 600}); err != nil {
		t.Fatalf("RebuildOutbox 202 failed: %v", err)
	}
	// 对已存在的 202 条件追加 #700 -> 裁剪 #500，剩 [700, 600]，FloorID=600
	if err := ff.PushOutboxIfExists(ctx, 202, 700); err != nil {
		t.Fatalf("PushOutboxIfExists 202 failed: %v", err)
	}
	// 对冷大 V 203 条件追加必须跳过
	if err := ff.PushOutboxIfExists(ctx, 203, 800); err != nil {
		t.Fatalf("PushOutboxIfExists 203 failed: %v", err)
	}

	outboxes, err := ff.ReadOutboxes(ctx, []uint64{201, 202, 203}, 0, 5)
	if err != nil {
		t.Fatalf("ReadOutboxes failed: %v", err)
	}
	if !outboxes[201].Exists || !slices.Equal(outboxes[201].IDs, []uint64{100}) || outboxes[201].FloorID != 0 {
		t.Fatalf("outbox 201 mismatch: %+v", outboxes[201])
	}
	if !outboxes[202].Exists || !slices.Equal(outboxes[202].IDs, []uint64{700, 600}) || outboxes[202].FloorID != 600 {
		t.Fatalf("outbox 202 mismatch: %+v", outboxes[202])
	}
	if outboxes[203].Exists {
		t.Fatalf("cold outbox 203 should not exist: %+v", outboxes[203])
	}

	// 5. RemoveFromOutbox 不删哨兵；DeleteInbox 彻底移除收件箱
	if err := ff.RemoveFromOutbox(ctx, 201, 100); err != nil {
		t.Fatalf("RemoveFromOutbox failed: %v", err)
	}
	afterRem, err := ff.ReadOutboxes(ctx, []uint64{201}, 0, 5)
	if err != nil || !afterRem[201].Exists || len(afterRem[201].IDs) != 0 {
		t.Fatalf("outbox 201 should remain exists with sentinel after removing last wp: %+v, err=%v", afterRem[201], err)
	}

	if err := ff.DeleteInbox(ctx, 101); err != nil {
		t.Fatalf("DeleteInbox failed: %v", err)
	}
	afterDel, err := ff.ReadInbox(ctx, 101, 0, 5)
	if err != nil || afterDel.Exists {
		t.Fatalf("inbox 101 should not exist after delete: %+v, err=%v", afterDel, err)
	}
}
