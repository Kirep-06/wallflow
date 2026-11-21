package repository

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/config"
)

func newTestFollowingFeedRepo(t *testing.T, inboxHot, outboxHot int) (*miniredis.Miniredis, *redis.Client, *FollowingFeedRepo) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewFollowingFeedRepo(rdb, config.FollowingFeedConfig{
		InboxHotSize:     inboxHot,
		OutboxHotSize:    outboxHot,
		InboxTTLSeconds:  3600,
		OutboxTTLSeconds: 3600,
	})
	return mr, rdb, repo
}

func TestFollowingFeedRepo_AddTimelineIfExistsAndSentinelProtection(t *testing.T) {
	ctx := context.Background()
	_, rdb, repo := newTestFollowingFeedRepo(t, 3, 2)

	// 1. Key 不存在时：条件追加返回 false 且绝不创建残缺收件箱。
	written, err := repo.AddToInboxIfExists(ctx, 1001, 10)
	if err != nil {
		t.Fatalf("AddToInboxIfExists on missing key: %v", err)
	}
	if written {
		t.Fatal("expected written=false when inbox does not exist")
	}
	if exists, err := rdb.Exists(ctx, InboxKey(1001)).Result(); err != nil || exists != 0 {
		t.Fatalf("missing inbox must not be created: exists=%d err=%v", exists, err)
	}

	// 2. 空收件箱初始化：写入哨兵 "0" 防穿透，TTL 为正。
	if err := repo.ReplaceInbox(ctx, 1001, nil); err != nil {
		t.Fatalf("ReplaceInbox empty: %v", err)
	}
	rr, err := repo.RangeInbox(ctx, 1001, 0, 10)
	if err != nil {
		t.Fatalf("RangeInbox empty: %v", err)
	}
	if !rr.Exists || len(rr.IDs) != 0 || rr.Card != 1 || rr.MinID != 0 {
		t.Fatalf("unexpected empty inbox range result: %+v", rr)
	}
	if ttl, err := rdb.TTL(ctx, InboxKey(1001)).Result(); err != nil || ttl <= 0 {
		t.Fatalf("expected positive TTL on inbox, got %v err=%v", ttl, err)
	}

	// 3. 连续追加超过 hot_size=3：裁剪最旧真实帖子 10、20，保留 30、40、50 且哨兵 "0" 始终位于 rank 0。
	for _, pid := range []uint64{10, 20, 30, 40, 50} {
		ok, err := repo.AddToInboxIfExists(ctx, 1001, pid)
		if err != nil || !ok {
			t.Fatalf("AddToInboxIfExists(%d): ok=%v err=%v", pid, ok, err)
		}
	}
	rawMembers, err := rdb.ZRange(ctx, InboxKey(1001), 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange raw: %v", err)
	}
	wantRaw := []string{"0", "30", "40", "50"}
	if !slices.Equal(rawMembers, wantRaw) {
		t.Fatalf("raw members mismatch (sentinel must stay at rank 0): got %v, want %v", rawMembers, wantRaw)
	}

	// 4. Range 排除哨兵 "0"，正确返回 Card（含哨兵=4）、MinID（rank 1 = 30）与倒序 ID。
	rr, err = repo.RangeInbox(ctx, 1001, 0, 10)
	if err != nil {
		t.Fatalf("RangeInbox full: %v", err)
	}
	if !rr.Exists || rr.Card != 4 || rr.MinID != 30 || !slices.Equal(rr.IDs, []uint64{50, 40, 30}) {
		t.Fatalf("unexpected full inbox range result: %+v", rr)
	}

	// 5. 带 beforeID 游标分页读取。
	rrBefore, err := repo.RangeInbox(ctx, 1001, 50, 10)
	if err != nil {
		t.Fatalf("RangeInbox before 50: %v", err)
	}
	if !slices.Equal(rrBefore.IDs, []uint64{40, 30}) {
		t.Fatalf("before 50 mismatch: got %v, want [40 30]", rrBefore.IDs)
	}
}

func TestFollowingFeedRepo_BatchFanoutAndCombinedRange(t *testing.T) {
	ctx := context.Background()
	_, rdb, repo := newTestFollowingFeedRepo(t, 2, 2)

	// 活跃粉丝 2001 已建箱，僵尸粉丝 2002 未建箱；大 V 9001 已建发件箱，大 V 9002 未建。
	if err := repo.ReplaceInbox(ctx, 2001, []uint64{100, 90}); err != nil {
		t.Fatal(err)
	}
	if err := repo.ReplaceOutbox(ctx, 9001, []uint64{105, 95}); err != nil {
		t.Fatal(err)
	}

	// 批量扇出帖子 110：只写入 2001 并裁剪最旧的 90，绝不为僵尸粉丝 2002 建 Key。
	if err := repo.BatchAddToInboxesIfExists(ctx, []uint64{2001, 2002}, 110); err != nil {
		t.Fatalf("BatchAddToInboxesIfExists: %v", err)
	}
	if exists, err := rdb.Exists(ctx, InboxKey(2002)).Result(); err != nil || exists != 0 {
		t.Fatalf("zombie follower inbox must not exist: exists=%d err=%v", exists, err)
	}

	inboxRes, outboxes, err := repo.RangeInboxAndOutboxes(ctx, 2001, true, []uint64{9001, 9002}, 0, 5)
	if err != nil {
		t.Fatalf("RangeInboxAndOutboxes: %v", err)
	}
	if !inboxRes.Exists || inboxRes.Card != 3 || inboxRes.MinID != 100 || !slices.Equal(inboxRes.IDs, []uint64{110, 100}) {
		t.Fatalf("unexpected active follower inbox: %+v", inboxRes)
	}
	if !outboxes[9001].Exists || outboxes[9001].MinID != 95 || !slices.Equal(outboxes[9001].IDs, []uint64{105, 95}) {
		t.Fatalf("unexpected bigv 9001 outbox: %+v", outboxes[9001])
	}
	if outboxes[9002].Exists {
		t.Fatalf("expected bigv 9002 outbox missing, got %+v", outboxes[9002])
	}
	if exists, err := rdb.Exists(ctx, OutboxKey(9002)).Result(); err != nil || exists != 0 {
		t.Fatalf("range must not create missing outbox key: exists=%d err=%v", exists, err)
	}
}

func TestFollowingFeedRepo_BigVsSentinelGuard(t *testing.T) {
	ctx := context.Background()
	_, _, repo := newTestFollowingFeedRepo(t, 5, 5)

	// 未初始化前：GetBigVs 返回 initialized=false，AddBigVIfExists 不应写入残缺集合。
	_, init, err := repo.GetBigVs(ctx)
	if err != nil || init {
		t.Fatalf("expected uninitialized bigvs, got init=%v err=%v", init, err)
	}
	added, err := repo.AddBigVIfExists(ctx, 888)
	if err != nil || added {
		t.Fatalf("AddBigVIfExists before init should skip: added=%v err=%v", added, err)
	}

	// 空列表初始化（0 个大 V）仍应通过哨兵 "0" 标记为已初始化。
	if err := repo.ReplaceBigVs(ctx, nil); err != nil {
		t.Fatalf("ReplaceBigVs empty: %v", err)
	}
	ids, init, err := repo.GetBigVs(ctx)
	if err != nil || !init || len(ids) != 0 {
		t.Fatalf("expected initialized empty bigvs, got ids=%v init=%v err=%v", ids, init, err)
	}

	// 初始化后晋升新大 V 应立即写入集合。
	added, err = repo.AddBigVIfExists(ctx, 888)
	if err != nil || !added {
		t.Fatalf("AddBigVIfExists after init failed: added=%v err=%v", added, err)
	}
	ids, init, err = repo.GetBigVs(ctx)
	if err != nil || !init {
		t.Fatalf("GetBigVs after add: init=%v err=%v", init, err)
	}
	if _, ok := ids[888]; !ok || len(ids) != 1 {
		t.Fatalf("expected bigv set {888}, got %v", ids)
	}
}
