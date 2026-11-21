package service

import (
	"slices"
	"testing"
)

func TestMergeAndPlanColdBackfill_AllHotNoBackfill(t *testing.T) {
	buckets := []TimelineBucket{
		{AuthorID: 0, IDs: []uint64{100, 90, 80}, SafeFloor: 0},
		{AuthorID: 11, IDs: []uint64{95, 90, 75}, SafeFloor: 0}, // 含与 inbox 重复的 90（刚晋升大 V 过渡）
		{AuthorID: 22, IDs: []uint64{85, 70}, SafeFloor: 0},
	}

	cand, plan := MergeAndPlanColdBackfill(buckets, 0, 5)
	wantCand := []uint64{100, 95, 90, 85, 80, 75, 70}
	if !slices.Equal(cand, wantCand) {
		t.Fatalf("cand mismatch: got %v, want %v", cand, wantCand)
	}
	if !plan.Empty() {
		t.Fatalf("expected no cold backfill when all safeFloors are 0, got %+v", plan)
	}
}

func TestMergeAndPlanColdBackfill_SingleHighVolumeBigV(t *testing.T) {
	// 高产大 V 11 热窗口已满，minID=88（safeFloor=88）；
	// 普通收件箱 safeFloor=50，大 V 22 未满 safeFloor=0。
	// want=5 时 cand=[100, 98, 95, 90, 88, 86, 80, 70]，cutoff=cand[4]=88 -> 不越界；
	// want=6 时 cutoff=cand[5]=86 -> 仅大 V 11 (88 > 86) 越界，不惊动 inbox 和大 V 22。
	buckets := []TimelineBucket{
		{AuthorID: 0, IDs: []uint64{100, 86, 80, 70}, SafeFloor: 50},
		{AuthorID: 11, IDs: []uint64{98, 95, 88}, SafeFloor: 88},
		{AuthorID: 22, IDs: []uint64{90}, SafeFloor: 0},
	}

	_, plan5 := MergeAndPlanColdBackfill(buckets, 0, 5)
	if !plan5.Empty() {
		t.Fatalf("want=5 should not trigger cold backfill (safeFloor 88 == cutoff 88), got %+v", plan5)
	}

	_, plan6 := MergeAndPlanColdBackfill(buckets, 0, 6)
	if plan6.NeedInbox {
		t.Fatalf("inbox should not be backfilled (safeFloor 50 <= cutoff 86), got %+v", plan6)
	}
	wantTargets := []ColdOutboxTarget{{AuthorID: 11, ColdBefore: 88}}
	if !slices.Equal(plan6.OutboxTargets, wantTargets) {
		t.Fatalf("outbox targets mismatch: got %+v, want %+v", plan6.OutboxTargets, wantTargets)
	}
}

func TestMergeAndPlanColdBackfill_OnlyInboxOverflows(t *testing.T) {
	// before=75 翻页时，inbox 热窗口 minID=70（safeFloor=70），大 V 11 热窗口未满（safeFloor=0）。
	// cand=[74, 70, 60, 50], want=4 -> cutoff=50，仅 inbox 越界且 coldBefore = min(75, 70) = 70。
	buckets := []TimelineBucket{
		{AuthorID: 0, IDs: []uint64{74, 70}, SafeFloor: 70},
		{AuthorID: 11, IDs: []uint64{60, 50}, SafeFloor: 0},
	}

	_, plan := MergeAndPlanColdBackfill(buckets, 75, 4)
	if !plan.NeedInbox || plan.InboxColdBefore != 70 {
		t.Fatalf("expected inbox backfill before 70, got %+v", plan)
	}
	if len(plan.OutboxTargets) != 0 {
		t.Fatalf("expected no outbox backfill, got %+v", plan.OutboxTargets)
	}
}

func TestMergeAndPlanColdBackfill_DeepCursorBelowSafeFloor(t *testing.T) {
	// 深页 before=40 已低于大 V 11 的热窗口 minID=120，Redis 对大 V 11 返回空切片；
	// 必须精准以 coldBefore = minPositive(40, 120) = 40 补查大 V 11，避免漏帖或重查 40..120。
	buckets := []TimelineBucket{
		{AuthorID: 0, IDs: []uint64{39, 38, 37, 36, 35}, SafeFloor: 0},
		{AuthorID: 11, IDs: nil, SafeFloor: 120},
	}

	_, plan := MergeAndPlanColdBackfill(buckets, 40, 5)
	if plan.NeedInbox {
		t.Fatalf("inbox should not backfill: %+v", plan)
	}
	wantTargets := []ColdOutboxTarget{{AuthorID: 11, ColdBefore: 40}}
	if !slices.Equal(plan.OutboxTargets, wantTargets) {
		t.Fatalf("outbox targets mismatch: got %+v, want %+v", plan.OutboxTargets, wantTargets)
	}
}

// TestMergeAndColdBackfill_MultiPageExactParity 验证极小热窗口下混合普通作者与多个高产大 V 连续翻页至底，
// 与全量真实排序结果逐条 100% 一致（无重复、无遗漏、无错序）。
func TestMergeAndColdBackfill_MultiPageExactParity(t *testing.T) {
	const (
		inboxHotSize  = 5
		outboxHotSize = 3
		pageLimit     = 4
	)

	// 构造 38 条跨度交错的帖子：普通作者集合(author=0) 与三个大 V(101 高产、102 中产、103 低产未满)
	normalPosts := []uint64{35, 31, 29, 25, 22, 18, 14, 11, 8, 5, 2}
	bigVPosts := map[uint64][]uint64{
		101: {36, 34, 33, 32, 30, 28, 27, 26, 24, 21, 19, 17, 15, 13, 10, 7, 4, 1}, // 高产大 V，远超 outboxHotSize
		102: {23, 20, 16, 12, 9, 6, 3},                                             // 中产大 V，超过 outboxHotSize
		103: {38, 37},                                                              // 低产大 V，总条数 < outboxHotSize(3)，safeFloor=0
	}

	// 计算真实全量倒序结果
	truth := MergeDescUnique([][]uint64{normalPosts, bigVPosts[101], bigVPosts[102], bigVPosts[103]}, 0)

	// 构造各自截断后的 Redis 热窗口
	hotSlice := func(full []uint64, capSize int) ([]uint64, uint64) {
		if len(full) <= capSize {
			return full, 0
		}
		hot := full[:capSize]
		return hot, hot[capSize-1]
	}
	inboxHot, inboxFloor := hotSlice(normalPosts, inboxHotSize)
	outboxHot := make(map[uint64][]uint64)
	outboxFloor := make(map[uint64]uint64)
	for bid, posts := range bigVPosts {
		h, f := hotSlice(posts, outboxHotSize)
		outboxHot[bid] = h
		outboxFloor[bid] = f
	}

	var gotAll []uint64
	var before uint64
	seen := make(map[uint64]bool)

	for page := 0; page < 50; page++ {
		want := pageLimit + 1
		buckets := []TimelineBucket{
			{AuthorID: 0, IDs: filterBeforeLimit(inboxHot, before, want), SafeFloor: inboxFloor},
		}
		for _, bid := range []uint64{101, 102, 103} {
			buckets = append(buckets, TimelineBucket{
				AuthorID:  bid,
				IDs:       filterBeforeLimit(outboxHot[bid], before, want),
				SafeFloor: outboxFloor[bid],
			})
		}

		cand, plan := MergeAndPlanColdBackfill(buckets, before, want)
		var extra [][]uint64
		if plan.NeedInbox {
			extra = append(extra, filterBeforeLimit(normalPosts, plan.InboxColdBefore, want))
		}
		for _, target := range plan.OutboxTargets {
			extra = append(extra, filterBeforeLimit(bigVPosts[target.AuthorID], target.ColdBefore, want))
		}
		finalIDs := cand
		if len(extra) > 0 {
			finalIDs = MergeDescUnique(append([][]uint64{cand}, extra...), want)
		} else if len(finalIDs) > want {
			finalIDs = finalIDs[:want]
		}

		hasMore := len(finalIDs) > pageLimit
		if hasMore {
			finalIDs = finalIDs[:pageLimit]
		}
		for _, id := range finalIDs {
			if seen[id] {
				t.Fatalf("duplicate post id %d on page %d", id, page)
			}
			seen[id] = true
			gotAll = append(gotAll, id)
		}
		if !hasMore {
			break
		}
		before = finalIDs[len(finalIDs)-1]
	}

	if !slices.Equal(gotAll, truth) {
		t.Fatalf("multi-page pagination mismatch:\ngot:  %v\nwant: %v", gotAll, truth)
	}
}
