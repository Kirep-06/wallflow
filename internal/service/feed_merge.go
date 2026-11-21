package service

import (
	"cmp"
	"slices"
)

// TimelineBucket 表示关注流多路聚合中的一路热数据（普通作者收件箱或单个大 V 发件箱）。
type TimelineBucket struct {
	AuthorID  uint64   // 0 表示普通作者汇总收件箱（inbox）；>0 表示大 V 作者 ID
	IDs       []uint64 // 该路热窗口内 id < before 的倒序候选帖子 ID
	SafeFloor uint64   // 可靠下界：0 表示全量历史在热窗口内；>0 表示小于 SafeFloor 的历史可能已被裁剪
}

// ColdOutboxTarget 描述需要单路冷补查的大 V 目标及查询上界。
type ColdOutboxTarget struct {
	AuthorID   uint64
	ColdBefore uint64 // 查询条件 id < ColdBefore（0 表示无上界）
}

// ColdBackfillPlan 描述精准单路冷补查计划。
type ColdBackfillPlan struct {
	NeedInbox       bool
	InboxColdBefore uint64
	OutboxTargets   []ColdOutboxTarget // 按 AuthorID 升序稳定输出
}

// Empty 返回是否存在任何需要回源 MySQL 冷补查的路。
func (p ColdBackfillPlan) Empty() bool {
	return !p.NeedInbox && len(p.OutboxTargets) == 0
}

// ComputeSafeFloor 根据 Redis 热窗口状态计算可靠下界：
// 热窗口未满（真实帖子数 card-1 < hotSize）说明全量历史均在缓存中，safeFloor = 0；
// 热窗口已满说明 < minID 的历史帖子可能已被裁剪，safeFloor = minID。
func ComputeSafeFloor(exists bool, card int64, hotSize int, minID uint64) uint64 {
	if !exists || hotSize <= 0 {
		return 0
	}
	realCount := card - 1 // 扣除哨兵成员 "0"
	if realCount < int64(hotSize) {
		return 0
	}
	return minID
}

// MergeDescUnique 将多路 ID 列表去重（忽略 0）并按 post_id 降序排序；limit > 0 时截断前 limit 条。
func MergeDescUnique(lists [][]uint64, limit int) []uint64 {
	total := 0
	for _, list := range lists {
		total += len(list)
	}
	if total == 0 {
		return []uint64{}
	}
	all := make([]uint64, 0, total)
	for _, list := range lists {
		for _, id := range list {
			if id > 0 {
				all = append(all, id)
			}
		}
	}
	if len(all) == 0 {
		return []uint64{}
	}
	slices.SortFunc(all, func(a, b uint64) int { return cmp.Compare(b, a) })

	capHint := len(all)
	if limit > 0 && limit < capHint {
		capHint = limit
	}
	out := make([]uint64, 0, capHint)
	var prev uint64
	for _, id := range all {
		if id == prev {
			continue
		}
		prev = id
		out = append(out, id)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// MergeAndPlanColdBackfill 执行关注流冷热衔接核心算法：
//  1. 将各路热候选集去重按 post_id DESC 归并得到 cand；
//  2. 计算截断线 cutoff（len(cand) >= want 时为 cand[want-1]，否则为 0）；
//  3. 仅筛选出 safeFloor > cutoff 的越界路（cutoff == 0 时要求 safeFloor > 0），
//     并给出每路精准冷查上界 coldBefore = minPositive(before, safeFloor)。
func MergeAndPlanColdBackfill(buckets []TimelineBucket, before uint64, want int) ([]uint64, ColdBackfillPlan) {
	lists := make([][]uint64, 0, len(buckets))
	for _, b := range buckets {
		if len(b.IDs) > 0 {
			lists = append(lists, b.IDs)
		}
	}
	cand := MergeDescUnique(lists, 0)

	var cutoff uint64
	if want > 0 && len(cand) >= want {
		cutoff = cand[want-1]
	}

	var plan ColdBackfillPlan
	seenOutbox := make(map[uint64]struct{})
	for _, b := range buckets {
		if b.SafeFloor == 0 || b.SafeFloor <= cutoff {
			continue
		}
		coldBefore := minPositive(before, b.SafeFloor)
		if b.AuthorID == 0 {
			if !plan.NeedInbox || coldBefore > plan.InboxColdBefore {
				plan.NeedInbox = true
				plan.InboxColdBefore = coldBefore
			}
			continue
		}
		if _, dup := seenOutbox[b.AuthorID]; dup {
			continue
		}
		seenOutbox[b.AuthorID] = struct{}{}
		plan.OutboxTargets = append(plan.OutboxTargets, ColdOutboxTarget{
			AuthorID:   b.AuthorID,
			ColdBefore: coldBefore,
		})
	}
	slices.SortFunc(plan.OutboxTargets, func(a, b ColdOutboxTarget) int {
		return cmp.Compare(a.AuthorID, b.AuthorID)
	})
	return cand, plan
}

// minPositive 返回两个非负整数中的较小正值；若其一为 0 则返回另一值。
func minPositive(a, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	return min(a, b)
}

// filterBeforeLimit 从已按降序排列的 ID 切片中筛出 id < before（before==0 表示无上界）的前 limit 条。
func filterBeforeLimit(sortedDesc []uint64, before uint64, limit int) []uint64 {
	if len(sortedDesc) == 0 || limit <= 0 {
		return []uint64{}
	}
	out := make([]uint64, 0, min(len(sortedDesc), limit))
	for _, id := range sortedDesc {
		if id == 0 {
			continue
		}
		if before > 0 && id >= before {
			continue
		}
		out = append(out, id)
		if len(out) >= limit {
			break
		}
	}
	return out
}
