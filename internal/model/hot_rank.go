package model

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"
)

const (
	// 全站热门榜 Top 100 候选扫描与重力时间衰减参数
	HotRankMaxSize     = 100
	HotRankWindow      = 30 * 24 * time.Hour
	HotRankBatchSize   = 1000
	HotRankMaxScan     = 20000
	HotRankCutoffGrace = time.Minute

	hotWeightDownload = 5.0
	hotWeightFav      = 3.0
	hotWeightLike     = 2.0
	hotWeightComment  = 2.0
	hotScoreBase      = 1.0
	hotTimeOffsetHrs  = 2.0
	hotGravityExp     = 1.5
)

// HotRankCandidate 为热门榜算分的轻量只读扫描投影（wallpapers + wallpaper_stats）。
type HotRankCandidate struct {
	ID            uint64
	CreatedAt     time.Time
	LikeCount     uint64
	FavCount      uint64
	DownloadCount uint64
	CommentCount  uint64
}

// HotRankEntry 表示热门榜中的单条壁纸及其重力时间衰减热度分。
type HotRankEntry struct {
	WallpaperID uint64
	Score       float64
}

// CalcHotScore 按重力时间衰减公式计算壁纸热度分（始终返回有限正数，避免与哨兵 0 冲突）。
func CalcHotScore(c HotRankCandidate, now time.Time) float64 {
	numerator := float64(c.DownloadCount)*hotWeightDownload +
		float64(c.FavCount)*hotWeightFav +
		float64(c.LikeCount)*hotWeightLike +
		float64(c.CommentCount)*hotWeightComment +
		hotScoreBase
	ageHours := max(0.0, now.Sub(c.CreatedAt).Hours())
	score := numerator / math.Pow(ageHours+hotTimeOffsetHrs, hotGravityExp)
	if math.IsNaN(score) || math.IsInf(score, 0) || score <= 0 {
		return math.SmallestNonzeroFloat64
	}
	return score
}

// SortHotRankEntries 按 (Score DESC, WallpaperID DESC) 执行确定性原地排序。
func SortHotRankEntries(entries []HotRankEntry) {
	slices.SortFunc(entries, func(a, b HotRankEntry) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return cmp.Compare(b.WallpaperID, a.WallpaperID)
	})
}

// ComputeHotTopN 分批扫描近 30 天正常壁纸（不足 Top 100 自动冷启动向历史补齐，上限 20000 行熔断），
// 在恒定内存内按重力时间衰减公式归约输出 Top 100。
func ComputeHotTopN(
	ctx context.Context,
	fetchBatch func(ctx context.Context, beforeID uint64, limit int) ([]HotRankCandidate, error),
	now time.Time,
) ([]HotRankEntry, error) {
	return computeHotTopN(ctx, fetchBatch, now, HotRankMaxSize, HotRankBatchSize, HotRankMaxScan)
}

func computeHotTopN(
	ctx context.Context,
	fetchBatch func(ctx context.Context, beforeID uint64, limit int) ([]HotRankCandidate, error),
	now time.Time,
	maxSize, batchSize, maxScan int,
) ([]HotRankEntry, error) {
	if fetchBatch == nil || maxSize <= 0 || batchSize <= 0 || maxScan <= 0 {
		return []HotRankEntry{}, nil
	}

	cutoff := now.Add(-HotRankWindow)
	stopCutoff := cutoff.Add(-HotRankCutoffGrace)

	recentTop := make([]HotRankEntry, 0, maxSize+batchSize)
	olderTop := make([]HotRankEntry, 0, maxSize+batchSize)

	var beforeID uint64
	scanned := 0

	for scanned < maxScan {
		want := min(batchSize, maxScan-scanned)
		rows, err := fetchBatch(ctx, beforeID, want)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}

		scanned += len(rows)
		for _, row := range rows {
			if row.ID == 0 {
				continue
			}
			entry := HotRankEntry{
				WallpaperID: row.ID,
				Score:       CalcHotScore(row, now),
			}
			if !row.CreatedAt.Before(cutoff) {
				recentTop = append(recentTop, entry)
			} else if len(recentTop) < maxSize {
				olderTop = append(olderTop, entry)
			}
		}

		if len(recentTop) > maxSize {
			SortHotRankEntries(recentTop)
			recentTop = recentTop[:maxSize]
		}
		if len(recentTop) >= maxSize {
			olderTop = olderTop[:0]
		} else if len(olderTop) > maxSize {
			SortHotRankEntries(olderTop)
			olderTop = olderTop[:maxSize]
		}

		last := rows[len(rows)-1]
		if len(rows) < want || last.ID <= 1 || (beforeID > 0 && last.ID >= beforeID) {
			break
		}
		if last.CreatedAt.Before(stopCutoff) && len(recentTop)+len(olderTop) >= maxSize {
			break
		}
		beforeID = last.ID
	}

	result := recentTop
	if len(result) < maxSize && len(olderTop) > 0 {
		result = append(result, olderTop...)
	}
	SortHotRankEntries(result)
	if len(result) > maxSize {
		result = result[:maxSize]
	}
	return result, nil
}

// SliceHotPage 按 (Score DESC, WallpaperID DESC) 拷贝排序后，基于可选复合游标截取至多 limit 条并返回是否有下一页。
func SliceHotPage(entries []HotRankEntry, afterScore float64, afterID uint64, hasCursor bool, limit int) ([]HotRankEntry, bool) {
	if len(entries) == 0 || limit <= 0 {
		return []HotRankEntry{}, false
	}

	sorted := slices.Clone(entries)
	SortHotRankEntries(sorted)

	start := 0
	if hasCursor {
		for start < len(sorted) {
			e := sorted[start]
			if e.Score > afterScore || (e.Score == afterScore && e.WallpaperID >= afterID) {
				start++
				continue
			}
			break
		}
	}

	rem := sorted[start:]
	if len(rem) > limit {
		return rem[:limit], true
	}
	return rem, false
}
