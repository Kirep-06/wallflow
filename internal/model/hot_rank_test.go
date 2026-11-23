package model

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"
)

func TestCalcHotScore(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	t.Run("zero interaction stays strictly positive and decays by age", func(t *testing.T) {
		fresh := CalcHotScore(HotRankCandidate{ID: 2, CreatedAt: now}, now)
		dayOld := CalcHotScore(HotRankCandidate{ID: 1, CreatedAt: now.Add(-24 * time.Hour)}, now)

		wantFresh := 1.0 / math.Pow(2.0, 1.5)
		if math.Abs(fresh-wantFresh) > 1e-12 {
			t.Fatalf("fresh score = %v, want %v", fresh, wantFresh)
		}
		if !(fresh > dayOld && dayOld > 0) {
			t.Fatalf("expected fresh (%v) > dayOld (%v) > 0", fresh, dayOld)
		}
	})

	t.Run("negative clock skew clamped to zero age without NaN", func(t *testing.T) {
		future := CalcHotScore(HotRankCandidate{
			ID:        10,
			CreatedAt: now.Add(5 * time.Hour),
			LikeCount: 1,
		}, now)
		want := 3.0 / math.Pow(2.0, 1.5)
		if math.IsNaN(future) || math.IsInf(future, 0) || math.Abs(future-want) > 1e-12 {
			t.Fatalf("future score = %v, want %v", future, want)
		}
	})

	t.Run("recent quality wallpaper outranks 7d 20x interaction old hit under G=1.5", func(t *testing.T) {
		recent := CalcHotScore(HotRankCandidate{
			ID:            200,
			CreatedAt:     now.Add(-6 * time.Hour),
			DownloadCount: 10,
			FavCount:      5,
			LikeCount:     10,
			CommentCount:  2,
		}, now)
		oldHit := CalcHotScore(HotRankCandidate{
			ID:            100,
			CreatedAt:     now.Add(-168 * time.Hour),
			DownloadCount: 200,
			FavCount:      100,
			LikeCount:     200,
			CommentCount:  30,
		}, now)
		if recent <= oldHit {
			t.Fatalf("expected recent (%v) > oldHit (%v)", recent, oldHit)
		}
	})
}

func TestComputeHotTopN(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	t.Run("early stops after crossing 30d window once maxSize satisfied", func(t *testing.T) {
		// 构造 12 条倒序数据：前 5 条在 30 天内，后 7 条在 40 天前；maxSize=3, batchSize=4。
		all := make([]HotRankCandidate, 0, 12)
		for i := 12; i >= 1; i-- {
			age := 40 * 24 * time.Hour
			if i >= 8 {
				age = time.Duration(13-i) * time.Hour
			}
			all = append(all, HotRankCandidate{
				ID:            uint64(i),
				CreatedAt:     now.Add(-age),
				DownloadCount: uint64(i),
			})
		}

		batches := 0
		got, err := computeHotTopN(context.Background(), func(_ context.Context, beforeID uint64, limit int) ([]HotRankCandidate, error) {
			batches++
			var out []HotRankCandidate
			for _, c := range all {
				if beforeID > 0 && c.ID >= beforeID {
					continue
				}
				out = append(out, c)
				if len(out) == limit {
					break
				}
			}
			return out, nil
		}, now, 3, 4, 100)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if batches != 2 {
			t.Fatalf("expected early stop at batch 2, got %d batches", batches)
		}
		if len(got) != 3 || got[0].WallpaperID != 12 || got[1].WallpaperID != 11 || got[2].WallpaperID != 10 {
			t.Fatalf("unexpected top 3: %+v", got)
		}
	})

	t.Run("cold start auto backfills older wallpapers when 30d pool smaller than maxSize", func(t *testing.T) {
		all := []HotRankCandidate{
			{ID: 5, CreatedAt: now.Add(-2 * time.Hour), LikeCount: 2},
			{ID: 4, CreatedAt: now.Add(-40 * 24 * time.Hour), LikeCount: 1},
			{ID: 3, CreatedAt: now.Add(-41 * 24 * time.Hour), LikeCount: 10},
			{ID: 2, CreatedAt: now.Add(-42 * 24 * time.Hour)},
			{ID: 1, CreatedAt: now.Add(-43 * 24 * time.Hour)},
		}

		got, err := computeHotTopN(context.Background(), func(_ context.Context, beforeID uint64, limit int) ([]HotRankCandidate, error) {
			var out []HotRankCandidate
			for _, c := range all {
				if beforeID > 0 && c.ID >= beforeID {
					continue
				}
				out = append(out, c)
				if len(out) == limit {
					break
				}
			}
			return out, nil
		}, now, 3, 2, 100)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("expected cold start backfilled to 3, got %d: %+v", len(got), got)
		}
		if got[0].WallpaperID != 5 || got[1].WallpaperID != 3 || got[2].WallpaperID != 4 {
			t.Fatalf("unexpected cold start ranking: %+v", got)
		}
	})

	t.Run("maxScan circuit breaker caps scanned rows during upload spike", func(t *testing.T) {
		scanned := 0
		got, err := computeHotTopN(context.Background(), func(_ context.Context, beforeID uint64, limit int) ([]HotRankCandidate, error) {
			start := uint64(50)
			if beforeID > 0 {
				start = beforeID - 1
			}
			out := make([]HotRankCandidate, 0, limit)
			for i := 0; i < limit && start > 0; i++ {
				out = append(out, HotRankCandidate{
					ID:        start,
					CreatedAt: now.Add(-time.Hour),
					LikeCount: start,
				})
				start--
			}
			scanned += len(out)
			return out, nil
		}, now, 3, 4, 10)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if scanned != 10 {
			t.Fatalf("expected scanned capped at 10, got %d", scanned)
		}
		if len(got) != 3 || got[0].WallpaperID != 50 || got[2].WallpaperID != 48 {
			t.Fatalf("unexpected capped top 3: %+v", got)
		}
	})
}

func TestSliceHotPage_TieBreakAndFullPagination(t *testing.T) {
	// 刻意打乱输入并包含相同分值（且覆盖字典序陷阱 ID=10 vs ID=9）
	input := []HotRankEntry{
		{WallpaperID: 9, Score: 1.5},
		{WallpaperID: 10, Score: 1.5},
		{WallpaperID: 8, Score: 1.5},
		{WallpaperID: 100, Score: 3.0},
		{WallpaperID: 7, Score: 1.5},
		{WallpaperID: 2, Score: 0.5},
	}
	orig := slices.Clone(input)

	var collected []uint64
	var afterScore float64
	var afterID uint64
	hasCur := false

	for range 10 {
		page, hasMore := SliceHotPage(input, afterScore, afterID, hasCur, 2)
		for _, e := range page {
			collected = append(collected, e.WallpaperID)
		}
		if !hasMore {
			break
		}
		last := page[len(page)-1]
		afterScore, afterID, hasCur = last.Score, last.WallpaperID, true
	}

	want := []uint64{100, 10, 9, 8, 7, 2}
	if !slices.Equal(collected, want) {
		t.Fatalf("paginated ids = %v, want %v", collected, want)
	}
	if !slices.Equal(input, orig) {
		t.Fatalf("SliceHotPage must not mutate caller slice")
	}
}
