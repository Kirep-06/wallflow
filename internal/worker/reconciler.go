package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/repository"
)

const (
	// M3 / M5 热窗口与大 V 集合对账周期
	reconcileInterval = 60 * time.Second
	reconcileTimeout  = 10 * time.Second

	// M6 互动计数低频滚动兜底对账参数
	statsReconcileInterval    = 10 * time.Minute
	statsReconcileTimeout     = 30 * time.Second
	statsReconcileBatchSize   = 100
	statsReconcileQuietWindow = 10 * time.Second
)

// runPeriodic 启动立即执行一次任务，随后按固定周期执行直至 ctx 取消。
func runPeriodic(ctx context.Context, interval, timeout time.Duration, fn func(context.Context)) {
	runOnce := func() {
		if ctx.Err() != nil {
			return
		}
		runCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		fn(runCtx)
	}

	runOnce()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// LatestFeedReconciler 负责启动预热并每 60s 将 MySQL 最新壁纸快照原子对账至 Redis feed:latest。
type LatestFeedReconciler struct {
	wallpaperRepo *repository.WallpaperRepository
	latestFeed    *cache.LatestFeed
}

// NewLatestFeedReconciler 构造最新流热窗口周期对账任务。
func NewLatestFeedReconciler(wallpaperRepo *repository.WallpaperRepository, latestFeed *cache.LatestFeed) *LatestFeedReconciler {
	return &LatestFeedReconciler{wallpaperRepo: wallpaperRepo, latestFeed: latestFeed}
}

// Run 启动最新流对账循环直至 ctx 取消。
func (r *LatestFeedReconciler) Run(ctx context.Context) {
	runPeriodic(ctx, reconcileInterval, reconcileTimeout, func(runCtx context.Context) {
		start := time.Now()
		ids, err := r.wallpaperRepo.ListLatestIDs(runCtx, 0, cache.LatestFeedMaxSize)
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(runCtx, "latest feed reconcile query mysql failed", slog.Any("err", err))
			}
			return
		}
		if err := r.latestFeed.Reconcile(runCtx, ids); err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(runCtx, "latest feed reconcile redis swap failed", slog.Int("count", len(ids)), slog.Any("err", err))
			}
			return
		}
		slog.InfoContext(runCtx, "latest feed reconciled", slog.Int("count", len(ids)), slog.Duration("cost", time.Since(start)))
	})
}

// BigVReconciler 负责启动预热并每 60s 将 MySQL 全站大 V 集合原子对账至 Redis feed:bigvs（含哨兵 "0"）。
type BigVReconciler struct {
	followRepo *repository.FollowRepository
	bigVSet    *cache.BigVSet
}

// NewBigVReconciler 构造全站大 V 周期对账任务。
func NewBigVReconciler(followRepo *repository.FollowRepository, bigVSet *cache.BigVSet) *BigVReconciler {
	return &BigVReconciler{followRepo: followRepo, bigVSet: bigVSet}
}

// Run 启动全站大 V 对账循环直至 ctx 取消。
func (r *BigVReconciler) Run(ctx context.Context) {
	runPeriodic(ctx, reconcileInterval, reconcileTimeout, func(runCtx context.Context) {
		start := time.Now()
		ids, err := r.followRepo.ListAllBigVIDs(runCtx)
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(runCtx, "bigv reconcile query mysql failed", slog.Any("err", err))
			}
			return
		}
		if err := r.bigVSet.Reconcile(runCtx, ids); err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(runCtx, "bigv reconcile redis swap failed", slog.Int("count", len(ids)), slog.Any("err", err))
			}
			return
		}
		slog.InfoContext(runCtx, "bigv set reconciled", slog.Int("count", len(ids)), slog.Duration("cost", time.Since(start)))
	})
}

// StatsReconciler 负责低频按主键游标滚动校对 wallpaper_stats 与明细表一致性，
// 结合 10s 冷静窗口与 updated_at 乐观锁 CAS 防止覆写在途热增量（不改动 download_count）。
type StatsReconciler struct {
	wallpaperRepo   *repository.WallpaperRepository
	interactionRepo *repository.InteractionRepository
	rdb             *redis.Client
	cursorID        uint64
}

// NewStatsReconciler 构造互动计数低频兜底对账任务。
func NewStatsReconciler(
	wallpaperRepo *repository.WallpaperRepository,
	interactionRepo *repository.InteractionRepository,
	rdb *redis.Client,
) *StatsReconciler {
	return &StatsReconciler{
		wallpaperRepo:   wallpaperRepo,
		interactionRepo: interactionRepo,
		rdb:             rdb,
	}
}

// Run 启动低频计数对账循环直至 ctx 取消。
func (r *StatsReconciler) Run(ctx context.Context) {
	runPeriodic(ctx, statsReconcileInterval, statsReconcileTimeout, func(runCtx context.Context) {
		r.reconcileOnce(ctx, runCtx)
	})
}

func (r *StatsReconciler) reconcileOnce(parentCtx, runCtx context.Context) {
	start := time.Now()
	ids, err := r.wallpaperRepo.ListActiveIDsAfter(runCtx, r.cursorID, statsReconcileBatchSize)
	if err != nil {
		if parentCtx.Err() == nil {
			slog.WarnContext(runCtx, "stats reconcile list wallpapers failed", slog.Any("err", err))
		}
		return
	}
	if len(ids) == 0 {
		r.cursorID = 0
		return
	}
	if len(ids) < statsReconcileBatchSize {
		r.cursorID = 0
	} else {
		r.cursorID = ids[len(ids)-1]
	}

	existing, err := r.interactionRepo.FindStatsReconcileRows(runCtx, ids)
	if err != nil {
		if parentCtx.Err() == nil {
			slog.WarnContext(runCtx, "stats reconcile load existing stats failed", slog.Any("err", err))
		}
		return
	}

	cutoff := time.Now().UTC().Add(-statsReconcileQuietWindow)
	candidates := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if row, ok := existing[id]; ok && row.UpdatedAt.After(cutoff) {
			continue
		}
		candidates = append(candidates, id)
	}
	if len(candidates) == 0 {
		return
	}

	likes, favs, comments, err := r.interactionRepo.CountExactInteractions(runCtx, candidates)
	if err != nil {
		if parentCtx.Err() == nil {
			slog.WarnContext(runCtx, "stats reconcile count exact interactions failed", slog.Any("err", err))
		}
		return
	}

	var changedKeys []string
	for _, id := range candidates {
		l, f, c := likes[id], favs[id], comments[id]
		row, hasRow := existing[id]
		if !hasRow {
			if l == 0 && f == 0 && c == 0 {
				continue
			}
			inserted, err := r.interactionRepo.InsertReconciledStats(runCtx, model.StatsReconcileRow{
				WallpaperID:  id,
				LikeCount:    l,
				FavCount:     f,
				CommentCount: c,
			})
			if err != nil {
				slog.WarnContext(runCtx, "stats reconcile insert missing stats failed",
					slog.Uint64("wallpaper_id", id),
					slog.Any("err", err),
				)
				continue
			}
			if inserted {
				changedKeys = append(changedKeys, cache.WallpaperStatsKey(id))
			}
			continue
		}

		if row.LikeCount == l && row.FavCount == f && row.CommentCount == c {
			continue
		}
		updated, err := r.interactionRepo.CompareAndUpdateStats(runCtx, id, l, f, c, row.UpdatedAt)
		if err != nil {
			slog.WarnContext(runCtx, "stats reconcile cas update failed",
				slog.Uint64("wallpaper_id", id),
				slog.Any("err", err),
			)
			continue
		}
		if updated {
			changedKeys = append(changedKeys, cache.WallpaperStatsKey(id))
		}
	}

	if len(changedKeys) > 0 && r.rdb != nil {
		if err := r.rdb.Unlink(runCtx, changedKeys...).Err(); err != nil {
			slog.WarnContext(runCtx, "stats reconcile unlink l2 cache failed",
				slog.Int("key_count", len(changedKeys)),
				slog.Any("err", err),
			)
		}
	}

	slog.InfoContext(runCtx, "interaction stats reconciled",
		slog.Int("scanned", len(ids)),
		slog.Int("checked", len(candidates)),
		slog.Int("fixed", len(changedKeys)),
		slog.Duration("cost", time.Since(start)),
	)
}
