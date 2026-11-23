package worker

import (
	"context"
	"log/slog"
	"time"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/repository"
)

const (
	reconcileInterval = 60 * time.Second
	reconcileTimeout  = 10 * time.Second
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
