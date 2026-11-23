package worker

import (
	"context"
	"log/slog"
	"time"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/repository"
)

const (
	latestFeedReconcileInterval = 60 * time.Second
	latestFeedReconcileTimeout  = 10 * time.Second
)

// LatestFeedReconciler 负责启动预热并周期性将 MySQL 最新壁纸快照原子对账至 Redis feed:latest。
type LatestFeedReconciler struct {
	wallpaperRepo *repository.WallpaperRepository
	latestFeed    *cache.LatestFeed
}

// NewLatestFeedReconciler 构造最新流热窗口周期对账任务。
func NewLatestFeedReconciler(
	wallpaperRepo *repository.WallpaperRepository,
	latestFeed *cache.LatestFeed,
) *LatestFeedReconciler {
	return &LatestFeedReconciler{
		wallpaperRepo: wallpaperRepo,
		latestFeed:    latestFeed,
	}
}

// Run 启动立即执行一次预热对账，随后按固定周期对账直至 ctx 取消。
func (r *LatestFeedReconciler) Run(ctx context.Context) {
	r.reconcileOnce(ctx)

	ticker := time.NewTicker(latestFeedReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileOnce(ctx)
		}
	}
}

func (r *LatestFeedReconciler) reconcileOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, latestFeedReconcileTimeout)
	defer cancel()

	ids, err := r.wallpaperRepo.ListLatestIDs(runCtx, 0, cache.LatestFeedMaxSize)
	if err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(runCtx, "latest feed reconcile query mysql failed",
				slog.Any("err", err),
			)
		}
		return
	}

	if err := r.latestFeed.Reconcile(runCtx, ids); err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(runCtx, "latest feed reconcile redis swap failed",
				slog.Int("count", len(ids)),
				slog.Any("err", err),
			)
		}
		return
	}

	slog.InfoContext(runCtx, "latest feed reconciled",
		slog.Int("count", len(ids)),
		slog.Duration("cost", time.Since(start)),
	)
}
