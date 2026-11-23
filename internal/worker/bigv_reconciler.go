package worker

import (
	"context"
	"log/slog"
	"time"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/repository"
)

const (
	bigVReconcileInterval = 60 * time.Second
	bigVReconcileTimeout  = 10 * time.Second
)

// BigVReconciler 负责启动预热并每 60s 将 MySQL 全站大 V 集合原子对账至 Redis feed:bigvs（含哨兵 "0"）。
type BigVReconciler struct {
	followRepo *repository.FollowRepository
	bigVSet    *cache.BigVSet
}

// NewBigVReconciler 构造全站大 V 周期对账任务。
func NewBigVReconciler(followRepo *repository.FollowRepository, bigVSet *cache.BigVSet) *BigVReconciler {
	return &BigVReconciler{
		followRepo: followRepo,
		bigVSet:    bigVSet,
	}
}

// Run 启动立即执行一次预热对账，随后按 60s 周期对账直至 ctx 取消。
func (r *BigVReconciler) Run(ctx context.Context) {
	r.reconcileOnce(ctx)

	ticker := time.NewTicker(bigVReconcileInterval)
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

func (r *BigVReconciler) reconcileOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, bigVReconcileTimeout)
	defer cancel()

	ids, err := r.followRepo.ListAllBigVIDs(runCtx)
	if err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(runCtx, "bigv reconcile query mysql failed", slog.Any("err", err))
		}
		return
	}

	if err := r.bigVSet.Reconcile(runCtx, ids); err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(runCtx, "bigv reconcile redis swap failed",
				slog.Int("count", len(ids)),
				slog.Any("err", err),
			)
		}
		return
	}

	slog.InfoContext(runCtx, "bigv set reconciled",
		slog.Int("count", len(ids)),
		slog.Duration("cost", time.Since(start)),
	)
}
