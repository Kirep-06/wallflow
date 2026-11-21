// Package consumer 包含 Worker 进程的定时任务与消息消费者。
package consumer

import (
	"context"
	"log/slog"
	"time"

	"wallpaper-feed/internal/service"
)

// LatestReconciler 定期对账最新流热窗口，兜底处理 Redis 重启或发帖写入失败。
type LatestReconciler struct {
	rebuilder *service.LatestRebuilder
	interval  time.Duration
}

func NewLatestReconciler(rebuilder *service.LatestRebuilder, interval time.Duration) *LatestReconciler {
	return &LatestReconciler{rebuilder: rebuilder, interval: interval}
}

// Run 启动对账循环，直到 ctx 取消。
func (r *LatestReconciler) Run(ctx context.Context) {
	r.runOnce(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runOnce(ctx)
		}
	}
}

func (r *LatestReconciler) runOnce(ctx context.Context) {
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.rebuilder.Reconcile(runCtx); err != nil {
		slog.ErrorContext(runCtx, "reconcile latest feed failed", "err", err)
	}
}
