package consumer

import (
	"context"
	"log/slog"
	"time"

	"wallpaper-feed/internal/repository"
)

const (
	bigVReconcileTimeout  = 30 * time.Second
	outboxCleanupTimeout  = 30 * time.Second
	outboxCleanupBatch    = 1000
	outboxCleanupMaxLoops = 10
)

// BigVStore 抽象全站大 V 列表查询能力。
type BigVStore interface {
	ListBigVIDs(ctx context.Context) ([]uint64, error)
}

// OutboxCleaner 抽象历史已投递事件清理能力。
type OutboxCleaner interface {
	CleanupSentBefore(ctx context.Context, cutoff time.Time, batchLimit, maxBatches int) (int64, error)
}

// FollowingMaintainer 运行于 Worker 进程，承担两项周期维护任务：
//  1. 按 reconcileInterval（默认 60s）对账并原子替换 Redis 全站大 V 集合 feed:bigvs；
//  2. 按 cleanupInterval（默认 1h）分批清理 retentionDays（默认 7 天）前已投递的 outbox_events。
type FollowingMaintainer struct {
	bigVs             BigVStore
	outbox            OutboxCleaner
	feedRepo          *repository.FollowingFeedRepo
	reconcileInterval time.Duration
	cleanupInterval   time.Duration
	retentionDays     int
}

func NewFollowingMaintainer(
	bigVs BigVStore,
	outbox OutboxCleaner,
	feedRepo *repository.FollowingFeedRepo,
	reconcileInterval time.Duration,
	cleanupInterval time.Duration,
	retentionDays int,
) *FollowingMaintainer {
	if reconcileInterval <= 0 {
		reconcileInterval = time.Minute
	}
	if cleanupInterval <= 0 {
		cleanupInterval = time.Hour
	}
	if retentionDays <= 0 {
		retentionDays = 7
	}
	return &FollowingMaintainer{
		bigVs:             bigVs,
		outbox:            outbox,
		feedRepo:          feedRepo,
		reconcileInterval: reconcileInterval,
		cleanupInterval:   cleanupInterval,
		retentionDays:     retentionDays,
	}
}

// Run 启动周期维护循环，直到 ctx 取消。
func (m *FollowingMaintainer) Run(ctx context.Context) {
	// 启动时立即对账一次全站大 V 集合，保证 feed:bigvs 快速就绪。
	m.reconcileBigVsOnce(ctx)

	reconcileTicker := time.NewTicker(m.reconcileInterval)
	cleanupTicker := time.NewTicker(m.cleanupInterval)
	defer reconcileTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcileTicker.C:
			m.reconcileBigVsOnce(ctx)
		case <-cleanupTicker.C:
			m.cleanupOutboxOnce(ctx)
		}
	}
}

func (m *FollowingMaintainer) reconcileBigVsOnce(ctx context.Context) {
	runCtx, cancel := context.WithTimeout(ctx, bigVReconcileTimeout)
	defer cancel()

	ids, err := m.bigVs.ListBigVIDs(runCtx)
	if err != nil {
		slog.ErrorContext(runCtx, "list bigv user ids failed", "err", err)
		return
	}
	if err := m.feedRepo.ReplaceBigVs(runCtx, ids); err != nil {
		slog.ErrorContext(runCtx, "reconcile feed:bigvs failed", "count", len(ids), "err", err)
		return
	}
	slog.DebugContext(runCtx, "reconciled feed:bigvs", "count", len(ids))
}

func (m *FollowingMaintainer) cleanupOutboxOnce(ctx context.Context) {
	runCtx, cancel := context.WithTimeout(ctx, outboxCleanupTimeout)
	defer cancel()

	cutoff := time.Now().Add(-time.Duration(m.retentionDays) * 24 * time.Hour)
	deleted, err := m.outbox.CleanupSentBefore(runCtx, cutoff, outboxCleanupBatch, outboxCleanupMaxLoops)
	if err != nil {
		slog.ErrorContext(runCtx, "cleanup sent outbox events failed", "deleted", deleted, "err", err)
		return
	}
	if deleted > 0 {
		slog.InfoContext(runCtx, "cleaned up sent outbox events", "deleted", deleted, "retention_days", m.retentionDays)
	}
}
