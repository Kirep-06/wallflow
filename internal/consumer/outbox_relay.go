package consumer

import (
	"context"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
)

const (
	outboxDBOpTimeout    = 5 * time.Second
	outboxConnRetryDelay = 2 * time.Second
	maxOutboxBackoffSec  = 60
)

// OutboxStore 抽象 Outbox 仓储能力，便于单测替换。
type OutboxStore interface {
	ClaimBatch(ctx context.Context, limit int, lease time.Duration, now time.Time) ([]*model.OutboxEvent, error)
	MarkSent(ctx context.Context, ids []uint64, now time.Time) error
	MarkFailed(ctx context.Context, id uint64, retryCount uint16, nextRetryAt time.Time, dead bool, lastErr string, now time.Time) error
	ReleaseLease(ctx context.Context, ids []uint64, nextRetryAt, now time.Time) error
}

// OutboxRelay 运行在 API 进程内，通过事件唤醒 + 定时轮询将 outbox_events 可靠投递至 RabbitMQ。
// 严格遵循三阶段租约模式：事务内只做 Claim 续租，事务外执行 Publisher Confirms 投递，最后回写状态。
type OutboxRelay struct {
	store    OutboxStore
	pub      mq.Publisher
	cfg      config.OutboxConfig
	notifyCh chan struct{}
}

func NewOutboxRelay(store OutboxStore, pub mq.Publisher, cfg config.OutboxConfig) *OutboxRelay {
	return &OutboxRelay{
		store:    store,
		pub:      pub,
		cfg:      cfg,
		notifyCh: make(chan struct{}, 1),
	}
}

// Notify 在业务事务提交后非阻塞唤醒 Relay，实现毫秒级投递。
func (r *OutboxRelay) Notify() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

// Run 持续执行捞取与投递，直至 ctx 取消并安全收尾当前已 Claim 的记录。
func (r *OutboxRelay) Run(ctx context.Context) {
	pollInterval := time.Duration(r.cfg.PollIntervalMs) * time.Millisecond
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	// 启动后立即尝试排空上次退出残留的待发事件。
	r.drain(ctx)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox relay stopped")
			return
		case <-r.notifyCh:
			r.drain(ctx)
		case <-ticker.C:
			r.drain(ctx)
		}
	}
}

func (r *OutboxRelay) drain(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		claimed, connErr := r.processBatch(ctx)
		if connErr || claimed < r.cfg.BatchSize {
			return
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) (int, bool) {
	claimCtx, cancel := context.WithTimeout(ctx, outboxDBOpTimeout)
	events, err := r.store.ClaimBatch(claimCtx, r.cfg.BatchSize, time.Duration(r.cfg.LeaseSeconds)*time.Second, time.Now())
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "claim outbox batch failed", "err", err)
		}
		return 0, false
	}
	if len(events) == 0 {
		return 0, false
	}

	baseCtx := context.WithoutCancel(ctx)
	pubTimeout := time.Duration(r.cfg.PublishTimeoutMs) * time.Millisecond
	sentIDs := make([]uint64, 0, len(events))
	var skippedIDs []uint64
	connBroken := false

	for i, ev := range events {
		if ctx.Err() != nil {
			// 进程正在优雅退出：尚未尝试的事件立即释放租约，留待下次启动补发。
			for _, rem := range events[i:] {
				skippedIDs = append(skippedIDs, rem.ID)
			}
			break
		}

		pubCtx, pubCancel := context.WithTimeout(baseCtx, pubTimeout)
		pubErr := r.pub.Publish(pubCtx, mq.Message{
			Exchange:   ev.ExchangeName,
			RoutingKey: ev.RoutingKey,
			MessageID:  ev.EventID,
			Body:       []byte(ev.Payload),
			Headers: amqp.Table{
				"x-retry-count": int32(0),
			},
		})
		pubCancel()

		if pubErr == nil {
			sentIDs = append(sentIDs, ev.ID)
			continue
		}

		now := time.Now()
		r.recordPublishFailure(baseCtx, ev, pubErr, now)

		if mq.IsConnectionError(pubErr) {
			// AMQP 连接级断开：立刻中止本批剩余事件并释放其租约，避免逐条等超时拖穿租约。
			slog.Warn("outbox relay aborted batch on amqp connection error",
				"event_id", ev.EventID, "remaining", len(events)-i-1, "err", pubErr)
			for _, rem := range events[i+1:] {
				skippedIDs = append(skippedIDs, rem.ID)
			}
			connBroken = true
			break
		}
	}

	now := time.Now()
	if len(sentIDs) > 0 {
		markCtx, markCancel := context.WithTimeout(baseCtx, outboxDBOpTimeout)
		if err := r.store.MarkSent(markCtx, sentIDs, now); err != nil {
			slog.Error("mark outbox events sent failed", "count", len(sentIDs), "err", err)
		}
		markCancel()
	}
	if len(skippedIDs) > 0 {
		relCtx, relCancel := context.WithTimeout(baseCtx, outboxDBOpTimeout)
		releaseAt := now
		if connBroken {
			releaseAt = now.Add(outboxConnRetryDelay)
		}
		if err := r.store.ReleaseLease(relCtx, skippedIDs, releaseAt, now); err != nil {
			slog.Warn("release outbox lease failed", "count", len(skippedIDs), "err", err)
		}
		relCancel()
	}

	return len(events), connBroken
}

func (r *OutboxRelay) recordPublishFailure(baseCtx context.Context, ev *model.OutboxEvent, pubErr error, now time.Time) {
	nextRetryCount := ev.RetryCount + 1
	dead := int(nextRetryCount) >= r.cfg.MaxRetries
	backoffSec := outboxBackoffSeconds(ev.RetryCount)
	nextRetryAt := now.Add(time.Duration(backoffSec) * time.Second)

	if dead {
		slog.Error("outbox event exceeded max retries and moved to dead status",
			"id", ev.ID, "event_id", ev.EventID, "retry_count", nextRetryCount, "err", pubErr)
	} else {
		slog.Warn("outbox event publish failed, scheduled retry",
			"id", ev.ID, "event_id", ev.EventID, "retry_count", nextRetryCount, "backoff_sec", backoffSec, "err", pubErr)
	}

	failCtx, cancel := context.WithTimeout(baseCtx, outboxDBOpTimeout)
	defer cancel()
	if err := r.store.MarkFailed(failCtx, ev.ID, nextRetryCount, nextRetryAt, dead, pubErr.Error(), now); err != nil {
		slog.Error("mark outbox event failed error", "id", ev.ID, "event_id", ev.EventID, "err", err)
	}
}

// outboxBackoffSeconds 计算单条事件指数退避秒数：min(2^retry_count, 60)。
func outboxBackoffSeconds(retryCount uint16) int {
	shift := min(retryCount, 6)
	sec := 1 << shift
	if sec > maxOutboxBackoffSec {
		return maxOutboxBackoffSec
	}
	return sec
}
