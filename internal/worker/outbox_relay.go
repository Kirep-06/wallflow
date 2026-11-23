package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/repository"
)

const (
	outboxBatchSize        = 20
	outboxPollInterval     = 2 * time.Second
	outboxBatchTimeout     = 30 * time.Second
	outboxPublishTimeout   = 3 * time.Second
	outboxMaxRetries       = 10
	outboxBaseBackoff      = time.Second
	outboxMaxBackoff       = 60 * time.Second
	outboxDisconnectDelay  = 2 * time.Second
	outboxBackoffJitter    = 0.2
	outboxMaxErrorRunes    = 500
	outboxCleanupInterval  = time.Hour
	outboxCleanupTimeout   = 30 * time.Second
	outboxCleanupBatchSize = 1000
	outboxPublishedKeep    = 7 * 24 * time.Hour
)

// OutboxRelay 在 API 进程内运行，负责将 outbox_events 经 Publisher Confirm 可靠投递至 RabbitMQ 并定期清理历史记录。
type OutboxRelay struct {
	outboxRepo *repository.OutboxRepository
	publisher  mq.Publisher
	notifyCh   chan struct{}
	nowFunc    func() time.Time
}

// NewOutboxRelay 构造事务发件箱中继器。
func NewOutboxRelay(outboxRepo *repository.OutboxRepository, publisher mq.Publisher) *OutboxRelay {
	return &OutboxRelay{
		outboxRepo: outboxRepo,
		publisher:  publisher,
		notifyCh:   make(chan struct{}, 1),
		nowFunc:    time.Now,
	}
}

// Notify 在发帖事务提交后非阻塞唤醒 OutboxRelay 立即拉取投递。
func (r *OutboxRelay) Notify() {
	if r == nil {
		return
	}
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

// Run 启动中继循环（即时唤醒 + 2s 定时兜底 + 1h 历史清理），直至 ctx 取消后排空当前批次退出。
func (r *OutboxRelay) Run(ctx context.Context) {
	if r == nil || r.outboxRepo == nil || r.publisher == nil {
		return
	}

	r.drainPending(ctx)

	pollTicker := time.NewTicker(outboxPollInterval)
	cleanupTicker := time.NewTicker(outboxCleanupInterval)
	defer pollTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.notifyCh:
			r.drainPending(ctx)
		case <-pollTicker.C:
			r.drainPending(ctx)
		case <-cleanupTicker.C:
			r.cleanupPublished(ctx)
		}
	}
}

func (r *OutboxRelay) drainPending(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outboxBatchTimeout)
		var aborted bool
		fetched, err := r.outboxRepo.ProcessPendingBatch(runCtx, r.nowFunc().UTC(), outboxBatchSize, func(batch []model.OutboxEvent) []repository.OutboxDeliveryOutcome {
			var outcomes []repository.OutboxDeliveryOutcome
			outcomes, aborted = r.deliverBatch(runCtx, batch)
			return outcomes
		})
		cancel()

		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("outbox relay process pending batch failed", slog.Any("err", err))
			}
			return
		}
		if fetched < outboxBatchSize || aborted {
			return
		}
	}
}

func (r *OutboxRelay) deliverBatch(ctx context.Context, batch []model.OutboxEvent) ([]repository.OutboxDeliveryOutcome, bool) {
	outcomes := make([]repository.OutboxDeliveryOutcome, 0, len(batch))
	now := r.nowFunc().UTC()

	for i, ev := range batch {
		pubCtx, cancel := context.WithTimeout(ctx, outboxPublishTimeout)
		err := r.publisher.Publish(pubCtx, mq.PublishMessage{
			Exchange:   ev.ExchangeName,
			RoutingKey: ev.RoutingKey,
			MessageID:  ev.EventID,
			Body:       []byte(ev.Payload),
		})
		cancel()

		if err == nil {
			outcomes = append(outcomes, repository.OutboxDeliveryOutcome{
				ID:        ev.ID,
				Published: true,
			})
			continue
		}

		errText := truncateRunes(err.Error(), outboxMaxErrorRunes)
		nextRetryCount := ev.RetryCount + 1
		dead := nextRetryCount >= outboxMaxRetries
		nextRetryAt := now
		if dead {
			slog.ErrorContext(ctx, "outbox event moved to dead status",
				slog.Uint64("id", ev.ID),
				slog.String("event_id", ev.EventID),
				slog.Uint64("retry_count", uint64(nextRetryCount)),
				slog.Any("err", err),
			)
		} else {
			nextRetryAt = now.Add(outboxBackoff(nextRetryCount))
			slog.WarnContext(ctx, "outbox event publish failed, scheduled retry",
				slog.Uint64("id", ev.ID),
				slog.String("event_id", ev.EventID),
				slog.Uint64("retry_count", uint64(nextRetryCount)),
				slog.Time("next_retry_at", nextRetryAt),
				slog.Any("err", err),
			)
		}

		outcomes = append(outcomes, repository.OutboxDeliveryOutcome{
			ID:          ev.ID,
			Dead:        dead,
			RetryCount:  nextRetryCount,
			NextRetryAt: nextRetryAt,
			LastError:   errText,
		})

		// MQ 断连时快速失败释放事务，剩余未尝试项不消耗重试次数仅延后短退避。
		if isMQDisconnectError(err) {
			deferUntil := now.Add(outboxDisconnectDelay)
			for _, rem := range batch[i+1:] {
				outcomes = append(outcomes, repository.OutboxDeliveryOutcome{
					ID:          rem.ID,
					RetryCount:  rem.RetryCount,
					NextRetryAt: deferUntil,
					LastError:   errText,
				})
			}
			return outcomes, true
		}
	}

	return outcomes, false
}

func (r *OutboxRelay) cleanupPublished(ctx context.Context) {
	cutoff := r.nowFunc().UTC().Add(-outboxPublishedKeep)
	var total int64

	for {
		if ctx.Err() != nil {
			return
		}
		runCtx, cancel := context.WithTimeout(ctx, outboxCleanupTimeout)
		deleted, err := r.outboxRepo.DeletePublishedBefore(runCtx, cutoff, outboxCleanupBatchSize)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("outbox relay cleanup published failed", slog.Any("err", err))
			}
			return
		}
		total += deleted
		if deleted < outboxCleanupBatchSize {
			break
		}
	}

	if total > 0 {
		slog.Info("outbox relay cleaned up published events", slog.Int64("deleted", total))
	}
}

func outboxBackoff(retryCount uint32) time.Duration {
	if retryCount <= 1 {
		return applyOutboxJitter(outboxBaseBackoff)
	}
	shift := min(retryCount-1, 6)
	base := min(outboxBaseBackoff<<shift, outboxMaxBackoff)
	return applyOutboxJitter(base)
}

func applyOutboxJitter(base time.Duration) time.Duration {
	factor := 1 + (rand.Float64()*2-1)*outboxBackoffJitter
	return max(time.Duration(float64(base)*factor), 200*time.Millisecond)
}

func isMQDisconnectError(err error) bool {
	return errors.Is(err, mq.ErrNotConnected) ||
		errors.Is(err, mq.ErrClosed) ||
		errors.Is(err, amqp.ErrClosed)
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	count := 0
	for idx := range s {
		if count == maxRunes {
			return s[:idx]
		}
		count++
	}
	return s
}
