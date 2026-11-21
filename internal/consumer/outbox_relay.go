package consumer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
)

const (
	outboxDBOpTimeout     = 5 * time.Second
	outboxConnBackoffBase = 2 * time.Second
	outboxConnBackoffMax  = 30 * time.Second
	maxOutboxBackoffSec   = 60
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
//
// 故障分级策略：
//   - 基础设施连接/传输故障（mq.IsConnectionError）：不递增 retry_count、绝不将正常事件置为死信 status=2，
//     保持 status=0 并按有界指数退避（默认 2s -> 30s）延后重试，MQ 恢复后自动补发；
//   - 事件自身或非连接级错误（字段非法、404/403/406 拓扑权限错误、Broker Nack 等）：消耗单事件重试预算，
//     按 min(2^retry_count, 60)s 退避，达到 MaxRetries 后置为 status=2（死信）并输出 Error 告警。
type OutboxRelay struct {
	store           OutboxStore
	pub             mq.Publisher
	cfg             config.OutboxConfig
	notifyCh        chan struct{}
	now             func() time.Time
	connBackoffBase time.Duration
	connBackoffMax  time.Duration

	connFailures     int
	connBackoffUntil time.Time
}

func NewOutboxRelay(store OutboxStore, pub mq.Publisher, cfg config.OutboxConfig) *OutboxRelay {
	return &OutboxRelay{
		store:           store,
		pub:             pub,
		cfg:             cfg,
		notifyCh:        make(chan struct{}, 1),
		now:             time.Now,
		connBackoffBase: outboxConnBackoffBase,
		connBackoffMax:  outboxConnBackoffMax,
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
		if !r.connBackoffUntil.IsZero() && r.now().Before(r.connBackoffUntil) {
			return
		}
		claimed, connErr := r.processBatch(ctx)
		if connErr || claimed < r.cfg.BatchSize {
			return
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) (int, bool) {
	claimNow := r.now()
	claimCtx, cancel := context.WithTimeout(ctx, outboxDBOpTimeout)
	events, err := r.store.ClaimBatch(claimCtx, r.cfg.BatchSize, time.Duration(r.cfg.LeaseSeconds)*time.Second, claimNow)
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
	var connDeferUntil time.Time

	for i, ev := range events {
		if ctx.Err() != nil {
			// 进程正在优雅退出：尚未尝试的事件立即释放租约，留待下次启动补发。
			for _, rem := range events[i:] {
				skippedIDs = append(skippedIDs, rem.ID)
			}
			break
		}

		if ev.EventID == "" || ev.ExchangeName == "" || ev.RoutingKey == "" {
			r.recordPermanentEventFailure(baseCtx, ev, errors.New("invalid outbox event metadata"), r.now())
			continue
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

		now := r.now()
		if ctx.Err() != nil && errors.Is(pubErr, context.Canceled) {
			// 进程优雅退出打断发布：释放当前及剩余事件租约，不计失败。
			for _, rem := range events[i:] {
				skippedIDs = append(skippedIDs, rem.ID)
			}
			break
		}

		if mq.IsConnectionError(pubErr) {
			// 基础设施连接故障：绝不消耗单事件重试预算，不标死信；
			// 记录本次连接异常信息并按有界指数退避延后整批未发事件，等待 MQ 恢复自动补发。
			if len(sentIDs) > 0 {
				r.connFailures = 1
			} else {
				r.connFailures++
			}
			backoff := r.connectionBackoff(r.connFailures)
			connDeferUntil = now.Add(backoff)
			r.connBackoffUntil = connDeferUntil

			slog.Warn("outbox relay deferred batch on amqp connection error",
				"id", ev.ID,
				"event_id", ev.EventID,
				"remaining", len(events)-i-1,
				"conn_failures", r.connFailures,
				"backoff", backoff,
				"err", pubErr,
			)
			r.deferEventOnConnectionError(baseCtx, ev, pubErr, connDeferUntil, now)
			for _, rem := range events[i+1:] {
				skippedIDs = append(skippedIDs, rem.ID)
			}
			connBroken = true
			break
		}

		// 非连接级错误（拓扑/权限/Broker Nack/单事件异常）：消耗该事件重试预算。
		r.recordPublishFailure(baseCtx, ev, pubErr, now)
	}

	if !connBroken && len(sentIDs) > 0 {
		r.connFailures = 0
		r.connBackoffUntil = time.Time{}
	}

	now := r.now()
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
		if connBroken && !connDeferUntil.IsZero() {
			releaseAt = connDeferUntil
		}
		if err := r.store.ReleaseLease(relCtx, skippedIDs, releaseAt, now); err != nil {
			slog.Warn("release outbox lease failed", "count", len(skippedIDs), "err", err)
		}
		relCancel()
	}

	return len(events), connBroken
}

// deferEventOnConnectionError 在基础设施连接故障时保留 status=0 与原有 retry_count，仅延后重试时间并记录 last_error。
func (r *OutboxRelay) deferEventOnConnectionError(baseCtx context.Context, ev *model.OutboxEvent, pubErr error, deferUntil, now time.Time) {
	failCtx, cancel := context.WithTimeout(baseCtx, outboxDBOpTimeout)
	defer cancel()
	if err := r.store.MarkFailed(failCtx, ev.ID, ev.RetryCount, deferUntil, false, pubErr.Error(), now); err != nil {
		slog.Warn("defer outbox event on connection error failed", "id", ev.ID, "event_id", ev.EventID, "err", err)
	}
}

func (r *OutboxRelay) recordPermanentEventFailure(baseCtx context.Context, ev *model.OutboxEvent, reason error, now time.Time) {
	slog.Error("outbox event has unrecoverable metadata and moved to dead status",
		"id", ev.ID, "event_id", ev.EventID, "err", reason)
	failCtx, cancel := context.WithTimeout(baseCtx, outboxDBOpTimeout)
	defer cancel()
	if err := r.store.MarkFailed(failCtx, ev.ID, ev.RetryCount+1, now, true, reason.Error(), now); err != nil {
		slog.Error("mark permanent outbox event failed error", "id", ev.ID, "event_id", ev.EventID, "err", err)
	}
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

func (r *OutboxRelay) connectionBackoff(failures int) time.Duration {
	base := r.connBackoffBase
	if base <= 0 {
		base = outboxConnBackoffBase
	}
	maxD := r.connBackoffMax
	if maxD <= 0 {
		maxD = outboxConnBackoffMax
	}
	if maxD < base {
		maxD = base
	}
	if failures <= 1 {
		return base
	}
	shift := min(failures-1, 6)
	d := base << shift
	if d > maxD || d <= 0 {
		return maxD
	}
	return d
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
