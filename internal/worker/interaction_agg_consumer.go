package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/repository"
)

const (
	aggConsumerTag     = "worker-interaction-agg"
	aggPrefetch        = 400
	aggBatchSize       = 200
	aggFlushInterval   = 2 * time.Second
	aggFlushTimeout    = 10 * time.Second
	aggMaxFlushRetries = 3
	aggRetryBaseDelay  = 300 * time.Millisecond
)

type statsBatchFlusher interface {
	BatchApplyStatsDeltas(ctx context.Context, deltas []model.StatsDelta) error
}

// InteractionAggConsumer 单协程零锁消费 stats.interaction.agg，按条数/时间窗口聚合计数批量刷盘，
// 落库并批量失效 L2 缓存后执行批量手动 Ack；停机时经 channel.Cancel 排空并同步完成最终 Flush 零丢数。
type InteractionAggConsumer struct {
	flusher        statsBatchFlusher
	rdb            *redis.Client
	batchSize      int
	flushInterval  time.Duration
	retryBaseDelay time.Duration
}

// NewInteractionAggConsumer 构造高频互动计数窗口聚合消费者。
func NewInteractionAggConsumer(repo *repository.InteractionRepository, rdb *redis.Client) *InteractionAggConsumer {
	return &InteractionAggConsumer{
		flusher:        repo,
		rdb:            rdb,
		batchSize:      aggBatchSize,
		flushInterval:  aggFlushInterval,
		retryBaseDelay: aggRetryBaseDelay,
	}
}

// Run 在给定 mq.Client 上启动单流聚合消费循环，直至 ctx 取消并完成最终同步 Flush 后返回。
func (c *InteractionAggConsumer) Run(ctx context.Context, client *mq.Client) error {
	return client.ConsumeStream(ctx, mq.QueueStatsInteractionAgg, aggConsumerTag, aggPrefetch, c.runSession)
}

func (c *InteractionAggConsumer) runSession(ctx context.Context, deliveries <-chan mq.StreamDelivery) error {
	pending := make(map[uint64]*model.StatsDelta, c.batchSize)
	var (
		pendingCount int
		lastDelivery *mq.StreamDelivery
	)

	flush := func(flushCtx context.Context) error {
		if pendingCount == 0 {
			return nil
		}

		deltas := make([]model.StatsDelta, 0, len(pending))
		keys := make([]string, 0, len(pending))
		for wpID, d := range pending {
			if d.IsZero() {
				continue
			}
			deltas = append(deltas, *d)
			keys = append(keys, cache.WallpaperStatsKey(wpID))
		}

		if len(deltas) > 0 {
			if err := c.applyWithRetry(flushCtx, deltas); err != nil {
				return err
			}
			if c.rdb != nil {
				if err := c.rdb.Unlink(flushCtx, keys...).Err(); err != nil {
					// MySQL 已落盘，缓存删除失败仅告警绝不跳过 Ack，防止重投造成 MySQL 重复累加。
					slog.WarnContext(flushCtx, "unlink wallpaper stats l2 cache failed",
						slog.Int("key_count", len(keys)),
						slog.Any("err", err),
					)
				}
			}
		}

		if lastDelivery != nil && lastDelivery.Ack != nil {
			if err := lastDelivery.Ack(true); err != nil {
				slog.WarnContext(flushCtx, "batch ack interaction deliveries failed",
					slog.Int("msg_count", pendingCount),
					slog.Any("err", err),
				)
				return err
			}
		}

		slog.InfoContext(flushCtx, "interaction stats batch flushed",
			slog.Int("msg_count", pendingCount),
			slog.Int("wallpaper_count", len(deltas)),
		)
		clear(pending)
		pendingCount = 0
		lastDelivery = nil
		return nil
	}

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				// 仅在优雅停机（ctx 已取消、Channel 完好排空）时执行最终同步 Flush 零丢数；
				// 若为非预期断连（ctx 未取消），Channel 已失效无法 Ack，应放弃内存缓冲由 Broker 重投防双倍累加。
				if pendingCount > 0 && ctx.Err() != nil {
					finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), aggFlushTimeout)
					err := flush(finalCtx)
					cancel()
					return err
				}
				return nil
			}

			ev, valid := decodeInteractionEvent(d.Body)
			if !valid {
				slog.ErrorContext(ctx, "drop malformed interaction event to dlq",
					slog.String("message_id", d.MessageID),
				)
				if d.Nack != nil {
					_ = d.Nack(false, false)
				}
				continue
			}

			delta, exists := pending[ev.WallpaperID]
			if !exists {
				delta = &model.StatsDelta{WallpaperID: ev.WallpaperID}
				pending[ev.WallpaperID] = delta
			}
			delta.Add(ev.Action, ev.Delta)
			pendingCount++
			dc := d
			lastDelivery = &dc

			if pendingCount >= c.batchSize {
				flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), aggFlushTimeout)
				err := flush(flushCtx)
				cancel()
				if err != nil {
					return err
				}
				ticker.Reset(c.flushInterval)
			}

		case <-ticker.C:
			if pendingCount == 0 {
				continue
			}
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), aggFlushTimeout)
			err := flush(flushCtx)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func (c *InteractionAggConsumer) applyWithRetry(ctx context.Context, deltas []model.StatsDelta) error {
	delay := c.retryBaseDelay
	var lastErr error
	for attempt := range aggMaxFlushRetries {
		if err := c.flusher.BatchApplyStatsDeltas(ctx, deltas); err == nil {
			return nil
		} else {
			lastErr = err
			slog.WarnContext(ctx, "interaction stats batch flush failed",
				slog.Int("attempt", attempt+1),
				slog.Int("wallpaper_count", len(deltas)),
				slog.Any("err", err),
			)
		}

		if attempt+1 >= aggMaxFlushRetries {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}
		delay *= 2
	}
	return lastErr
}

func decodeInteractionEvent(body []byte) (model.InteractionEvent, bool) {
	var ev model.InteractionEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return model.InteractionEvent{}, false
	}
	if ev.WallpaperID == 0 {
		return model.InteractionEvent{}, false
	}
	var probe model.StatsDelta
	if !probe.Add(ev.Action, ev.Delta) {
		return model.InteractionEvent{}, false
	}
	return ev, true
}
