package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ 交换机、队列、路由键与重试头常量（M5/M6/M7 复用）。
const (
	// 交换机
	ExchangeEvents = "wallpaper.events" // topic 业务事件总交换机
	ExchangeDLX    = "wallpaper.dlx"    // direct 死信与延迟回流交换机

	// 业务路由键
	RoutingKeyWallpaperPublished   = "wallpaper.published"
	RoutingKeyWallpaperInteraction = "wallpaper.interaction"

	// M5 关注流三队列闭环（主队列 -> 5s 延迟重试队列 -> 终态 DLQ）
	QueueFollowingFanout      = "feed.following.fanout"
	QueueFollowingFanoutRetry = "feed.following.fanout.retry"
	QueueFollowingFanoutDLQ   = "feed.following.fanout.dlq"

	// M5 精确死信路由键
	RoutingKeyFollowingFanoutWork  = "feed.following.fanout.work"
	RoutingKeyFollowingFanoutRetry = "feed.following.fanout.retry"
	RoutingKeyFollowingFanoutDLQ   = "feed.following.fanout.dlq"

	// M6 高频互动计数聚合队列与独立毒丸 DLQ
	QueueStatsInteractionAgg         = "stats.interaction.agg"
	QueueStatsInteractionAggDLQ      = "stats.interaction.agg.dlq"
	RoutingKeyStatsInteractionAggDLQ = "stats.interaction.agg.dlq"

	// 重试诊断头与队列级延迟 TTL
	HeaderRetryCount       = "x-retry-count"
	HeaderLastError        = "x-last-error"
	RetryDelayMS     int32 = 5000
)

// DeclareTopology 幂等声明 M5 所需的交换机、三队列闭环与精确绑定关系。
func DeclareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeEvents, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("mq declare events exchange: %w", err)
	}
	if err := ch.ExchangeDeclare(ExchangeDLX, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("mq declare dlx exchange: %w", err)
	}

	// 1. 终态死信队列
	if _, err := ch.QueueDeclare(QueueFollowingFanoutDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("mq declare dlq: %w", err)
	}
	if err := ch.QueueBind(QueueFollowingFanoutDLQ, RoutingKeyFollowingFanoutDLQ, ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("mq bind dlq: %w", err)
	}

	// 2. 5s 固定 TTL 延迟重试队列（无消费者，到期经 DLX 精确回流主队列）
	retryArgs := amqp.Table{
		"x-message-ttl":             RetryDelayMS,
		"x-dead-letter-exchange":    ExchangeDLX,
		"x-dead-letter-routing-key": RoutingKeyFollowingFanoutWork,
	}
	if _, err := ch.QueueDeclare(QueueFollowingFanoutRetry, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("mq declare retry queue: %w", err)
	}
	if err := ch.QueueBind(QueueFollowingFanoutRetry, RoutingKeyFollowingFanoutRetry, ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("mq bind retry queue: %w", err)
	}

	// 3. 扇出主队列（Nack(requeue=false) 原生直达终态 DLQ，同时承接重试队列回流）
	mainArgs := amqp.Table{
		"x-dead-letter-exchange":    ExchangeDLX,
		"x-dead-letter-routing-key": RoutingKeyFollowingFanoutDLQ,
	}
	if _, err := ch.QueueDeclare(QueueFollowingFanout, true, false, false, false, mainArgs); err != nil {
		return fmt.Errorf("mq declare main queue: %w", err)
	}
	if err := ch.QueueBind(QueueFollowingFanout, RoutingKeyWallpaperPublished, ExchangeEvents, false, nil); err != nil {
		return fmt.Errorf("mq bind main queue to events: %w", err)
	}
	if err := ch.QueueBind(QueueFollowingFanout, RoutingKeyFollowingFanoutWork, ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("mq bind main queue to dlx work: %w", err)
	}

	// 4. M6 互动计数削峰队列与独立毒丸死信队列
	if _, err := ch.QueueDeclare(QueueStatsInteractionAggDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("mq declare stats agg dlq: %w", err)
	}
	if err := ch.QueueBind(QueueStatsInteractionAggDLQ, RoutingKeyStatsInteractionAggDLQ, ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("mq bind stats agg dlq: %w", err)
	}
	statsArgs := amqp.Table{
		"x-dead-letter-exchange":    ExchangeDLX,
		"x-dead-letter-routing-key": RoutingKeyStatsInteractionAggDLQ,
	}
	if _, err := ch.QueueDeclare(QueueStatsInteractionAgg, true, false, false, false, statsArgs); err != nil {
		return fmt.Errorf("mq declare stats agg queue: %w", err)
	}
	if err := ch.QueueBind(QueueStatsInteractionAgg, RoutingKeyWallpaperInteraction, ExchangeEvents, false, nil); err != nil {
		return fmt.Errorf("mq bind stats agg queue to events: %w", err)
	}

	return nil
}
