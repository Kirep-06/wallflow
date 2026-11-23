package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// ExchangeEvents 为全站统一业务事件 topic 交换机（M5/M6/M7 共用）。
	ExchangeEvents = "wallpaper.events"
	// ExchangeDLX 为全站统一死信与延迟回流 direct 交换机（按精确 routing_key 隔离，严禁 topic + "#"）。
	ExchangeDLX = "wallpaper.dlx"

	// RoutingKeyWallpaperPublished 为 M5 壁纸投稿发布事件路由键。
	RoutingKeyWallpaperPublished = "wallpaper.published"

	// QueueFollowingFanout 为关注流推拉扇出主消费队列。
	QueueFollowingFanout = "feed.following.fanout"
	// QueueFollowingFanoutRetry 为 5s 固定 TTL 无消费者延迟重试队列。
	QueueFollowingFanoutRetry = "feed.following.fanout.retry"
	// QueueFollowingFanoutDLQ 为超限或毒丸消息终态死信队列。
	QueueFollowingFanoutDLQ = "feed.following.fanout.dlq"

	// RoutingKeyFollowingFanoutWork 为重试队列到期回流主队列的精确路由键。
	RoutingKeyFollowingFanoutWork = "feed.following.fanout.work"
	// RoutingKeyFollowingFanoutRetry 为消费瞬态失败投往重试队列的精确路由键。
	RoutingKeyFollowingFanoutRetry = "feed.following.fanout.retry"
	// RoutingKeyFollowingFanoutDLQ 为主队列原生死信投往终态 DLQ 的精确路由键。
	RoutingKeyFollowingFanoutDLQ = "feed.following.fanout.dlq"

	// HeaderRetryCount 记录消息已经过延迟重试队列的次数。
	HeaderRetryCount = "x-retry-count"
	// HeaderLastError 记录最近一次消费失败的截断摘要。
	HeaderLastError = "x-last-error"

	// RetryDelayMS 为延迟重试队列的统一队列级 TTL（毫秒），避免 per-message TTL 队头阻塞。
	RetryDelayMS int32 = 5000
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

	return nil
}
