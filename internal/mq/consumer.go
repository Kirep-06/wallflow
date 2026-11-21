package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
)

const (
	retryCountHeader     = "x-retry-count"
	messageHandleTimeout = 30 * time.Second
	retryPublishTimeout  = 5 * time.Second
	consumerDrainTimeout = 15 * time.Second
	requeuePause         = 200 * time.Millisecond
)

// ErrPermanent 表示消息体损坏或业务校验不可恢复错误，消费端将直接 Nack(requeue=false) 进入 DLQ。
var ErrPermanent = errors.New("permanent consume error")

// HandlerFunc 是单条消息的业务处理回调。
type HandlerFunc func(ctx context.Context, body []byte) error

// Consumer 封装受控协程池、QoS 限流、延迟重试队列转发、DLQ 路由与 Cancel 优雅退出。
type Consumer struct {
	client      *Client
	cfg         config.RabbitMQConfig
	prefetch    int
	concurrency int
	consumerTag string
	retryPub    *ConfirmPublisher
}

func NewConsumer(client *Client, mqCfg config.RabbitMQConfig, workerCfg config.WorkerConfig) *Consumer {
	prefetch := workerCfg.FanoutPrefetch
	if prefetch <= 0 {
		prefetch = 8
	}
	concurrency := workerCfg.FanoutConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	return &Consumer{
		client:      client,
		cfg:         mqCfg,
		prefetch:    prefetch,
		concurrency: concurrency,
		consumerTag: fmt.Sprintf("wallpaper-worker-%d", os.Getpid()),
		retryPub:    NewConfirmPublisher(client),
	}
}

// Run 持续监听队列直到 ctx 取消。断线自动重建消费通道；退出时先 Cancel 停止新投递并等待在途消息完成。
func (c *Consumer) Run(ctx context.Context, handler HandlerFunc) error {
	defer func() { _ = c.retryPub.Close() }()

	for {
		if ctx.Err() != nil {
			return nil
		}
		conn, err := c.client.WaitConnection(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrClientClosed) {
				return nil
			}
			slog.Warn("wait rabbitmq connection failed", "err", err)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			slog.Warn("open consumer channel failed", "err", err)
			c.client.ReportError(conn)
			if !sleepCtx(ctx, time.Second) {
				return nil
			}
			continue
		}
		if err := ch.Qos(c.prefetch, 0, false); err != nil {
			_ = ch.Close()
			slog.Warn("set consumer qos failed", "err", err)
			c.client.ReportError(conn)
			if !sleepCtx(ctx, time.Second) {
				return nil
			}
			continue
		}
		deliveries, err := ch.Consume(
			c.cfg.FanoutQueue,
			c.consumerTag,
			false, // autoAck
			false, // exclusive
			false, // noLocal
			false, // noWait
			nil,
		)
		if err != nil {
			_ = ch.Close()
			slog.Warn("start amqp consume failed", "queue", c.cfg.FanoutQueue, "err", err)
			c.client.ReportError(conn)
			if !sleepCtx(ctx, time.Second) {
				return nil
			}
			continue
		}

		slog.Info("amqp consumer started", "queue", c.cfg.FanoutQueue, "prefetch", c.prefetch, "concurrency", c.concurrency)

		var wg sync.WaitGroup
		for range c.concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for d := range deliveries {
					c.processDelivery(ctx, d, handler)
				}
			}()
		}

		doneCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneCh)
		}()

		select {
		case <-doneCh:
			_ = ch.Close()
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("amqp delivery stream closed, reconnecting", "queue", c.cfg.FanoutQueue)
			c.client.ReportError(conn)
			if !sleepCtx(ctx, time.Second) {
				return nil
			}
		case <-ctx.Done():
			slog.Info("amqp consumer shutting down, cancelling tag", "queue", c.cfg.FanoutQueue, "tag", c.consumerTag)
			if err := ch.Cancel(c.consumerTag, false); err != nil {
				slog.Warn("amqp consumer cancel failed", "queue", c.cfg.FanoutQueue, "err", err)
			}
			timer := time.NewTimer(consumerDrainTimeout)
			select {
			case <-doneCh:
				timer.Stop()
			case <-timer.C:
				slog.Warn("amqp consumer drain timeout, closing channel", "queue", c.cfg.FanoutQueue)
			}
			_ = ch.Close()
			slog.Info("amqp consumer stopped", "queue", c.cfg.FanoutQueue)
			return nil
		}
	}
}

func (c *Consumer) processDelivery(rootCtx context.Context, d amqp.Delivery, handler HandlerFunc) {
	// 与根 ctx 取消解耦，保证收到 SIGTERM 后在途消息有机会跑完扇出并 Ack/Nack。
	baseCtx := context.WithoutCancel(rootCtx)
	msgCtx, cancel := context.WithTimeout(baseCtx, messageHandleTimeout)
	defer cancel()

	err := handler(msgCtx, d.Body)
	if err == nil {
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Warn("amqp ack failed", "message_id", d.MessageId, "err", ackErr)
		}
		return
	}

	if errors.Is(err, ErrPermanent) {
		slog.Error("permanent message error, routing to dlq", "queue", c.cfg.FanoutQueue, "message_id", d.MessageId, "err", err)
		if nackErr := d.Nack(false, false); nackErr != nil {
			slog.Warn("amqp nack to dlq failed", "message_id", d.MessageId, "err", nackErr)
		}
		return
	}

	retryCount := extractRetryCount(d.Headers)
	if retryCount >= c.cfg.MaxConsumeRetries {
		slog.Error("message exceeded max consume retries, routing to dlq",
			"queue", c.cfg.FanoutQueue, "message_id", d.MessageId, "retry_count", retryCount, "err", err)
		if nackErr := d.Nack(false, false); nackErr != nil {
			slog.Warn("amqp nack to dlq failed", "message_id", d.MessageId, "err", nackErr)
		}
		return
	}

	headers := cloneHeaders(d.Headers)
	headers[retryCountHeader] = int32(retryCount + 1)

	pubCtx, pubCancel := context.WithTimeout(baseCtx, retryPublishTimeout)
	pubErr := c.retryPub.Publish(pubCtx, Message{
		Exchange:   "",
		RoutingKey: c.cfg.RetryQueue,
		MessageID:  d.MessageId,
		Body:       d.Body,
		Headers:    headers,
	})
	pubCancel()

	if pubErr != nil {
		slog.Error("publish to retry queue failed, requeuing original message",
			"queue", c.cfg.FanoutQueue, "retry_queue", c.cfg.RetryQueue, "message_id", d.MessageId, "err", pubErr)
		if nackErr := d.Nack(false, true); nackErr != nil {
			slog.Warn("amqp nack requeue failed", "message_id", d.MessageId, "err", nackErr)
		}
		time.Sleep(requeuePause)
		return
	}

	slog.Warn("message scheduled for delayed retry",
		"queue", c.cfg.FanoutQueue, "message_id", d.MessageId, "next_retry", retryCount+1, "err", err)
	if ackErr := d.Ack(false); ackErr != nil {
		slog.Warn("amqp ack after retry publish failed", "message_id", d.MessageId, "err", ackErr)
	}
}

func extractRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	v, ok := headers[retryCountHeader]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	default:
		return 0
	}
}

func cloneHeaders(src amqp.Table) amqp.Table {
	dst := make(amqp.Table, len(src)+1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
