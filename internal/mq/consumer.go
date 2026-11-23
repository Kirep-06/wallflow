package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultConsumerPrefetch = 8
	defaultConsumerWorkers  = 4
	defaultMaxRetries       = 5
	consumerDrainTimeout    = 15 * time.Second
	consumerHandleTimeout   = 30 * time.Second
	maxHeaderErrorLen       = 256
	consumerReconnectWait   = time.Second
)

// ErrPermanent 标识不可恢复的毒丸消息错误，消费框架将直接 Nack(requeue=false) 送入终态 DLQ。
var ErrPermanent = errors.New("mq: permanent delivery error")

// PermanentError 将底层错误包装为不可恢复毒丸错误。
func PermanentError(err error) error {
	if err == nil {
		return ErrPermanent
	}
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// Delivery 为脱耦底层连接句柄的消费消息视图。
type Delivery struct {
	Body       []byte
	MessageID  string
	Headers    amqp.Table
	RetryCount int
}

// MessageHandler 为单条队列消息的业务处理回调。
type MessageHandler func(ctx context.Context, d Delivery) error

// ConsumerOptions 配置消费队列、并发度与延迟重试路由键。
type ConsumerOptions struct {
	Queue           string
	ConsumerTag     string
	Prefetch        int
	Workers         int
	MaxRetries      int
	RetryRoutingKey string
}

type deliveryAcker interface {
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
}

// Consume 在 Client 上运行具备断线重连重建、有界并发 worker、三队列延迟重试与
// channel.Cancel 优雅排空能力的消费者循环，直至 ctx 取消并排空在途消息后返回。
func (c *Client) Consume(ctx context.Context, opts ConsumerOptions, handler MessageHandler) error {
	if c == nil {
		return ErrNotConnected
	}
	opts = normalizeConsumerOptions(opts)

	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := c.waitReady(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrClosed) {
				return nil
			}
			return err
		}

		if err := consumeSession(ctx, conn, c, opts, handler); err != nil && ctx.Err() == nil {
			slog.Warn("rabbitmq consumer session ended, waiting for reconnect",
				slog.String("queue", opts.Queue),
				slog.Any("err", err),
			)
		}
		if ctx.Err() != nil {
			return nil
		}

		timer := time.NewTimer(consumerReconnectWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func consumeSession(
	ctx context.Context,
	conn *amqp.Connection,
	retryPub Publisher,
	opts ConsumerOptions,
	handler MessageHandler,
) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer func() {
		if !ch.IsClosed() {
			_ = ch.Close()
		}
	}()

	if err := ch.Qos(opts.Prefetch, 0, false); err != nil {
		return fmt.Errorf("set consumer qos: %w", err)
	}

	deliveries, err := ch.Consume(opts.Queue, opts.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start consume %s: %w", opts.Queue, err)
	}

	slog.Info("rabbitmq consumer started",
		slog.String("queue", opts.Queue),
		slog.Int("prefetch", opts.Prefetch),
		slog.Int("workers", opts.Workers),
	)

	// 停机时先 channel.Cancel 停止拉取新消息，由 Broker 关闭 deliveries 实现在途排空。
	sessionDone := make(chan struct{})
	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		select {
		case <-ctx.Done():
			slog.Info("rabbitmq consumer draining in-flight deliveries", slog.String("queue", opts.Queue))
			if err := ch.Cancel(opts.ConsumerTag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
				slog.Warn("rabbitmq consumer channel cancel failed",
					slog.String("queue", opts.Queue),
					slog.Any("err", err),
				)
			}
		case <-sessionDone:
		}
	}()

	var ackMu sync.Mutex
	var wg sync.WaitGroup
	for range opts.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for raw := range deliveries {
				handleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consumerHandleTimeout)
				d := Delivery{
					Body:       raw.Body,
					MessageID:  raw.MessageId,
					Headers:    raw.Headers,
					RetryCount: parseRetryCount(raw.Headers),
				}
				handleDelivery(handleCtx, d, lockedAcker{mu: &ackMu, inner: raw}, retryPub, opts, handler)
				cancel()
			}
		}()
	}

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-ctx.Done():
		drainTimer := time.NewTimer(consumerDrainTimeout)
		defer drainTimer.Stop()
		select {
		case <-waitDone:
		case <-drainTimer.C:
			slog.Warn("rabbitmq consumer drain timed out",
				slog.String("queue", opts.Queue),
				slog.Duration("timeout", consumerDrainTimeout),
			)
		}
	}

	close(sessionDone)
	<-cancelDone
	return nil
}

type lockedAcker struct {
	mu    *sync.Mutex
	inner deliveryAcker
}

func (a lockedAcker) Ack(multiple bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inner.Ack(multiple)
}

func (a lockedAcker) Nack(multiple, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inner.Nack(multiple, requeue)
}

func handleDelivery(
	ctx context.Context,
	d Delivery,
	acker deliveryAcker,
	retryPub Publisher,
	opts ConsumerOptions,
	handler MessageHandler,
) {
	err := handler(ctx, d)
	if err == nil {
		if ackErr := acker.Ack(false); ackErr != nil {
			slog.WarnContext(ctx, "rabbitmq ack delivery failed",
				slog.String("queue", opts.Queue),
				slog.String("message_id", d.MessageID),
				slog.Any("err", ackErr),
			)
		}
		return
	}

	if errors.Is(err, ErrPermanent) || d.RetryCount >= opts.MaxRetries || retryPub == nil || opts.RetryRoutingKey == "" {
		slog.ErrorContext(ctx, "rabbitmq delivery routed to dlq",
			slog.String("queue", opts.Queue),
			slog.String("message_id", d.MessageID),
			slog.Int("retry_count", d.RetryCount),
			slog.Bool("permanent", errors.Is(err, ErrPermanent)),
			slog.Any("err", err),
		)
		if nackErr := acker.Nack(false, false); nackErr != nil {
			slog.WarnContext(ctx, "rabbitmq nack to dlq failed",
				slog.String("queue", opts.Queue),
				slog.String("message_id", d.MessageID),
				slog.Any("err", nackErr),
			)
		}
		return
	}

	nextRetry := d.RetryCount + 1
	headers := make(amqp.Table, len(d.Headers)+2)
	maps.Copy(headers, d.Headers)
	headers[HeaderRetryCount] = int32(nextRetry)
	headers[HeaderLastError] = truncateError(err, maxHeaderErrorLen)

	pubErr := retryPub.Publish(ctx, PublishMessage{
		Exchange:   ExchangeDLX,
		RoutingKey: opts.RetryRoutingKey,
		MessageID:  d.MessageID,
		Body:       d.Body,
		Headers:    headers,
	})
	if pubErr != nil {
		// 重投重试队列失败时严禁提前 Ack，交由 Broker 回收重投防丢消息。
		slog.WarnContext(ctx, "rabbitmq publish to retry queue failed, requeuing",
			slog.String("queue", opts.Queue),
			slog.String("message_id", d.MessageID),
			slog.Int("next_retry", nextRetry),
			slog.Any("err", pubErr),
		)
		_ = acker.Nack(false, true)
		return
	}

	slog.WarnContext(ctx, "rabbitmq delivery scheduled for delayed retry",
		slog.String("queue", opts.Queue),
		slog.String("message_id", d.MessageID),
		slog.Int("retry_count", nextRetry),
		slog.Any("err", err),
	)
	if ackErr := acker.Ack(false); ackErr != nil {
		slog.WarnContext(ctx, "rabbitmq ack after retry publish failed",
			slog.String("queue", opts.Queue),
			slog.String("message_id", d.MessageID),
			slog.Any("err", ackErr),
		)
	}
}

func normalizeConsumerOptions(opts ConsumerOptions) ConsumerOptions {
	if opts.Prefetch <= 0 {
		opts.Prefetch = defaultConsumerPrefetch
	}
	if opts.Workers <= 0 {
		opts.Workers = defaultConsumerWorkers
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = defaultMaxRetries
	}
	return opts
}

func parseRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	raw, ok := headers[HeaderRetryCount]
	if !ok || raw == nil {
		return 0
	}
	var n int64
	switch v := raw.(type) {
	case int:
		n = int64(v)
	case int8:
		n = int64(v)
	case int16:
		n = int64(v)
	case int32:
		n = int64(v)
	case int64:
		n = v
	case uint:
		n = int64(v)
	case uint8:
		n = int64(v)
	case uint16:
		n = int64(v)
	case uint32:
		n = int64(v)
	case uint64:
		n = int64(v)
	case float64:
		n = int64(v)
	default:
		return 0
	}
	if n < 0 {
		return 0
	}
	return int(n)
}

func truncateError(err error, limit int) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) <= limit {
		return s
	}
	return s[:limit]
}
