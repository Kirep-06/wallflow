package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
)

const (
	dialTimeout        = 5 * time.Second
	heartbeatInterval  = 10 * time.Second
	reconnectMinDelay  = time.Second
	reconnectMaxDelay  = 30 * time.Second
	reconnectJitterPct = 0.2
)

var (
	ErrNotConnected  = errors.New("mq: not connected")
	ErrClosed        = errors.New("mq: client closed")
	ErrPublishNacked = errors.New("mq: publish nacked by broker")
)

// PublishMessage 描述一次持久化发布的完整消息元数据与载荷。
type PublishMessage struct {
	Exchange   string
	RoutingKey string
	MessageID  string
	Body       []byte
	Headers    amqp.Table
}

// Publisher 定义外部消息发布边界接口（隔离 amqp091-go 便于离线单测）。
type Publisher interface {
	Publish(ctx context.Context, msg PublishMessage) error
}

// Client 统一封装 Connection 级自动重连、幂等拓扑声明、并发安全 Confirm 发布与消息消费入口。
type Client struct {
	cfg config.RabbitMQConfig

	connMu      sync.RWMutex
	conn        *amqp.Connection
	gen         uint64
	readyCh     chan struct{}
	readyClosed bool
	closed      bool

	pubMu     sync.Mutex
	pubCh     *amqp.Channel
	pubGen    uint64
	pubClosed chan *amqp.Error

	stopCancel context.CancelFunc
	loopDone   chan struct{}
}

var _ Publisher = (*Client)(nil)

// NewClient 启动 RabbitMQ 客户端；启动时 MQ 暂不可达仅记录告警并转入后台重连，不阻断进程启动。
func NewClient(cfg config.RabbitMQConfig) *Client {
	loopCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:        cfg,
		readyCh:    make(chan struct{}),
		stopCancel: cancel,
		loopDone:   make(chan struct{}),
	}

	if conn, err := c.dialAndDeclare(); err != nil {
		slog.Warn("rabbitmq initial connect failed, retrying in background",
			slog.String("addr", cfg.SafeAddr()),
			slog.Any("err", err),
		)
	} else {
		c.setConn(conn)
		slog.Info("rabbitmq connected and topology declared", slog.String("addr", cfg.SafeAddr()))
	}

	go c.maintainLoop(loopCtx)
	return c
}

// Publish 以持久化模式发布消息并强制等待 Broker Confirm Ack 后返回 nil。
func (c *Client) Publish(ctx context.Context, msg PublishMessage) error {
	if c == nil {
		return ErrNotConnected
	}

	c.pubMu.Lock()
	defer c.pubMu.Unlock()

	ch, err := c.ensurePubChannelLocked()
	if err != nil {
		return err
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		msg.Exchange,
		msg.RoutingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			MessageId:    msg.MessageID,
			Headers:      msg.Headers,
			Timestamp:    time.Now().UTC(),
			Body:         msg.Body,
		},
	)
	if err != nil {
		c.resetPubChannelLocked()
		return fmt.Errorf("mq publish: %w", err)
	}
	if confirm == nil {
		c.resetPubChannelLocked()
		return errors.New("mq publish: nil deferred confirmation")
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		c.resetPubChannelLocked()
		return fmt.Errorf("mq publish confirm wait: %w", err)
	}
	if !acked {
		return ErrPublishNacked
	}
	return nil
}

// Close 停止后台重连循环并依次关闭发布 Channel 与底层 TCP Connection。
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.connMu.Lock()
	if c.closed {
		c.connMu.Unlock()
		return nil
	}
	c.closed = true
	if !c.readyClosed {
		close(c.readyCh)
		c.readyClosed = true
	}
	cancel := c.stopCancel
	c.connMu.Unlock()

	cancel()
	<-c.loopDone

	var errs []error
	c.pubMu.Lock()
	if c.pubCh != nil && !c.pubCh.IsClosed() {
		if err := c.pubCh.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, fmt.Errorf("mq close publish channel: %w", err))
		}
	}
	c.pubCh = nil
	c.pubClosed = nil
	c.pubMu.Unlock()

	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()

	if conn != nil && !conn.IsClosed() {
		if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, fmt.Errorf("mq close connection: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (c *Client) currentConn() (*amqp.Connection, uint64, error) {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	if c.closed {
		return nil, 0, ErrClosed
	}
	if c.conn == nil || c.conn.IsClosed() {
		return nil, c.gen, ErrNotConnected
	}
	return c.conn, c.gen, nil
}

func (c *Client) waitReady(ctx context.Context) (*amqp.Connection, error) {
	for {
		c.connMu.RLock()
		if c.closed {
			c.connMu.RUnlock()
			return nil, ErrClosed
		}
		if c.conn != nil && !c.conn.IsClosed() {
			conn := c.conn
			c.connMu.RUnlock()
			return conn, nil
		}
		waitCh := c.readyCh
		c.connMu.RUnlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waitCh:
		}
	}
}

func (c *Client) ensurePubChannelLocked() (*amqp.Channel, error) {
	conn, gen, err := c.currentConn()
	if err != nil {
		c.resetPubChannelLocked()
		return nil, err
	}

	if c.pubCh != nil && !c.pubCh.IsClosed() && c.pubGen == gen {
		select {
		case <-c.pubClosed:
			c.resetPubChannelLocked()
		default:
			return c.pubCh, nil
		}
	} else {
		c.resetPubChannelLocked()
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("mq open publish channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("mq enable publisher confirm: %w", err)
	}

	c.pubCh = ch
	c.pubGen = gen
	c.pubClosed = ch.NotifyClose(make(chan *amqp.Error, 1))
	return ch, nil
}

func (c *Client) resetPubChannelLocked() {
	if c.pubCh != nil && !c.pubCh.IsClosed() {
		_ = c.pubCh.Close()
	}
	c.pubCh = nil
	c.pubClosed = nil
}

func (c *Client) maintainLoop(ctx context.Context) {
	defer close(c.loopDone)

	delay := reconnectMinDelay
	for {
		conn, _, err := c.currentConn()
		if err == nil {
			delay = reconnectMinDelay
			closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case <-ctx.Done():
				return
			case amqpErr, ok := <-closeCh:
				c.clearConn(conn)
				if ctx.Err() != nil {
					return
				}
				slog.Warn("rabbitmq connection closed, starting reconnect",
					slog.String("addr", c.cfg.SafeAddr()),
					slog.Bool("has_err", ok && amqpErr != nil),
					slog.Any("err", amqpErr),
				)
			}
			continue
		}
		if errors.Is(err, ErrClosed) || ctx.Err() != nil {
			return
		}

		timer := time.NewTimer(jitteredBackoff(delay))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		newConn, dialErr := c.dialAndDeclare()
		if dialErr != nil {
			slog.Warn("rabbitmq reconnect failed",
				slog.String("addr", c.cfg.SafeAddr()),
				slog.Duration("next_retry_base", delay),
				slog.Any("err", dialErr),
			)
			delay = min(delay*2, reconnectMaxDelay)
			continue
		}

		if !c.setConn(newConn) {
			_ = newConn.Close()
			return
		}
		delay = reconnectMinDelay
		slog.Info("rabbitmq reconnected and topology re-declared", slog.String("addr", c.cfg.SafeAddr()))
	}
}

func (c *Client) dialAndDeclare() (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(c.cfg.URI(), amqp.Config{
		Heartbeat: heartbeatInterval,
		Locale:    "en_US",
		Dial:      amqp.DefaultDial(dialTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open topology channel: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	_ = ch.Close()
	return conn, nil
}

func (c *Client) setConn(conn *amqp.Connection) bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.closed {
		return false
	}
	c.conn = conn
	c.gen++
	if !c.readyClosed {
		close(c.readyCh)
		c.readyClosed = true
	}
	return true
}

func (c *Client) clearConn(old *amqp.Connection) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.closed || c.conn != old {
		return
	}
	c.conn = nil
	c.gen++
	if c.readyClosed {
		c.readyCh = make(chan struct{})
		c.readyClosed = false
	}
}

func jitteredBackoff(base time.Duration) time.Duration {
	factor := 1 + (rand.Float64()*2-1)*reconnectJitterPct
	return max(time.Duration(float64(base)*factor), 100*time.Millisecond)
}
