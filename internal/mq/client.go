// Package mq 封装 RabbitMQ (amqp091-go) 连接管理、拓扑声明、Publisher Confirms 与受控消费者。
package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
)

const (
	dialTimeout         = 5 * time.Second
	amqpHeartbeat       = 10 * time.Second
	initialBackoff      = 1 * time.Second
	maxReconnectBackoff = 30 * time.Second
)

// ErrClientClosed 表示 MQ 客户端已主动关闭。
var ErrClientClosed = errors.New("amqp client closed")

// Client 维护单条 RabbitMQ TCP 连接，提供幂等拓扑声明与指数退避自动重连。
type Client struct {
	cfg          config.RabbitMQConfig
	sanitizedURL string

	mu          sync.RWMutex
	conn        *amqp.Connection
	closed      bool
	readyCh     chan struct{}
	closeCh     chan struct{}
	reconnectCh chan struct{}

	wg sync.WaitGroup
}

// NewClient 创建 RabbitMQ 连接管理器并声明队列拓扑。
// 若配置非法返回错误；若 RabbitMQ 启动时暂时不可达则记 Warn 并转入后台指数退避自动重连，避免阻断 API 降级启动。
func NewClient(cfg config.RabbitMQConfig) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := &Client{
		cfg:          cfg,
		sanitizedURL: cfg.SanitizedURL(),
		readyCh:      make(chan struct{}),
		closeCh:      make(chan struct{}),
		reconnectCh:  make(chan struct{}, 1),
	}
	if conn, err := c.dialAndDeclare(); err != nil {
		slog.Warn("initial rabbitmq connect failed, will retry in background", "addr", c.sanitizedURL, "err", err)
	} else {
		c.setConn(conn)
		slog.Info("rabbitmq connected", "addr", c.sanitizedURL)
	}
	c.wg.Add(1)
	go c.maintainLoop()
	return c, nil
}

// Config 返回当前 MQ 配置副本。
func (c *Client) Config() config.RabbitMQConfig { return c.cfg }

// Connection 返回当前可用连接；若正处于断线重连期则返回 nil。
func (c *Client) Connection() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed || c.conn == nil || c.conn.IsClosed() {
		return nil
	}
	return c.conn
}

// WaitConnection 阻塞等待直到获取有效 AMQP 连接，或 ctx 取消 / Client 关闭。
func (c *Client) WaitConnection(ctx context.Context) (*amqp.Connection, error) {
	for {
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return nil, ErrClientClosed
		}
		if c.conn != nil && !c.conn.IsClosed() {
			conn := c.conn
			c.mu.RUnlock()
			return conn, nil
		}
		ready := c.readyCh
		c.mu.RUnlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closeCh:
			return nil, ErrClientClosed
		case <-ready:
		}
	}
}

// ReportError 在发布或消费发现连接级故障时通知管理器触发重连（按连接引用比对，避免误杀新连接）。
func (c *Client) ReportError(badConn *amqp.Connection) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var toClose *amqp.Connection
	if badConn != nil && c.conn == badConn {
		toClose = c.conn
		c.conn = nil
		c.resetReadyLocked()
	}
	c.mu.Unlock()

	if toClose != nil {
		_ = toClose.Close()
	}
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

// Close 停止后台重连协程并关闭当前 AMQP 连接。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	close(c.closeCh)
	c.mu.Unlock()

	var closeErr error
	if conn != nil && !conn.IsClosed() {
		closeErr = conn.Close()
	}
	c.wg.Wait()
	return closeErr
}

func (c *Client) maintainLoop() {
	defer c.wg.Done()
	backoff := initialBackoff

	for {
		conn := c.Connection()
		if conn != nil {
			backoff = initialBackoff
			connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case <-c.closeCh:
				return
			case err, ok := <-connClose:
				if c.isClosed() {
					return
				}
				if ok && err != nil {
					slog.Warn("rabbitmq connection closed, reconnecting", "addr", c.sanitizedURL, "err", err)
				} else {
					slog.Warn("rabbitmq connection dropped, reconnecting", "addr", c.sanitizedURL)
				}
				c.clearIfSame(conn)
			case <-c.reconnectCh:
				if c.isClosed() {
					return
				}
				if c.Connection() != nil {
					continue
				}
			}
		}

		if c.isClosed() {
			return
		}
		newConn, err := c.dialAndDeclare()
		if err != nil {
			slog.Warn("rabbitmq reconnect failed", "addr", c.sanitizedURL, "retry_in", backoff, "err", err)
			timer := time.NewTimer(backoff)
			select {
			case <-c.closeCh:
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, maxReconnectBackoff)
			continue
		}
		if !c.setConn(newConn) {
			_ = newConn.Close()
			return
		}
		backoff = initialBackoff
		slog.Info("rabbitmq reconnected", "addr", c.sanitizedURL)
	}
}

func (c *Client) dialAndDeclare() (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{
		Heartbeat: amqpHeartbeat,
		Dial:      amqp.DefaultDial(dialTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq %s: %w", c.sanitizedURL, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open topology channel: %w", err)
	}
	if err := DeclareTopology(ch, c.cfg); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	_ = ch.Close()
	return conn, nil
}

func (c *Client) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Client) setConn(conn *amqp.Connection) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.conn = conn
	select {
	case <-c.readyCh:
	default:
		close(c.readyCh)
	}
	return true
}

func (c *Client) clearIfSame(conn *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.conn = nil
		c.resetReadyLocked()
	}
}

func (c *Client) resetReadyLocked() {
	select {
	case <-c.readyCh:
		c.readyCh = make(chan struct{})
	default:
	}
}

// DeclareTopology 幂等声明模块 04 所需的 Exchange、主队列、延迟重试队列与死信队列绑定关系。
func DeclareTopology(ch *amqp.Channel, cfg config.RabbitMQConfig) error {
	if err := ch.ExchangeDeclare(
		cfg.Exchange,
		"topic",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w", cfg.Exchange, err)
	}

	if err := ch.ExchangeDeclare(
		cfg.DLXExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dlx exchange %s: %w", cfg.DLXExchange, err)
	}

	if _, err := ch.QueueDeclare(
		cfg.DLQQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dlq queue %s: %w", cfg.DLQQueue, err)
	}
	if err := ch.QueueBind(
		cfg.DLQQueue,
		cfg.FanoutQueue,
		cfg.DLXExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dlq queue %s: %w", cfg.DLQQueue, err)
	}

	if _, err := ch.QueueDeclare(
		cfg.FanoutQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    cfg.DLXExchange,
			"x-dead-letter-routing-key": cfg.FanoutQueue,
		},
	); err != nil {
		return fmt.Errorf("declare fanout queue %s: %w", cfg.FanoutQueue, err)
	}
	if err := ch.QueueBind(
		cfg.FanoutQueue,
		model.EventTypePostCreated,
		cfg.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind fanout queue %s: %w", cfg.FanoutQueue, err)
	}

	// 延迟重试队列到期后走默认交换机 "" 点对点回流主队列，避免多队列绑定 post.created 时重复广播。
	if _, err := ch.QueueDeclare(
		cfg.RetryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":             int32(cfg.RetryDelayMs),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": cfg.FanoutQueue,
		},
	); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", cfg.RetryQueue, err)
	}
	return nil
}
