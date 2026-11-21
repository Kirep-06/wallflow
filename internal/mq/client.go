// Package mq 封装 RabbitMQ (amqp091-go) 连接管理、拓扑声明、Publisher Confirms 与受控消费者。
package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/model"
)

const (
	dialTimeout         = 5 * time.Second
	setupTimeout        = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second
	defaultCloseTimeout = 2 * time.Second
	amqpHeartbeat       = 10 * time.Second
	initialBackoff      = 1 * time.Second
	maxReconnectBackoff = 30 * time.Second
)

// ErrClientClosed 表示 MQ 客户端已主动关闭。
var ErrClientClosed = errors.New("amqp client closed")

// deadlineConn 包装底层 net.Conn，提供运行期兜底写期限与可并发调用的强制断连能力。
// 当外部超时或主动关闭触发 forceClose 时，立即设置当前时间为 deadline 并关闭底层连接，
// 确保阻塞在 TCP Write/Read 中的系统调用立刻解除阻塞返回错误。
type deadlineConn struct {
	net.Conn
	closed       atomic.Bool
	writeTimeout atomic.Int64 // 纳秒；0 表示尚未启用运行期写超时（握手/拓扑阶段由整连接 deadline 控制）
}

func newDeadlineConn(c net.Conn) *deadlineConn {
	return &deadlineConn{Conn: c}
}

func (d *deadlineConn) enableWriteTimeout(timeout time.Duration) {
	if timeout > 0 {
		d.writeTimeout.Store(int64(timeout))
	}
}

func (d *deadlineConn) Write(p []byte) (int, error) {
	if d.closed.Load() {
		return 0, net.ErrClosed
	}
	wt := time.Duration(d.writeTimeout.Load())
	if wt > 0 {
		_ = d.Conn.SetWriteDeadline(time.Now().Add(wt))
	}
	n, err := d.Conn.Write(p)
	if wt > 0 && !d.closed.Load() {
		_ = d.Conn.SetWriteDeadline(time.Time{})
	}
	return n, err
}

func (d *deadlineConn) forceClose() {
	if d == nil {
		return
	}
	d.closed.Store(true)
	_ = d.Conn.SetDeadline(time.Now())
	_ = d.Conn.Close()
}

// managedConn 将 AMQP 连接与底层可控 net.Conn 绑定，确保超时中断与有界关闭能作用到真实 I/O。
type managedConn struct {
	amqpConn  *amqp.Connection
	rawConn   *deadlineConn
	forceOnce sync.Once
}

func (m *managedConn) forceClose() {
	if m == nil {
		return
	}
	m.forceOnce.Do(func() {
		if m.rawConn != nil {
			m.rawConn.forceClose()
		}
	})
}

func (m *managedConn) isClosed() bool {
	if m == nil || m.amqpConn == nil {
		return true
	}
	if m.rawConn != nil && m.rawConn.closed.Load() {
		return true
	}
	return m.amqpConn.IsClosed()
}

// closeGracefully 在给定 deadline 内优雅关闭 AMQP 连接；若因网络阻塞超时则强制关闭底层 socket，避免阻塞退出。
func (m *managedConn) closeGracefully(timeout time.Duration) error {
	if m == nil {
		return nil
	}
	if m.isClosed() {
		m.forceClose()
		return nil
	}
	if timeout <= 0 {
		timeout = defaultCloseTimeout
	}
	timer := time.AfterFunc(timeout, m.forceClose)
	defer timer.Stop()

	err := m.amqpConn.CloseDeadline(time.Now().Add(timeout))
	if err != nil {
		m.forceClose()
	}
	return err
}

// closeChannelBounded 在给定超时内关闭单个 AMQP Channel；若对端不回包导致阻塞，则强制断连解除阻塞。
func (m *managedConn) closeChannelBounded(ch *amqp.Channel, timeout time.Duration) error {
	if ch == nil || ch.IsClosed() {
		return nil
	}
	if m == nil || m.isClosed() {
		m.forceClose()
		return nil
	}
	if timeout <= 0 {
		timeout = defaultCloseTimeout
	}
	timer := time.AfterFunc(timeout, m.forceClose)
	defer timer.Stop()
	return ch.Close()
}

type dialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Client 维护单条 RabbitMQ TCP 连接，提供幂等拓扑声明、可执行超时控制与指数退避自动重连。
type Client struct {
	cfg          config.RabbitMQConfig
	sanitizedURL string

	dialFunc       dialContextFunc
	dialTimeout    time.Duration
	setupTimeout   time.Duration
	writeTimeout   time.Duration
	closeTimeout   time.Duration
	heartbeat      time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration

	baseCtx    context.Context
	baseCancel context.CancelFunc

	mu          sync.RWMutex
	conn        *managedConn
	closed      bool
	readyCh     chan struct{}
	closeCh     chan struct{}
	reconnectCh chan struct{}

	wg sync.WaitGroup
}

type clientOption func(*Client)

// NewClient 创建 RabbitMQ 连接管理器并声明队列拓扑。
// 若配置非法返回错误；若 RabbitMQ 启动时暂时不可达则记 Warn 并转入后台指数退避自动重连，避免阻断 API 降级启动。
func NewClient(cfg config.RabbitMQConfig) (*Client, error) {
	return newClientWithOptions(cfg)
}

func newClientWithOptions(cfg config.RabbitMQConfig, opts ...clientOption) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:            cfg,
		sanitizedURL:   cfg.SanitizedURL(),
		dialTimeout:    dialTimeout,
		setupTimeout:   setupTimeout,
		writeTimeout:   defaultWriteTimeout,
		closeTimeout:   defaultCloseTimeout,
		heartbeat:      amqpHeartbeat,
		initialBackoff: initialBackoff,
		maxBackoff:     maxReconnectBackoff,
		baseCtx:        baseCtx,
		baseCancel:     baseCancel,
		readyCh:        make(chan struct{}),
		closeCh:        make(chan struct{}),
		reconnectCh:    make(chan struct{}, 1),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	if c.dialFunc == nil {
		dt := c.dialTimeout
		c.dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{Timeout: dt}
			return d.DialContext(ctx, network, addr)
		}
	}

	if mc, err := c.dialAndDeclare(); err != nil {
		slog.Warn("initial rabbitmq connect failed, will retry in background", "addr", c.sanitizedURL, "err", err)
	} else {
		c.setConn(mc)
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
	mc, err := c.managedConnection()
	if err != nil || mc == nil {
		return nil
	}
	return mc.amqpConn
}

func (c *Client) managedConnection() (*managedConn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClientClosed
	}
	if c.conn == nil || c.conn.isClosed() {
		return nil, ErrConnectionUnavailable
	}
	return c.conn, nil
}

// WaitConnection 阻塞等待直到获取有效 AMQP 连接，或 ctx 取消 / Client 关闭。
func (c *Client) WaitConnection(ctx context.Context) (*amqp.Connection, error) {
	mc, err := c.waitManagedConnection(ctx)
	if err != nil {
		return nil, err
	}
	return mc.amqpConn, nil
}

func (c *Client) waitManagedConnection(ctx context.Context) (*managedConn, error) {
	for {
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return nil, ErrClientClosed
		}
		if c.conn != nil && !c.conn.isClosed() {
			mc := c.conn
			c.mu.RUnlock()
			return mc, nil
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

// ReportError 在发布或消费发现连接级故障时通知管理器强制摘除故障连接并触发重连（按连接引用比对，避免误杀新连接）。
func (c *Client) ReportError(badConn *amqp.Connection) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var toClose *managedConn
	if badConn != nil && c.conn != nil && c.conn.amqpConn == badConn {
		toClose = c.conn
		c.conn = nil
		c.resetReadyLocked()
	}
	c.mu.Unlock()

	if toClose != nil {
		// 故障连接不再等待 AMQP CloseOk 握手，立即关闭底层 socket 解除所有在途阻塞。
		toClose.forceClose()
	}
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

// Close 停止后台重连协程（立即中断在途拨号/拓扑声明）并在有界期限内关闭当前 AMQP 连接。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	mc := c.conn
	c.conn = nil
	close(c.closeCh)
	c.mu.Unlock()

	// 先取消 baseCtx，立刻打断 maintainLoop 中可能阻塞的拨号、握手或拓扑声明。
	c.baseCancel()

	var closeErr error
	if mc != nil {
		closeErr = mc.closeGracefully(c.closeTimeout)
	}
	c.wg.Wait()
	return closeErr
}

func (c *Client) maintainLoop() {
	defer c.wg.Done()
	backoff := c.initialBackoff

	for {
		mc, _ := c.managedConnection()
		if mc != nil {
			backoff = c.initialBackoff
			connClose := mc.amqpConn.NotifyClose(make(chan *amqp.Error, 1))
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
				c.clearIfSame(mc)
			case <-c.reconnectCh:
				if c.isClosed() {
					return
				}
				if cur, _ := c.managedConnection(); cur != nil {
					continue
				}
			}
		}

		if c.isClosed() {
			return
		}
		newMC, err := c.dialAndDeclare()
		if err != nil {
			if c.isClosed() {
				return
			}
			slog.Warn("rabbitmq reconnect failed", "addr", c.sanitizedURL, "retry_in", backoff, "err", err)
			timer := time.NewTimer(backoff)
			select {
			case <-c.closeCh:
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, c.maxBackoff)
			continue
		}
		if !c.setConn(newMC) {
			newMC.forceClose()
			return
		}
		backoff = c.initialBackoff
		slog.Info("rabbitmq reconnected", "addr", c.sanitizedURL)
	}
}

func (c *Client) dialAndDeclare() (*managedConn, error) {
	totalTimeout := c.dialTimeout + c.setupTimeout
	if totalTimeout <= 0 {
		totalTimeout = dialTimeout + setupTimeout
	}
	dialCtx, dialCancel := context.WithTimeout(c.baseCtx, totalTimeout)
	defer dialCancel()

	var rawMu sync.Mutex
	var captured *deadlineConn
	stopCancelWatch := context.AfterFunc(dialCtx, func() {
		rawMu.Lock()
		dc := captured
		rawMu.Unlock()
		if dc != nil {
			dc.forceClose()
		}
	})
	defer stopCancelWatch()

	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{
		Heartbeat: c.heartbeat,
		Dial: func(network, addr string) (net.Conn, error) {
			nc, err := c.dialFunc(dialCtx, network, addr)
			if err != nil {
				return nil, err
			}
			dc := newDeadlineConn(nc)
			rawMu.Lock()
			captured = dc
			rawMu.Unlock()
			if dialCtx.Err() != nil {
				dc.forceClose()
				return nil, dialCtx.Err()
			}
			// 为 TLS/AMQP 握手设置期限（openComplete 后会清零）。
			if err := dc.SetDeadline(time.Now().Add(c.dialTimeout)); err != nil {
				dc.forceClose()
				return nil, err
			}
			return dc, nil
		},
	})
	if err != nil {
		rawMu.Lock()
		dc := captured
		rawMu.Unlock()
		if dc != nil {
			dc.forceClose()
		}
		return nil, fmt.Errorf("dial rabbitmq %s: %w", c.sanitizedURL, err)
	}

	rawMu.Lock()
	dc := captured
	rawMu.Unlock()
	if dc == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dial rabbitmq %s: missing underlying connection", c.sanitizedURL)
	}

	mc := &managedConn{amqpConn: conn, rawConn: dc}

	// 为建拓扑通道与 DeclareTopology 施加显式 I/O 期限，防止握手后 Broker 挂起永久阻塞。
	setupDeadline := time.Now().Add(c.setupTimeout)
	if ctxDeadline, ok := dialCtx.Deadline(); ok && ctxDeadline.Before(setupDeadline) {
		setupDeadline = ctxDeadline
	}
	if err := dc.SetDeadline(setupDeadline); err != nil {
		mc.forceClose()
		return nil, fmt.Errorf("set topology deadline: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		mc.forceClose()
		return nil, fmt.Errorf("open topology channel: %w", err)
	}
	if err := DeclareTopology(ch, c.cfg); err != nil {
		mc.forceClose()
		return nil, err
	}
	if err := ch.Close(); err != nil {
		mc.forceClose()
		return nil, fmt.Errorf("close topology channel: %w", err)
	}

	// 停止建连期 cancel watchdog，并确认期间未被取消/强关，避免返回已断连接。
	stopped := stopCancelWatch()
	if !stopped || dialCtx.Err() != nil || dc.closed.Load() {
		mc.forceClose()
		if dialCtx.Err() != nil {
			return nil, fmt.Errorf("dial rabbitmq %s: %w", c.sanitizedURL, dialCtx.Err())
		}
		return nil, fmt.Errorf("dial rabbitmq %s: %w", c.sanitizedURL, ErrConnectionUnavailable)
	}

	// 清除握手/拓扑总 deadline，启用运行期底层写期限兜底。
	if err := dc.SetDeadline(time.Time{}); err != nil {
		mc.forceClose()
		return nil, fmt.Errorf("clear topology deadline: %w", err)
	}
	dc.enableWriteTimeout(c.writeTimeout)
	return mc, nil
}

func (c *Client) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Client) setConn(mc *managedConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.conn = mc
	select {
	case <-c.readyCh:
	default:
		close(c.readyCh)
	}
	return true
}

func (c *Client) clearIfSame(mc *managedConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == mc {
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
