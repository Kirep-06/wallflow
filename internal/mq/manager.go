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
	// ErrNotConnected 表示当前 RabbitMQ Connection 处于断线重连中。
	ErrNotConnected = errors.New("mq: not connected")
	// ErrClosed 表示连接管理器已被主动关闭。
	ErrClosed = errors.New("mq: manager closed")
)

// ConnManager 封装真正的 Connection 级 RabbitMQ 自动重连与拓扑幂等声明管理。
type ConnManager struct {
	cfg config.RabbitMQConfig

	mu          sync.RWMutex
	conn        *amqp.Connection
	gen         uint64
	readyCh     chan struct{}
	readyClosed bool
	closed      bool

	stopCancel context.CancelFunc
	loopDone   chan struct{}
}

// NewConnManager 启动 Connection 级重连管理器。
// 若启动时 RabbitMQ 暂不可达，仅记录告警并转入后台指数退避重连，绝不阻断进程启动。
func NewConnManager(cfg config.RabbitMQConfig) *ConnManager {
	loopCtx, cancel := context.WithCancel(context.Background())
	m := &ConnManager{
		cfg:        cfg,
		readyCh:    make(chan struct{}),
		stopCancel: cancel,
		loopDone:   make(chan struct{}),
	}

	if conn, err := m.dialAndDeclare(); err != nil {
		slog.Warn("rabbitmq initial connect failed, retrying in background",
			slog.String("addr", cfg.SafeAddr()),
			slog.Any("err", err),
		)
	} else {
		m.setConn(conn)
		slog.Info("rabbitmq connected and topology declared",
			slog.String("addr", cfg.SafeAddr()),
		)
	}

	go m.maintainLoop(loopCtx)
	return m
}

// CurrentConn 返回当前活跃连接及其代际编号；断连时返回 ErrNotConnected。
func (m *ConnManager) CurrentConn() (*amqp.Connection, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, 0, ErrClosed
	}
	if m.conn == nil || m.conn.IsClosed() {
		return nil, m.gen, ErrNotConnected
	}
	return m.conn, m.gen, nil
}

// WaitReady 阻塞等待直至连接可用、管理器关闭或 ctx 取消。
func (m *ConnManager) WaitReady(ctx context.Context) (*amqp.Connection, uint64, error) {
	for {
		m.mu.RLock()
		if m.closed {
			m.mu.RUnlock()
			return nil, 0, ErrClosed
		}
		if m.conn != nil && !m.conn.IsClosed() {
			conn, gen := m.conn, m.gen
			m.mu.RUnlock()
			return conn, gen, nil
		}
		waitCh := m.readyCh
		m.mu.RUnlock()

		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-waitCh:
		}
	}
}

// Close 停止后台重连循环并关闭底层 RabbitMQ TCP 连接。
func (m *ConnManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	if !m.readyClosed {
		close(m.readyCh)
		m.readyClosed = true
	}
	cancel := m.stopCancel
	m.mu.Unlock()

	cancel()
	<-m.loopDone

	m.mu.Lock()
	conn := m.conn
	m.conn = nil
	m.mu.Unlock()

	if conn != nil && !conn.IsClosed() {
		if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("mq close connection: %w", err)
		}
	}
	return nil
}

func (m *ConnManager) maintainLoop(ctx context.Context) {
	defer close(m.loopDone)

	delay := reconnectMinDelay
	for {
		conn, _, err := m.CurrentConn()
		if err == nil {
			delay = reconnectMinDelay
			closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
			select {
			case <-ctx.Done():
				return
			case amqpErr, ok := <-closeCh:
				m.clearConn(conn)
				if ctx.Err() != nil {
					return
				}
				slog.Warn("rabbitmq connection closed, starting reconnect",
					slog.String("addr", m.cfg.SafeAddr()),
					slog.Bool("has_err", ok && amqpErr != nil),
					slog.Any("err", amqpErr),
				)
			}
			continue
		}
		if errors.Is(err, ErrClosed) || ctx.Err() != nil {
			return
		}

		wait := jitteredBackoff(delay)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		newConn, dialErr := m.dialAndDeclare()
		if dialErr != nil {
			slog.Warn("rabbitmq reconnect failed",
				slog.String("addr", m.cfg.SafeAddr()),
				slog.Duration("next_retry_base", delay),
				slog.Any("err", dialErr),
			)
			delay = min(delay*2, reconnectMaxDelay)
			continue
		}

		if !m.setConn(newConn) {
			_ = newConn.Close()
			return
		}
		delay = reconnectMinDelay
		slog.Info("rabbitmq reconnected and topology re-declared",
			slog.String("addr", m.cfg.SafeAddr()),
		)
	}
}

func (m *ConnManager) dialAndDeclare() (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(m.cfg.URI(), amqp.Config{
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

func (m *ConnManager) setConn(conn *amqp.Connection) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return false
	}
	m.conn = conn
	m.gen++
	if !m.readyClosed {
		close(m.readyCh)
		m.readyClosed = true
	}
	return true
}

func (m *ConnManager) clearConn(old *amqp.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed || m.conn != old {
		return
	}
	m.conn = nil
	m.gen++
	if m.readyClosed {
		m.readyCh = make(chan struct{})
		m.readyClosed = false
	}
}

func jitteredBackoff(base time.Duration) time.Duration {
	factor := 1 + (rand.Float64()*2-1)*reconnectJitterPct
	return max(time.Duration(float64(base)*factor), 100*time.Millisecond)
}
