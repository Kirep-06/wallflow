package mq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrConnectionUnavailable 表示当前 AMQP 连接断开、不可用或网络写/确认超时导致断连，调用方可快速中止整批发布。
var ErrConnectionUnavailable = errors.New("amqp connection unavailable")

// ErrPublishNacked 表示 Broker 明确拒绝确认（Nack）该条发布消息。
var ErrPublishNacked = errors.New("amqp publish nacked by broker")

// IsConnectionError 判断错误是否属于基础设施连接级或传输层故障（而非单条事件自身的业务/拓扑错误）。
// 注意：调用方主动取消上下文（context.Canceled）不属于连接错误。
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrConnectionUnavailable) ||
		errors.Is(err, amqp.ErrClosed) ||
		errors.Is(err, ErrClientClosed) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case amqp.ConnectionForced, amqp.FrameError, amqp.ChannelError, amqp.UnexpectedFrame, amqp.InternalError:
			return true
		}
	}
	return false
}

// Message 是待投递到 RabbitMQ 的持久化消息。
type Message struct {
	Exchange   string
	RoutingKey string
	MessageID  string
	Body       []byte
	Headers    amqp.Table
}

// Publisher 抽象带可靠确认的消息发布接口（便于 OutboxRelay 与消费者重试复用及单测替换）。
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// ConfirmPublisher 在单条 Confirm Channel 上串行投递消息并等待 Broker Publisher Confirm。
// 所有网络写入、通道建立、确认等待与关闭流程均受可执行期限约束：
// 当 ctx 超时/取消或 Close() 被调用时，立即强制中断底层 net.Conn，避免阻塞在 amqp091-go I/O 或锁上拖住重连与退出。
type ConfirmPublisher struct {
	client       *Client
	sem          chan struct{}
	closeTimeout time.Duration

	stateMu      sync.Mutex
	closed       bool
	closeCh      chan struct{}
	activeCancel context.CancelFunc
	activeConn   *managedConn

	// 以下字段由 sem 串行通道独占保护。
	conn    *managedConn
	ch      *amqp.Channel
	chClose chan *amqp.Error
}

func NewConfirmPublisher(client *Client) *ConfirmPublisher {
	ct := defaultCloseTimeout
	if client != nil && client.closeTimeout > 0 {
		ct = client.closeTimeout
	}
	return &ConfirmPublisher{
		client:       client,
		sem:          make(chan struct{}, 1),
		closeTimeout: ct,
		closeCh:      make(chan struct{}),
	}
}

// Publish 以持久化模式发布单条消息并等待 Publisher Confirm。
func (p *ConfirmPublisher) Publish(ctx context.Context, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.isClosed() {
		return ErrClientClosed
	}
	if err := ctx.Err(); err != nil {
		return p.wrapContextErr(err)
	}

	// 获取串行发布权时同样响应 ctx 与 Close，避免等待前序阻塞发布拖死调用方或退出流程。
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-p.closeCh:
		return ErrClientClosed
	case <-ctx.Done():
		return p.wrapContextErr(ctx.Err())
	}

	if p.isClosed() {
		return ErrClientClosed
	}
	if err := ctx.Err(); err != nil {
		return p.wrapContextErr(err)
	}

	mc, err := p.client.managedConnection()
	if err != nil {
		return err
	}

	opCtx, opCancel := context.WithCancel(ctx)
	defer opCancel()

	p.stateMu.Lock()
	if p.closed {
		p.stateMu.Unlock()
		return ErrClientClosed
	}
	p.activeCancel = opCancel
	p.activeConn = mc
	p.stateMu.Unlock()
	defer func() {
		p.stateMu.Lock()
		p.activeCancel = nil
		p.activeConn = nil
		p.stateMu.Unlock()
	}()

	// 硬期限看门狗：无论是 Channel()、Confirm()、Publish 写帧还是 WaitConfirm 阶段，
	// 一旦 opCtx 超时/取消或 Close() 触发，立即强关底层 net.Conn 解除内核 I/O 阻塞并通知 Client 重连。
	var interrupted atomic.Bool
	stopWatch := context.AfterFunc(opCtx, func() {
		interrupted.Store(true)
		mc.forceClose()
		p.client.ReportError(mc.amqpConn)
	})
	defer func() {
		if !stopWatch() {
			// 看门狗已在收尾瞬间触发并强关了底层连接，清理本地通道引用避免下一次误用。
			p.clearChannelRefs()
		}
	}()

	ch, err := p.ensureChannel(mc)
	if err != nil {
		return p.handleOpError(ctx, mc, err, interrupted.Load(), "open confirm channel")
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		opCtx,
		msg.Exchange,
		msg.RoutingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			MessageId:    msg.MessageID,
			Headers:      msg.Headers,
			Body:         msg.Body,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return p.handleOpError(ctx, mc, err, interrupted.Load(), "amqp publish")
	}

	ok, err := confirm.WaitContext(opCtx)
	if err != nil {
		return p.handleOpError(ctx, mc, err, interrupted.Load(), "amqp wait confirm")
	}
	if !ok {
		return ErrPublishNacked
	}
	return nil
}

// Close 立即中断任何在途阻塞发布，并在有界期限内关闭当前缓存的发布通道。
func (p *ConfirmPublisher) Close() error {
	p.stateMu.Lock()
	if p.closed {
		p.stateMu.Unlock()
		return nil
	}
	p.closed = true
	close(p.closeCh)
	cancel := p.activeCancel
	activeConn := p.activeConn
	p.stateMu.Unlock()

	// 先在 sem 锁外立刻打断在途 Publish 的底层 I/O，绝不被阻塞发布锁死。
	if cancel != nil {
		cancel()
	}
	if activeConn != nil {
		activeConn.forceClose()
	}

	timeout := p.getCloseTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-timer.C:
		return fmt.Errorf("close confirm publisher timed out: %w", context.DeadlineExceeded)
	}

	ch := p.ch
	mc := p.conn
	p.clearChannelRefs()
	if ch == nil || ch.IsClosed() {
		return nil
	}
	return mc.closeChannelBounded(ch, timeout)
}

func (p *ConfirmPublisher) ensureChannel(mc *managedConn) (*amqp.Channel, error) {
	if p.ch != nil && p.conn == mc && !p.ch.IsClosed() {
		select {
		case <-p.chClose:
			p.dropChannelBounded()
		default:
			return p.ch, nil
		}
	} else {
		p.dropChannelBounded()
	}

	ch, err := mc.amqpConn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = mc.closeChannelBounded(ch, p.getCloseTimeout())
		return nil, err
	}
	p.conn = mc
	p.ch = ch
	p.chClose = ch.NotifyClose(make(chan *amqp.Error, 1))
	return ch, nil
}

func (p *ConfirmPublisher) handleOpError(ctx context.Context, mc *managedConn, opErr error, interrupted bool, action string) error {
	if p.isClosed() {
		p.clearChannelRefs()
		return ErrClientClosed
	}

	ctxErr := ctx.Err()
	connFailed := interrupted || mc.isClosed() || IsConnectionError(opErr) || ctxErr != nil
	if connFailed {
		p.clearChannelRefs()
		mc.forceClose()
		p.client.ReportError(mc.amqpConn)

		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %s: %w", ErrConnectionUnavailable, action, ctxErr)
		}
		if errors.Is(ctxErr, context.Canceled) {
			return ctxErr
		}
		if IsConnectionError(opErr) {
			if errors.Is(opErr, ErrConnectionUnavailable) || errors.Is(opErr, ErrClientClosed) {
				return opErr
			}
			return fmt.Errorf("%w: %s: %w", ErrConnectionUnavailable, action, opErr)
		}
		return fmt.Errorf("%w: %s: %w", ErrConnectionUnavailable, action, opErr)
	}

	// 连接仍健康但发生 Channel 级非连接异常（如 404/403/406）：仅丢弃坏通道，保留连接并透传非连接错误。
	p.dropChannelBounded()
	return fmt.Errorf("%s: %w", action, opErr)
}

func (p *ConfirmPublisher) dropChannelBounded() {
	ch := p.ch
	mc := p.conn
	p.clearChannelRefs()
	if ch == nil || ch.IsClosed() || mc == nil || mc.isClosed() {
		return
	}
	_ = mc.closeChannelBounded(ch, p.getCloseTimeout())
}

func (p *ConfirmPublisher) clearChannelRefs() {
	p.ch = nil
	p.conn = nil
	p.chClose = nil
}

func (p *ConfirmPublisher) isClosed() bool {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	return p.closed
}

func (p *ConfirmPublisher) getCloseTimeout() time.Duration {
	if p.closeTimeout > 0 {
		return p.closeTimeout
	}
	return defaultCloseTimeout
}

func (p *ConfirmPublisher) wrapContextErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrConnectionUnavailable, err)
	}
	return err
}
