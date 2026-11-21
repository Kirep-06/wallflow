package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrConnectionUnavailable 表示当前 AMQP 连接断开或不可用，调用方可快速中止整批发布。
var ErrConnectionUnavailable = errors.New("amqp connection unavailable")

// IsConnectionError 判断错误是否属于 AMQP 连接级不可用。
func IsConnectionError(err error) bool {
	return errors.Is(err, ErrConnectionUnavailable) || errors.Is(err, amqp.ErrClosed) || errors.Is(err, ErrClientClosed)
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
type ConfirmPublisher struct {
	client *Client

	mu      sync.Mutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	chClose chan *amqp.Error
}

func NewConfirmPublisher(client *Client) *ConfirmPublisher {
	return &ConfirmPublisher{client: client}
}

// Publish 以持久化模式发布单条消息并等待 Publisher Confirm。
func (p *ConfirmPublisher) Publish(ctx context.Context, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch, conn, err := p.ensureChannelLocked()
	if err != nil {
		return err
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
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
		p.dropChannelLocked()
		if errors.Is(err, amqp.ErrClosed) || conn.IsClosed() {
			p.client.ReportError(conn)
			return fmt.Errorf("%w: %v", ErrConnectionUnavailable, err)
		}
		return fmt.Errorf("amqp publish: %w", err)
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil {
		p.dropChannelLocked()
		if errors.Is(err, amqp.ErrClosed) || conn.IsClosed() {
			p.client.ReportError(conn)
			return fmt.Errorf("%w: %v", ErrConnectionUnavailable, err)
		}
		return fmt.Errorf("amqp wait confirm: %w", err)
	}
	if !ok {
		return errors.New("amqp publish nacked by broker")
	}
	return nil
}

// Close 关闭当前缓存的发布通道。
func (p *ConfirmPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == nil {
		return nil
	}
	err := p.ch.Close()
	p.ch = nil
	p.conn = nil
	p.chClose = nil
	return err
}

func (p *ConfirmPublisher) ensureChannelLocked() (*amqp.Channel, *amqp.Connection, error) {
	conn := p.client.Connection()
	if conn == nil {
		return nil, nil, ErrConnectionUnavailable
	}
	if p.ch != nil && p.conn == conn && !p.ch.IsClosed() {
		select {
		case <-p.chClose:
			p.dropChannelLocked()
		default:
			return p.ch, p.conn, nil
		}
	} else {
		p.dropChannelLocked()
	}

	ch, err := conn.Channel()
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || conn.IsClosed() {
			p.client.ReportError(conn)
			return nil, nil, fmt.Errorf("%w: open confirm channel: %v", ErrConnectionUnavailable, err)
		}
		return nil, nil, fmt.Errorf("open confirm channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		if errors.Is(err, amqp.ErrClosed) || conn.IsClosed() {
			p.client.ReportError(conn)
			return nil, nil, fmt.Errorf("%w: enable confirm mode: %v", ErrConnectionUnavailable, err)
		}
		return nil, nil, fmt.Errorf("enable confirm mode: %w", err)
	}
	p.conn = conn
	p.ch = ch
	p.chClose = ch.NotifyClose(make(chan *amqp.Error, 1))
	return p.ch, p.conn, nil
}

func (p *ConfirmPublisher) dropChannelLocked() {
	if p.ch != nil && !p.ch.IsClosed() {
		_ = p.ch.Close()
	}
	p.ch = nil
	p.conn = nil
	p.chClose = nil
}
