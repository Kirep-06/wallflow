package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrPublishNacked 表示 Broker 显式 Nack 了发布消息（如磁盘/内存告警）。
var ErrPublishNacked = errors.New("mq: publish nacked by broker")

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

// ConfirmPublisher 封装并发安全、自动随 Connection 重建的 RabbitMQ Confirm 发布器。
type ConfirmPublisher struct {
	connMgr *ConnManager

	mu       sync.Mutex
	ch       *amqp.Channel
	connGen  uint64
	chClosed chan *amqp.Error
}

var _ Publisher = (*ConfirmPublisher)(nil)

// NewConfirmPublisher 构造基于 ConnManager 的持久化 Confirm 发布器。
func NewConfirmPublisher(connMgr *ConnManager) *ConfirmPublisher {
	return &ConfirmPublisher{connMgr: connMgr}
}

// Publish 以持久化模式发布消息并强制等待 RabbitMQ Broker Confirm Ack 后才返回 nil。
func (p *ConfirmPublisher) Publish(ctx context.Context, msg PublishMessage) error {
	if p == nil || p.connMgr == nil {
		return ErrNotConnected
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	ch, err := p.ensureChannelLocked()
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
		p.resetChannelLocked()
		return fmt.Errorf("mq publish: %w", err)
	}
	if confirm == nil {
		p.resetChannelLocked()
		return errors.New("mq publish: nil deferred confirmation")
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		p.resetChannelLocked()
		return fmt.Errorf("mq publish confirm wait: %w", err)
	}
	if !acked {
		return ErrPublishNacked
	}
	return nil
}

// Close 主动关闭内部 Confirm Channel。
func (p *ConfirmPublisher) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	ch := p.ch
	p.ch = nil
	p.chClosed = nil
	if ch != nil && !ch.IsClosed() {
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("mq close publisher channel: %w", err)
		}
	}
	return nil
}

func (p *ConfirmPublisher) ensureChannelLocked() (*amqp.Channel, error) {
	conn, gen, err := p.connMgr.CurrentConn()
	if err != nil {
		p.resetChannelLocked()
		return nil, err
	}

	if p.ch != nil && !p.ch.IsClosed() && p.connGen == gen {
		select {
		case <-p.chClosed:
			p.resetChannelLocked()
		default:
			return p.ch, nil
		}
	} else {
		p.resetChannelLocked()
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("mq open publish channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("mq enable publisher confirm: %w", err)
	}

	p.ch = ch
	p.connGen = gen
	p.chClosed = ch.NotifyClose(make(chan *amqp.Error, 1))
	return ch, nil
}

func (p *ConfirmPublisher) resetChannelLocked() {
	if p.ch != nil && !p.ch.IsClosed() {
		_ = p.ch.Close()
	}
	p.ch = nil
	p.chClosed = nil
}
