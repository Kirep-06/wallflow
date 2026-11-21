package model

import "time"

const (
	OutboxStatusPending int8 = 0 // 待投递
	OutboxStatusSent    int8 = 1 // 已投递
	OutboxStatusDead    int8 = 2 // 超过最大重试次数的失败记录（死信）
)

const (
	EventTypePostCreated = "post.created"
)

// OutboxEvent 对应 MySQL outbox_events 本地事务消息表。
// Payload 使用 string 避免 go-sql-driver/mysql 将 []byte 当作 binary 写入 JSON 列触发字符集报错。
type OutboxEvent struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	EventID      string    `gorm:"column:event_id;size:64;not null;uniqueIndex:uk_event_id"`
	ExchangeName string    `gorm:"column:exchange_name;size:64;not null"`
	RoutingKey   string    `gorm:"column:routing_key;size:64;not null"`
	Payload      string    `gorm:"column:payload;type:json;not null"`
	Status       int8      `gorm:"column:status;not null;default:0"`
	RetryCount   uint16    `gorm:"column:retry_count;not null;default:0"`
	NextRetryAt  time.Time `gorm:"column:next_retry_at;precision:3;not null"`
	LastError    string    `gorm:"column:last_error;size:255;not null;default:''"`
	CreatedAt    time.Time `gorm:"column:created_at;precision:3;not null"`
	UpdatedAt    time.Time `gorm:"column:updated_at;precision:3;not null"`
}

func (OutboxEvent) TableName() string { return "outbox_events" }

// PostCreatedEvent 是发帖后通过 Outbox + RabbitMQ 广播的领域事件载荷。
// 不在消息体中固化 is_bigv，由消费者按实时状态判定走推模式还是拉模式。
type PostCreatedEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	PostID    uint64 `json:"post_id"`
	AuthorID  uint64 `json:"author_id"`
	CreatedAt int64  `json:"created_at"` // Unix 毫秒时间戳
}
