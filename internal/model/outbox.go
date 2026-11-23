package model

import (
	"strconv"
	"time"
)

// OutboxEvent 状态枚举（显式数值，禁止 iota）。
const (
	OutboxStatusPending   uint8 = 0 // 待投递
	OutboxStatusPublished uint8 = 1 // 已确认落盘投递
	OutboxStatusDead      uint8 = 2 // 超限死信态
)

// OutboxEvent 对应 outbox_events 事务发件箱表（DB 实体刻意不加 json tag）。
type OutboxEvent struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	EventID      string    `gorm:"column:event_id;size:64;not null;uniqueIndex:uk_event_id"`
	ExchangeName string    `gorm:"column:exchange_name;size:64;not null"`
	RoutingKey   string    `gorm:"column:routing_key;size:64;not null"`
	Payload      string    `gorm:"column:payload;type:json;not null"`
	Status       uint8     `gorm:"column:status;not null;default:0"`
	RetryCount   uint32    `gorm:"column:retry_count;not null;default:0"`
	NextRetryAt  time.Time `gorm:"column:next_retry_at;not null"`
	LastError    string    `gorm:"column:last_error;size:500;not null;default:''"`
	CreatedAt    time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt    time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (OutboxEvent) TableName() string {
	return "outbox_events"
}

// WallpaperPublishedEvent 为壁纸发布异步事件（routing_key = wallpaper.published）的轻量 JSON 载荷。
type WallpaperPublishedEvent struct {
	EventID     string    `json:"event_id"`
	WallpaperID uint64    `json:"wallpaper_id"`
	AuthorID    uint64    `json:"author_id"`
	PublishedAt time.Time `json:"published_at"`
}

// NewWallpaperPublishedEvent 按统一幂等命名规范构造壁纸发布事件载荷。
func NewWallpaperPublishedEvent(wallpaperID, authorID uint64, publishedAt time.Time) WallpaperPublishedEvent {
	if publishedAt.IsZero() {
		publishedAt = time.Now().UTC()
	} else {
		publishedAt = publishedAt.UTC()
	}
	return WallpaperPublishedEvent{
		EventID:     "wp_pub:" + strconv.FormatUint(wallpaperID, 10),
		WallpaperID: wallpaperID,
		AuthorID:    authorID,
		PublishedAt: publishedAt,
	}
}
