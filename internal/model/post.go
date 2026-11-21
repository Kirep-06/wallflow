// Package model 定义 GORM 实体与对外 DTO。
package model

import "time"

const (
	PostStatusNormal  int8 = 1
	PostStatusDeleted int8 = 2
)

type Post struct {
	ID         uint64      `gorm:"primaryKey;autoIncrement"`
	UserID     uint64      `gorm:"not null"`
	Content    string      `gorm:"size:2000;not null"`
	ImageCount uint8       `gorm:"not null"`
	Status     int8        `gorm:"not null;default:1"`
	CreatedAt  time.Time   `gorm:"precision:3"`
	UpdatedAt  time.Time   `gorm:"precision:3"`
	Images     []PostImage `gorm:"foreignKey:PostID"`
}

func (Post) TableName() string { return "posts" }

type PostImage struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	PostID    uint64    `gorm:"not null"`
	SortNo    uint8     `gorm:"not null"`
	ObjectKey string    `gorm:"size:255;not null"`
	Width     uint32    `gorm:"not null"`
	Height    uint32    `gorm:"not null"`
	SizeBytes uint32    `gorm:"not null"`
	Mime      string    `gorm:"size:32;not null"`
	CreatedAt time.Time `gorm:"precision:3"`
}

func (PostImage) TableName() string { return "post_images" }

// PostCache 是写入 L1/L2 的帖子详情快照：只存 object_key，URL 在输出时拼接。
type PostCache struct {
	ID        uint64       `json:"id"`
	UserID    uint64       `json:"user_id"`
	Content   string       `json:"content"`
	Status    int8         `json:"status"`
	Images    []ImageCache `json:"images"`
	CreatedAt int64        `json:"created_at"` // 毫秒
}

type ImageCache struct {
	Key    string `json:"key"`
	Width  uint32 `json:"w"`
	Height uint32 `json:"h"`
}

func NewPostCache(p *Post) *PostCache {
	pc := &PostCache{
		ID:        p.ID,
		UserID:    p.UserID,
		Content:   p.Content,
		Status:    p.Status,
		CreatedAt: p.CreatedAt.UnixMilli(),
		Images:    make([]ImageCache, 0, len(p.Images)),
	}
	for _, img := range p.Images {
		pc.Images = append(pc.Images, ImageCache{Key: img.ObjectKey, Width: img.Width, Height: img.Height})
	}
	return pc
}

// PostDTO 是接口输出的帖子；ID 序列化为字符串避免 JS 精度丢失。
type PostDTO struct {
	ID        uint64     `json:"id,string"`
	UserID    uint64     `json:"user_id,string"`
	Content   string     `json:"content"`
	Images    []ImageDTO `json:"images"`
	CreatedAt int64      `json:"created_at"`
}

type ImageDTO struct {
	URL      string `json:"url"`
	ThumbURL string `json:"thumb_url"`
	Width    uint32 `json:"width"`
	Height   uint32 `json:"height"`
}

// FeedPage 是游标分页的统一响应体。
type FeedPage struct {
	Items      []*PostDTO `json:"items"`
	NextCursor string     `json:"next_cursor"`
	HasMore    bool       `json:"has_more"`
}
