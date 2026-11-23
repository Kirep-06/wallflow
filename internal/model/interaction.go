package model

import (
	"fmt"
	"time"
)

// InteractionKind 区分点赞与收藏两类同构关系，用于参数化复用仓储与服务逻辑。
type InteractionKind uint8

const (
	// 一级评论状态
	CommentStatusActive  uint8 = 1
	CommentStatusDeleted uint8 = 2

	// 点赞/收藏同构关系枚举
	InteractionKindLike     InteractionKind = 1
	InteractionKindFavorite InteractionKind = 2

	// 互动 MQ 事件动作名（供 M6 聚合与 M7 热榜共用）
	InteractionActionLike     = "like"
	InteractionActionFavorite = "favorite"
	InteractionActionDownload = "download"
	InteractionActionComment  = "comment"
)

// WallpaperStats 对应 MySQL wallpaper_stats 冷热分离计数表实体。
type WallpaperStats struct {
	WallpaperID   uint64    `gorm:"primaryKey"`
	LikeCount     uint64    `gorm:"not null;default:0"`
	FavCount      uint64    `gorm:"not null;default:0"`
	DownloadCount uint64    `gorm:"not null;default:0"`
	CommentCount  uint64    `gorm:"not null;default:0"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime"`
}

func (WallpaperStats) TableName() string {
	return "wallpaper_stats"
}

// ToStatsCache 将统计实体转换为三级缓存快照。
func (s WallpaperStats) ToStatsCache() WallpaperStatsCache {
	return WallpaperStatsCache{
		LikeCount:     s.LikeCount,
		FavCount:      s.FavCount,
		DownloadCount: s.DownloadCount,
		CommentCount:  s.CommentCount,
	}
}

// WallpaperLike 对应 MySQL wallpaper_likes 点赞明细表实体。
type WallpaperLike struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	UserID      uint64    `gorm:"not null;uniqueIndex:uk_user_wp"`
	WallpaperID uint64    `gorm:"not null;uniqueIndex:uk_user_wp"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (WallpaperLike) TableName() string {
	return "wallpaper_likes"
}

// WallpaperFavorite 对应 MySQL wallpaper_favorites 收藏明细表实体。
type WallpaperFavorite struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	UserID      uint64    `gorm:"not null;uniqueIndex:uk_user_wp"`
	WallpaperID uint64    `gorm:"not null;uniqueIndex:uk_user_wp"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (WallpaperFavorite) TableName() string {
	return "wallpaper_favorites"
}

// Comment 对应 MySQL comments 一级评论表实体。
type Comment struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	WallpaperID uint64    `gorm:"not null"`
	UserID      uint64    `gorm:"not null"`
	Content     string    `gorm:"type:varchar(500);not null"`
	Status      uint8     `gorm:"not null;default:1"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (Comment) TableName() string {
	return "comments"
}

// InteractionEdge 为点赞/收藏列表覆盖索引游标扫描的轻量投影。
type InteractionEdge struct {
	ID          uint64
	WallpaperID uint64
}

// CommentDeleteRecord 为评论软删鉴权的轻量投影。
type CommentDeleteRecord struct {
	ID          uint64
	WallpaperID uint64
	UserID      uint64
	Status      uint8
}

// StatsReconcileRow 为低频对账读取 wallpaper_stats 现有值与乐观锁时间戳的投影。
type StatsReconcileRow struct {
	WallpaperID  uint64
	LikeCount    uint64
	FavCount     uint64
	CommentCount uint64
	UpdatedAt    time.Time
}

// StatsDelta 表示聚合窗口内单张壁纸四项计数的带符号净增量。
type StatsDelta struct {
	WallpaperID uint64
	Like        int64
	Fav         int64
	Download    int64
	Comment     int64
}

// Add 校验动作与增量合法性并累加到对应维度；非法事件返回 false。
func (d *StatsDelta) Add(action string, delta int64) bool {
	switch action {
	case InteractionActionLike:
		if delta != 1 && delta != -1 {
			return false
		}
		d.Like += delta
	case InteractionActionFavorite:
		if delta != 1 && delta != -1 {
			return false
		}
		d.Fav += delta
	case InteractionActionDownload:
		if delta != 1 {
			return false
		}
		d.Download += delta
	case InteractionActionComment:
		if delta != 1 && delta != -1 {
			return false
		}
		d.Comment += delta
	default:
		return false
	}
	return true
}

// IsZero 返回四项净增量是否已完全抵消为 0。
func (d StatsDelta) IsZero() bool {
	return d.Like == 0 && d.Fav == 0 && d.Download == 0 && d.Comment == 0
}

// WallpaperStatsCache 为 wallpaper:stats:{id} 三级缓存快照结构。
type WallpaperStatsCache struct {
	LikeCount     uint64 `json:"like_count"`
	FavCount      uint64 `json:"fav_count"`
	DownloadCount uint64 `json:"download_count"`
	CommentCount  uint64 `json:"comment_count"`
}

// InteractionEvent 为 wallpaper.interaction 削峰通道的事件 JSON 载荷（user_id 同步供 M7 热榜防刷复用）。
type InteractionEvent struct {
	EventID     string    `json:"event_id"`
	WallpaperID uint64    `json:"wallpaper_id"`
	UserID      uint64    `json:"user_id"`
	Action      string    `json:"action"`
	Delta       int64     `json:"delta"`
	OccurredAt  time.Time `json:"occurred_at"`
}

// NewInteractionEvent 构造标准化的互动增量事件。
func NewInteractionEvent(wallpaperID, userID uint64, action string, delta int64, now time.Time) InteractionEvent {
	ts := now.UTC()
	return InteractionEvent{
		EventID:     fmt.Sprintf("act:%s:%d:%d:%d", action, userID, wallpaperID, ts.UnixNano()),
		WallpaperID: wallpaperID,
		UserID:      userID,
		Action:      action,
		Delta:       delta,
		OccurredAt:  ts,
	}
}

// LikeActionResult 为点赞/取消点赞接口响应结构。
type LikeActionResult struct {
	IsLiked bool `json:"is_liked"`
}

// FavoriteActionResult 为收藏/取消收藏接口响应结构。
type FavoriteActionResult struct {
	IsFavorited bool `json:"is_favorited"`
}

// CreateCommentRequest 为发表一级评论请求体。
type CreateCommentRequest struct {
	Content string `json:"content"`
}

// CommentItem 为一级评论响应项。
type CommentItem struct {
	ID          uint64          `json:"id,string"`
	WallpaperID uint64          `json:"wallpaper_id,string"`
	Content     string          `json:"content"`
	Author      WallpaperAuthor `json:"author"`
	CreatedAt   time.Time       `json:"created_at"`
}
