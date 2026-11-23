package model

import "time"

// UserFollow 对应 MySQL user_follows 表实体。
type UserFollow struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement"`
	FollowerID uint64    `gorm:"not null;uniqueIndex:uk_follower_followee,priority:1;index:idx_follower_list,priority:1"`
	FolloweeID uint64    `gorm:"not null;uniqueIndex:uk_follower_followee,priority:2;index:idx_followee_list,priority:1"`
	CreatedAt  time.Time `gorm:"autoCreateTime"`
}

// TableName 指定 GORM 表名。
func (UserFollow) TableName() string {
	return "user_follows"
}

// UserFollowStats 对应 MySQL user_follow_stats 表实体。
type UserFollowStats struct {
	UserID         uint64    `gorm:"primaryKey"`
	FollowingCount uint64    `gorm:"not null;default:0"`
	FollowerCount  uint64    `gorm:"not null;default:0"`
	IsBigV         uint8     `gorm:"column:is_bigv;not null;default:0"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime"`
}

// TableName 指定 GORM 表名。
func (UserFollowStats) TableName() string {
	return "user_follow_stats"
}

// ToStatsCache 将数据库统计行转换为缓存结构。
func (s UserFollowStats) ToStatsCache() FollowStatsCache {
	return FollowStatsCache{
		FollowingCount: int64(s.FollowingCount),
		FollowerCount:  int64(s.FollowerCount),
		IsBigV:         s.IsBigV == 1,
	}
}

// FollowEdge 为覆盖索引扫描出的轻量关系投影（id 作分页游标，peer_id 为对端用户 ID）。
type FollowEdge struct {
	ID     uint64
	PeerID uint64
}

// FollowStatsCache 为 follow:stats:{uid} 三级缓存结构。
type FollowStatsCache struct {
	FollowingCount int64 `json:"following_count"`
	FollowerCount  int64 `json:"follower_count"`
	IsBigV         bool  `json:"is_bigv"`
}

// FollowActionResult 为关注 / 取消关注接口的响应结构。
type FollowActionResult struct {
	IsFollowing   bool  `json:"is_following"`
	FollowerCount int64 `json:"follower_count"`
}

// FollowUserItem 为关注 / 粉丝列表中的用户卡片摘要结构。
type FollowUserItem struct {
	ID            uint64 `json:"id,string"`
	Username      string `json:"username"`
	Nickname      string `json:"nickname"`
	AvatarURL     string `json:"avatar_url"`
	Bio           string `json:"bio"`
	FollowerCount int64  `json:"follower_count"`
	IsFollowing   bool   `json:"is_following"`
}
