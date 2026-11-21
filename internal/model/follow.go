package model

import "time"

type UserFollow struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement"`
	FollowerID uint64    `gorm:"column:follower_id;not null;uniqueIndex:uk_follower_followee,priority:1;index:idx_follower_list,priority:1;index:idx_followee_list,priority:3"`
	FolloweeID uint64    `gorm:"column:followee_id;not null;uniqueIndex:uk_follower_followee,priority:2;index:idx_follower_list,priority:3;index:idx_followee_list,priority:1"`
	CreatedAt  time.Time `gorm:"column:created_at;precision:3;not null"`
}

func (UserFollow) TableName() string { return "user_follows" }

type UserFollowStats struct {
	UserID         uint64    `gorm:"primaryKey;column:user_id"`
	FollowingCount uint32    `gorm:"column:following_count;not null;default:0"`
	FollowerCount  uint32    `gorm:"column:follower_count;not null;default:0"`
	UpdatedAt      time.Time `gorm:"column:updated_at;precision:3;not null"`
}

func (UserFollowStats) TableName() string { return "user_follow_stats" }

// FollowStats 表示用户的关注数与粉丝数，同时作为 follow:stats:{uid} 缓存载荷。
type FollowStats struct {
	FollowingCount int64 `json:"following_count"`
	FollowerCount  int64 `json:"follower_count"`
}

// FollowEdge 表示覆盖索引扫描得到的一条关注关系边（ID 为 user_follows 主键，用作游标；UserID 为对端用户 ID）。
type FollowEdge struct {
	ID     uint64 `gorm:"column:id"`
	UserID uint64 `gorm:"column:user_id"`
}

// FollowUserDTO 是关注/粉丝列表中的单个用户条目。
type FollowUserDTO struct {
	ID          uint64 `json:"id,string"`
	Username    string `json:"username"`
	Nickname    string `json:"nickname"`
	AvatarURL   string `json:"avatar_url"`
	Bio         string `json:"bio"`
	IsFollowing bool   `json:"is_following"`
}

// FollowUserPage 是关注/粉丝列表的游标分页响应。
type FollowUserPage struct {
	Items      []*FollowUserDTO `json:"items"`
	NextCursor string           `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
}
