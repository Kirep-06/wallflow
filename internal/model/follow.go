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
	IsBigV         int8      `gorm:"column:is_bigv;not null;default:0;index:idx_bigv,priority:1"`
	UpdatedAt      time.Time `gorm:"column:updated_at;precision:3;not null"`
}

func (UserFollowStats) TableName() string { return "user_follow_stats" }

// FollowStats 表示用户的关注数、粉丝数与大 V 标识，同时作为 follow:stats:{uid} 缓存载荷。
type FollowStats struct {
	FollowingCount int64 `json:"following_count"`
	FollowerCount  int64 `json:"follower_count"`
	IsBigV         bool  `json:"is_bigv,omitempty"`
}

// FollowMutationResult 表示关注/取关事务提交后的变更与大 V 状态。
type FollowMutationResult struct {
	Changed        bool // 本次关系是否实际变更（RowsAffected == 1）
	FolloweeIsBigV bool // 事务完成后被关注方是否是大 V
	PromotedBigV   bool // 本次关注是否使被关注方首次晋升为大 V（只升不自动降）
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
