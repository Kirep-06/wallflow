package model

import "time"

const (
	UserStatusNormal int8 = 1
	UserStatusBanned int8 = 2
)

type User struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	Username     string    `gorm:"size:32;not null;uniqueIndex:uk_username"`
	PasswordHash string    `gorm:"size:255;not null"`
	Nickname     string    `gorm:"size:32;not null"`
	AvatarKey    string    `gorm:"size:255;not null;default:''"`
	Bio          string    `gorm:"size:200;not null;default:''"`
	Status       int8      `gorm:"not null;default:1"`
	CreatedAt    time.Time `gorm:"precision:3"`
	UpdatedAt    time.Time `gorm:"precision:3"`
}

func (User) TableName() string { return "users" }

// UserAuthRecord 是认证流程专用记录（包含 password_hash，不进入公开资料缓存）。
type UserAuthRecord struct {
	ID           uint64 `gorm:"column:id"`
	Username     string `gorm:"column:username"`
	PasswordHash string `gorm:"column:password_hash"`
	Nickname     string `gorm:"column:nickname"`
	AvatarKey    string `gorm:"column:avatar_key"`
	Status       int8   `gorm:"column:status"`
}

// UserStatusRecord 用于 Refresh 时从 MySQL 校验用户存在性与封禁状态（不含 password_hash）。
type UserStatusRecord struct {
	ID        uint64 `gorm:"column:id"`
	Username  string `gorm:"column:username"`
	Nickname  string `gorm:"column:nickname"`
	AvatarKey string `gorm:"column:avatar_key"`
	Status    int8   `gorm:"column:status"`
}

// UserPublic 是不含 password_hash 的用户公开信息（用于指定列查询）。
type UserPublic struct {
	ID        uint64    `gorm:"column:id"`
	Username  string    `gorm:"column:username"`
	Nickname  string    `gorm:"column:nickname"`
	AvatarKey string    `gorm:"column:avatar_key"`
	Bio       string    `gorm:"column:bio"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

// UserProfileCache 是写入 L1/L2 的用户公开资料快照：不含 password_hash 与 status，只存 avatar_key。
type UserProfileCache struct {
	ID        uint64 `json:"id"`
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	AvatarKey string `json:"avatar_key"`
	Bio       string `json:"bio"`
	CreatedAt int64  `json:"created_at"` // Unix 毫秒
}

func NewUserProfileCache(u *UserPublic) *UserProfileCache {
	return &UserProfileCache{
		ID:        u.ID,
		Username:  u.Username,
		Nickname:  u.Nickname,
		AvatarKey: u.AvatarKey,
		Bio:       u.Bio,
		CreatedAt: u.CreatedAt.UnixMilli(),
	}
}

type UserSummaryDTO struct {
	ID        uint64 `json:"id,string"`
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	AvatarURL string `json:"avatar_url"`
}

type UserProfileDTO struct {
	ID             uint64 `json:"id,string"`
	Username       string `json:"username"`
	Nickname       string `json:"nickname"`
	AvatarURL      string `json:"avatar_url"`
	Bio            string `json:"bio"`
	FollowingCount int64  `json:"following_count"`
	FollowerCount  int64  `json:"follower_count"`
	IsFollowing    bool   `json:"is_following"`
	CreatedAt      int64  `json:"created_at"`
}

type AuthTokenData struct {
	AccessToken  string         `json:"access_token"`
	RefreshToken string         `json:"refresh_token"`
	ExpiresIn    int64          `json:"expires_in"`
	User         UserSummaryDTO `json:"user"`
}
