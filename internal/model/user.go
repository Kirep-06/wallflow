package model

import (
	"bytes"
	"encoding/json"
	"time"
)

const (
	UserStatusActive int8 = 1
	UserStatusBanned int8 = 2
)

// User 对应 MySQL users 表实体。
type User struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	Username     string    `gorm:"type:varchar(32);not null;uniqueIndex:uk_username"`
	PasswordHash string    `gorm:"type:varchar(255);not null"`
	Nickname     string    `gorm:"type:varchar(32);not null"`
	AvatarKey    string    `gorm:"type:varchar(255);not null;default:''"`
	Bio          string    `gorm:"type:varchar(200);not null;default:''"`
	Status       int8      `gorm:"type:tinyint;not null;default:1"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
}

// TableName 指定 GORM 表名。
func (User) TableName() string {
	return "users"
}

// UserAuthRecord 为登录鉴权查询的专用记录结构。
type UserAuthRecord struct {
	ID           uint64
	Username     string
	PasswordHash string
	Nickname     string
	AvatarKey    string
	Bio          string
	Status       int8
	CreatedAt    time.Time
}

// UserProfileCache 为三级缓存用户资料实体，物理隔离 password_hash 与 status 字段。
type UserProfileCache struct {
	ID        uint64    `json:"id,string"`
	Username  string    `json:"username"`
	Nickname  string    `json:"nickname"`
	AvatarKey string    `json:"avatar_key"`
	Bio       string    `json:"bio"`
	CreatedAt time.Time `json:"created_at"`
}

// ToProfileCache 将数据库实体投影为安全缓存结构。
func (u *User) ToProfileCache() UserProfileCache {
	return UserProfileCache{
		ID:        u.ID,
		Username:  u.Username,
		Nickname:  u.Nickname,
		AvatarKey: u.AvatarKey,
		Bio:       u.Bio,
		CreatedAt: u.CreatedAt.UTC(),
	}
}

// ToProfileCache 将认证记录投影为安全缓存结构。
func (r *UserAuthRecord) ToProfileCache() UserProfileCache {
	return UserProfileCache{
		ID:        r.ID,
		Username:  r.Username,
		Nickname:  r.Nickname,
		AvatarKey: r.AvatarKey,
		Bio:       r.Bio,
		CreatedAt: r.CreatedAt.UTC(),
	}
}

// PatchString 用于 PATCH 请求中精确区分“未传字段”、“显式传 null”与“正常字符串值”。
type PatchString struct {
	Set   bool
	Null  bool
	Value string
}

// UnmarshalJSON 解析 JSON 字符串字段，显式标记 null。
func (p *PatchString) UnmarshalJSON(data []byte) error {
	p.Set = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		p.Null = true
		return nil
	}
	return json.Unmarshal(data, &p.Value)
}

// RegisterRequest 为账号注册请求体。
type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
}

// LoginRequest 为用户名密码登录请求体。
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// RefreshRequest 为刷新令牌请求体。
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// PatchProfileRequest 为个人资料部分更新请求体。
type PatchProfileRequest struct {
	Nickname  PatchString `json:"nickname"`
	Bio       PatchString `json:"bio"`
	AvatarKey PatchString `json:"avatar_key"`
}

// TokenPair 为双 Token 响应结构。
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
}

// UserProfile 为个人资料响应结构（id 一律序列化为 string）。
type UserProfile struct {
	ID             string    `json:"id"`
	Username       string    `json:"username"`
	Nickname       string    `json:"nickname"`
	AvatarURL      string    `json:"avatar_url"`
	Bio            string    `json:"bio"`
	FollowingCount int64     `json:"following_count"`
	FollowerCount  int64     `json:"follower_count"`
	WallpaperCount int64     `json:"wallpaper_count"`
	CreatedAt      time.Time `json:"created_at"`
}

// UserPublicProfile 为创作者公开主页响应结构。
type UserPublicProfile struct {
	UserProfile
	IsFollowing bool `json:"is_following"`
}

// AuthResult 为注册/登录成功响应结构。
type AuthResult struct {
	TokenPair
	User UserProfile `json:"user"`
}
