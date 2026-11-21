// Package storage 抽象对象存储：签发上传凭证、校验对象、拼接 CDN URL。
package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var ErrObjectNotFound = errors.New("object not found")

const (
	// KeyPrefix 是帖子图片对象 Key 的强制前缀，由上传策略 saveKey 生成。
	KeyPrefix = "posts/"
	// AvatarKeyPrefix 是用户头像对象 Key 的前缀。
	AvatarKeyPrefix = "avatars/"
	// AvatarMaxSizeBytes 是头像允许的最大字节数（5 MiB）。
	AvatarMaxSizeBytes int64 = 5 << 20
	// MaxObjectKeyLen 是对象 Key 最大长度。
	MaxObjectKeyLen = 255
)

type UploadScene string

const (
	UploadScenePost   UploadScene = "post"
	UploadSceneAvatar UploadScene = "avatar"
)

var avatarKeyTailPattern = regexp.MustCompile(`^\d{4}/(0[1-9]|1[0-2])/[A-Za-z0-9_-]{1,64}(\.[A-Za-z0-9]{1,10})?$`)

// ValidateAvatarKey 校验非空头像对象 Key 是否属于指定用户前缀且符合上传策略生成格式。
func ValidateAvatarKey(userID uint64, key string) bool {
	if userID == 0 || key == "" || len(key) > MaxObjectKeyLen || strings.Contains(key, "..") {
		return false
	}
	prefix := fmt.Sprintf("%s%d/", AvatarKeyPrefix, userID)
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	tail := strings.TrimPrefix(key, prefix)
	return avatarKeyTailPattern.MatchString(tail)
}

type UploadToken struct {
	Token     string `json:"token"`
	UploadURL string `json:"upload_url"`
	ExpiresIn int    `json:"expires_in"`
}

type ObjectInfo struct {
	Size     int64
	MimeType string
}

type Service interface {
	UploadToken(ctx context.Context, scene UploadScene, userID uint64) (*UploadToken, error)
	Stat(ctx context.Context, key string) (*ObjectInfo, error)
	URL(key string) string
	ThumbURL(key string) string
}
