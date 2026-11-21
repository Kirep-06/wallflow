// Package storage 抽象对象存储：签发上传凭证、校验对象、拼接 CDN URL。
package storage

import (
	"context"
	"errors"
)

var ErrObjectNotFound = errors.New("object not found")

// KeyPrefix 是帖子图片对象 Key 的强制前缀，由上传策略 saveKey 生成。
const KeyPrefix = "posts/"

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
	UploadToken(ctx context.Context) (*UploadToken, error)
	Stat(ctx context.Context, key string) (*ObjectInfo, error)
	URL(key string) string
	ThumbURL(key string) string
}
