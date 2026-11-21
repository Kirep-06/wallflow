package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/go-sdk/v7/auth"
	"github.com/qiniu/go-sdk/v7/client"
	qnstorage "github.com/qiniu/go-sdk/v7/storage"

	"wallpaper-feed/internal/config"
)

// AllowedMimes 是允许上传的图片类型。
var AllowedMimes = []string{"image/jpeg", "image/png", "image/webp"}

const thumbSuffix = "?imageView2/2/w/400/format/webp"

// returnBody 由七牛在上传成功后原样返回给客户端，客户端再提交到创建帖子接口。
const returnBody = `{"key":"$(key)","size":$(fsize),"mime":"$(mimeType)","width":$(imageInfo.width),"height":$(imageInfo.height)}`

type Qiniu struct {
	cfg    config.StorageConfig
	cred   *auth.Credentials
	bucket *qnstorage.BucketManager
}

func NewQiniu(cfg config.StorageConfig) *Qiniu {
	cred := auth.New(cfg.AccessKey, cfg.SecretKey)
	return &Qiniu{
		cfg:    cfg,
		cred:   cred,
		bucket: qnstorage.NewBucketManager(cred, &qnstorage.Config{UseHTTPS: true}),
	}
}

func (q *Qiniu) UploadToken(_ context.Context) (*UploadToken, error) {
	policy := qnstorage.PutPolicy{
		Scope:        q.cfg.Bucket,
		SaveKey:      KeyPrefix + "$(year)/$(mon)/$(etag)$(ext)",
		ForceSaveKey: true,
		FsizeLimit:   int64(q.cfg.MaxFileSizeMB) << 20,
		MimeLimit:    strings.Join(AllowedMimes, ";"),
		DetectMime:   1,
		ReturnBody:   returnBody,
		Expires:      uint64(q.cfg.TokenExpireSeconds),
	}
	return &UploadToken{
		Token:     policy.UploadToken(q.cred),
		UploadURL: q.cfg.UploadURL,
		ExpiresIn: q.cfg.TokenExpireSeconds,
	}, nil
}

func (q *Qiniu) Stat(_ context.Context, key string) (*ObjectInfo, error) {
	info, err := q.bucket.Stat(q.cfg.Bucket, key)
	if err != nil {
		var ei *client.ErrorInfo
		if (errors.As(err, &ei) && ei.Code == 612) || strings.Contains(err.Error(), "no such file") {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("qiniu stat %s: %w", key, err)
	}
	return &ObjectInfo{Size: info.Fsize, MimeType: info.MimeType}, nil
}

func (q *Qiniu) URL(key string) string {
	return strings.TrimRight(q.cfg.CDNDomain, "/") + "/" + key
}

func (q *Qiniu) ThumbURL(key string) string { return q.URL(key) + thumbSuffix }
