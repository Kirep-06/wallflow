package storage

import (
	"context"
	"strings"
)

var _ StorageService = (*NoopStorage)(nil)

// NoopStorage 为本地无七牛凭据时的降级存储实现；凭证签发与图片核验返回 ErrStorageDisabled。
type NoopStorage struct {
	cdnDomain string
}

// NewNoopStorage 构造降级对象存储实现。
func NewNoopStorage(cdnDomain string) *NoopStorage {
	return &NoopStorage{
		cdnDomain: strings.TrimRight(strings.TrimSpace(cdnDomain), "/"),
	}
}

func (s *NoopStorage) GenerateUploadToken(_ context.Context, _ uint64, _ UploadScene, _ string) (*UploadCredential, error) {
	return nil, ErrStorageDisabled
}

func (s *NoopStorage) InspectImage(_ context.Context, _ string) (*ImageMeta, error) {
	return nil, ErrStorageDisabled
}

// BuildThumbURL 在降级模式下退化为原图地址。
func (s *NoopStorage) BuildThumbURL(objectKey string) string {
	return buildCDNURL(s.cdnDomain, objectKey, "")
}

// BuildRawURL 组装原图 CDN 地址。
func (s *NoopStorage) BuildRawURL(objectKey string) string {
	return buildCDNURL(s.cdnDomain, objectKey, "")
}
