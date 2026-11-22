package service

import (
	"context"
	"errors"
	"strings"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/storage"
)

const (
	wallpaperMinLongEdge  uint32 = 1280
	wallpaperMinShortEdge uint32 = 480
	wallpaperMaxAspect           = 4.0
	avatarMinEdge         uint32 = 100
	avatarMaxEdge         uint32 = 4096
)

// UploadService 负责七牛客户端直传凭证签发。
type UploadService struct {
	store storage.StorageService
}

// NewUploadService 构造上传领域服务。
func NewUploadService(store storage.StorageService) *UploadService {
	return &UploadService{store: store}
}

// CreateUploadToken 校验上传参数并签发锁定唯一 object_key 的短时直传凭证。
func (s *UploadService) CreateUploadToken(ctx context.Context, userID uint64, req model.UploadTokenRequest) (*model.UploadTokenResult, error) {
	scene := storage.UploadScene(strings.TrimSpace(req.Scene))
	maxBytes, ok := storage.MaxBytesForScene(scene)
	if !ok {
		return nil, errno.ErrInvalidParam.WithMessage("上传场景仅支持 wallpaper 或 avatar")
	}

	contentType := strings.ToLower(strings.TrimSpace(req.ContentType))
	if _, ok := storage.ExtForContentType(contentType); !ok {
		return nil, errno.ErrInvalidParam.WithMessage("仅支持 image/jpeg、image/png 或 image/webp 格式")
	}

	if req.FileSize <= 0 || req.FileSize > maxBytes {
		return nil, errno.ErrInvalidParam.WithMessage("文件大小超出允许范围")
	}

	cred, err := s.store.GenerateUploadToken(ctx, userID, scene, contentType)
	if err != nil {
		return nil, errno.ErrInternal.Wrap(err)
	}

	return &model.UploadTokenResult{
		UploadToken:  cred.UploadToken,
		UploadURL:    cred.UploadURL,
		ObjectKey:    cred.ObjectKey,
		ExpiresIn:    cred.ExpiresIn,
		MaxSizeBytes: cred.MaxSizeBytes,
	}, nil
}

func verifyUploadedImage(
	ctx context.Context,
	store storage.StorageService,
	scene storage.UploadScene,
	userID uint64,
	objectKey string,
) (*storage.ImageMeta, error) {
	if !storage.ValidateObjectKey(scene, userID, objectKey) {
		return nil, errno.ErrInvalidParam.WithMessage("object_key 不合法或无权使用")
	}

	meta, err := store.InspectImage(ctx, objectKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, storage.ErrInvalidImage) {
			return nil, errno.ErrUploadVerifyFailed
		}
		return nil, errno.ErrInternal.Wrap(err)
	}

	maxBytes, _ := storage.MaxBytesForScene(scene)
	if meta == nil || meta.SizeBytes < 1 || meta.SizeBytes > maxBytes {
		return nil, errno.ErrUploadVerifyFailed.WithMessage("上传文件大小超出限制")
	}
	return meta, nil
}

func verifyWallpaperObject(ctx context.Context, store storage.StorageService, userID uint64, objectKey string) (*storage.ImageMeta, error) {
	meta, err := verifyUploadedImage(ctx, store, storage.SceneWallpaper, userID, objectKey)
	if err != nil {
		return nil, err
	}

	longEdge := max(meta.Width, meta.Height)
	shortEdge := min(meta.Width, meta.Height)
	if longEdge < wallpaperMinLongEdge || shortEdge < wallpaperMinShortEdge || float64(longEdge)/float64(shortEdge) > wallpaperMaxAspect {
		return nil, errno.ErrUploadVerifyFailed.WithMessage("壁纸分辨率或宽高比不符合要求")
	}
	return meta, nil
}

func verifyAvatarObject(ctx context.Context, store storage.StorageService, userID uint64, objectKey string) error {
	meta, err := verifyUploadedImage(ctx, store, storage.SceneAvatar, userID, objectKey)
	if err != nil {
		return err
	}

	if meta.Width < avatarMinEdge || meta.Width > avatarMaxEdge || meta.Height < avatarMinEdge || meta.Height > avatarMaxEdge {
		return errno.ErrUploadVerifyFailed.WithMessage("头像分辨率需在 100x100 至 4096x4096 之间")
	}
	return nil
}
