package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"wallpaper-feed/internal/config"
)

// UploadScene 表示客户端直传业务场景。
type UploadScene string

const (
	SceneWallpaper UploadScene = "wallpaper"
	SceneAvatar    UploadScene = "avatar"

	WallpaperMaxBytes int64 = 20 << 20 // 20 MiB
	AvatarMaxBytes    int64 = 5 << 20  // 5 MiB
	MaxObjectKeyLen         = 255

	uploadTokenTTL    = 10 * time.Minute
	imageInfoTimeout  = 5 * time.Second
	maxImageInfoBytes = 8192
	qiniuThumbQuery   = "imageView2/2/w/800/format/webp/q/80"
)

var (
	// ErrStorageDisabled 表示对象存储处于 noop 降级模式，不支持签发凭证或远程核验。
	ErrStorageDisabled = errors.New("storage: disabled in noop mode")
	// ErrObjectNotFound 表示对象在远端存储中不存在（?imageInfo 返回 404）。
	ErrObjectNotFound = errors.New("storage: object not found")
	// ErrInvalidImage 表示远端对象不是合法图片、响应体超限或格式不在白名单内。
	ErrInvalidImage = errors.New("storage: invalid image object")
)

var (
	objectKeySuffixPattern = regexp.MustCompile(`^\d{8}/[0-9a-f]{32}\.(jpg|png|webp)$`)

	contentTypeToExt = map[string]string{
		"image/jpeg": "jpg",
		"image/png":  "png",
		"image/webp": "webp",
	}

	extToContentType = map[string]string{
		".jpg":  "image/jpeg",
		".png":  "image/png",
		".webp": "image/webp",
	}

	imageInfoFormatToMime = map[string]string{
		"jpeg": "image/jpeg",
		"png":  "image/png",
		"webp": "image/webp",
	}
)

// UploadCredential 为客户端直传凭证响应载荷。
type UploadCredential struct {
	UploadToken  string
	UploadURL    string
	ObjectKey    string
	ExpiresIn    int64
	MaxSizeBytes int64
}

// ImageMeta 为远端 ?imageInfo 探测得到的图片元数据。
type ImageMeta struct {
	SizeBytes int64
	MimeType  string
	Width     uint32
	Height    uint32
}

// StorageService 抽象对象存储的直传凭证签发、图片元信息核验与 CDN URL 组装能力。
type StorageService interface {
	GenerateUploadToken(ctx context.Context, userID uint64, scene UploadScene, contentType string) (*UploadCredential, error)
	InspectImage(ctx context.Context, objectKey string) (*ImageMeta, error)
	BuildThumbURL(objectKey string) string
	BuildRawURL(objectKey string) string
}

// New 根据配置构造对象存储实现（noop 或 qiniu）。
func New(cfg config.StorageConfig) (StorageService, error) {
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	if provider == "" || provider == config.StorageProviderNoop {
		return NewNoopStorage(cfg.CDNDomain), nil
	}
	if provider == config.StorageProviderQiniu {
		return NewQiniuStorage(cfg)
	}
	return nil, fmt.Errorf("storage: unsupported provider %q", cfg.Provider)
}

// MaxBytesForScene 返回指定上传场景允许的最大文件字节数。
func MaxBytesForScene(scene UploadScene) (int64, bool) {
	switch scene {
	case SceneWallpaper:
		return WallpaperMaxBytes, true
	case SceneAvatar:
		return AvatarMaxBytes, true
	default:
		return 0, false
	}
}

// ExtForContentType 根据白名单 Content-Type 返回服务端受控扩展名。
func ExtForContentType(contentType string) (string, bool) {
	ext, ok := contentTypeToExt[strings.ToLower(strings.TrimSpace(contentType))]
	return ext, ok
}

// MimeFromObjectKey 根据受控对象键扩展名推导对应的白名单 MIME 类型。
func MimeFromObjectKey(objectKey string) (string, bool) {
	mime, ok := extToContentType[strings.ToLower(path.Ext(strings.TrimSpace(objectKey)))]
	return mime, ok
}

// ValidateObjectKey 校验对象键长度、路径安全性以及与场景和用户的严格归属关系。
func ValidateObjectKey(scene UploadScene, userID uint64, objectKey string) bool {
	if userID == 0 || objectKey == "" || len(objectKey) > MaxObjectKeyLen || strings.Contains(objectKey, "..") {
		return false
	}
	prefix, ok := sceneKeyPrefix(scene, userID)
	if !ok || !strings.HasPrefix(objectKey, prefix) {
		return false
	}
	return objectKeySuffixPattern.MatchString(strings.TrimPrefix(objectKey, prefix))
}

func sceneKeyPrefix(scene UploadScene, userID uint64) (string, bool) {
	switch scene {
	case SceneWallpaper:
		return "wallpapers/" + strconv.FormatUint(userID, 10) + "/", true
	case SceneAvatar:
		return "avatars/" + strconv.FormatUint(userID, 10) + "/", true
	default:
		return "", false
	}
}

func generateObjectKey(scene UploadScene, userID uint64, ext string, now time.Time) (string, error) {
	prefix, ok := sceneKeyPrefix(scene, userID)
	if !ok || userID == 0 {
		return "", fmt.Errorf("storage: invalid scene %q or user id %d", scene, userID)
	}
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return fmt.Sprintf("%s%s/%s.%s", prefix, now.UTC().Format("20060102"), hex.EncodeToString(raw[:]), ext), nil
}

func buildCDNURL(cdnDomain, objectKey, query string) string {
	domain := strings.TrimRight(strings.TrimSpace(cdnDomain), "/")
	key := strings.TrimLeft(strings.TrimSpace(objectKey), "/")
	if domain == "" || key == "" {
		return ""
	}
	if query != "" {
		return domain + "/" + key + "?" + query
	}
	return domain + "/" + key
}
