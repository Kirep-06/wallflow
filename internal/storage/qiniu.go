package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/qiniu/go-sdk/v7/auth"
	qiniuStorage "github.com/qiniu/go-sdk/v7/storage"

	"wallpaper-feed/internal/config"
)

var _ StorageService = (*QiniuStorage)(nil)

// QiniuStorage 实现七牛云客户端直传凭证签发、?imageInfo 图片核验与 CDN 样式 URL 组装。
type QiniuStorage struct {
	mac            *auth.Credentials
	bucket         string
	uploadURL      string
	cdnDomain      string
	httpClient     *http.Client
	inspectTimeout time.Duration
}

// NewQiniuStorage 构造七牛云对象存储实现（仅初始化签发凭据与 HTTP 探测客户端，不初始化 BucketManager）。
func NewQiniuStorage(cfg config.StorageConfig) (*QiniuStorage, error) {
	cdnDomain := strings.TrimRight(strings.TrimSpace(cfg.CDNDomain), "/")
	bucket := strings.TrimSpace(cfg.Qiniu.Bucket)
	ak := strings.TrimSpace(cfg.Qiniu.AccessKey)
	sk := strings.TrimSpace(cfg.Qiniu.SecretKey)

	if ak == "" || sk == "" {
		return nil, fmt.Errorf("storage: qiniu access key and secret key are required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("storage: qiniu bucket is required")
	}
	if cdnDomain == "" {
		return nil, fmt.Errorf("storage: cdn_domain is required")
	}
	uploadURL, err := cfg.Qiniu.ResolveUploadURL()
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}

	return &QiniuStorage{
		mac:            auth.New(ak, sk),
		bucket:         bucket,
		uploadURL:      uploadURL,
		cdnDomain:      cdnDomain,
		httpClient:     &http.Client{},
		inspectTimeout: imageInfoTimeout,
	}, nil
}

// GenerateUploadToken 签发锁定唯一 object_key、InsertOnly、体积与对应单 MIME 的七牛直传凭证。
func (s *QiniuStorage) GenerateUploadToken(_ context.Context, userID uint64, scene UploadScene, contentType string) (*UploadCredential, error) {
	maxBytes, ok := MaxBytesForScene(scene)
	if !ok {
		return nil, fmt.Errorf("storage: unsupported upload scene %q", scene)
	}
	normalizedType := strings.ToLower(strings.TrimSpace(contentType))
	ext, ok := ExtForContentType(normalizedType)
	if !ok {
		return nil, fmt.Errorf("storage: unsupported content type %q", contentType)
	}
	objectKey, err := generateObjectKey(scene, userID, ext, time.Now())
	if err != nil {
		return nil, err
	}

	expiresSec := uint64(uploadTokenTTL / time.Second)
	policy := qiniuStorage.PutPolicy{
		Scope:      s.bucket + ":" + objectKey,
		InsertOnly: 1,
		FsizeMin:   1,
		FsizeLimit: maxBytes,
		MimeLimit:  normalizedType,
		Expires:    expiresSec,
	}

	return &UploadCredential{
		UploadToken:  policy.UploadToken(s.mac),
		UploadURL:    s.uploadURL,
		ObjectKey:    objectKey,
		ExpiresIn:    int64(expiresSec),
		MaxSizeBytes: maxBytes,
	}, nil
}

type qiniuImageInfo struct {
	Size   int64  `json:"size"`
	Format string `json:"format"`
	Width  int64  `json:"width"`
	Height int64  `json:"height"`
}

// InspectImage 通过单次 GET <cdn_domain>/<object_key>?imageInfo 权威核验远端图片元数据。
// 若配置为七牛测试域名（*.clouddn.com，CDN 默认忽略查询参数），改为直接拉取原图头部解析，避免 ?imageInfo JSON 污染图片缓存。
func (s *QiniuStorage) InspectImage(ctx context.Context, objectKey string) (*ImageMeta, error) {
	if isQiniuTestDomain(s.cdnDomain) {
		return s.inspectRawImage(ctx, objectKey)
	}

	targetURL := buildCDNURL(s.cdnDomain, objectKey, "imageInfo")
	if targetURL == "" {
		return nil, ErrInvalidImage
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.inspectTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("storage build imageInfo request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storage request imageInfo: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrObjectNotFound
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError:
		return nil, ErrInvalidImage
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("storage imageInfo unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxImageInfoBytes+1))
	if err != nil {
		return nil, fmt.Errorf("storage read imageInfo body: %w", err)
	}
	if len(body) > maxImageInfoBytes {
		return nil, ErrInvalidImage
	}

	var info qiniuImageInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, ErrInvalidImage
	}
	if info.Size <= 0 || info.Width <= 0 || info.Height <= 0 || info.Width > math.MaxUint32 || info.Height > math.MaxUint32 {
		return nil, ErrInvalidImage
	}
	mimeType, ok := imageInfoFormatToMime[strings.ToLower(strings.TrimSpace(info.Format))]
	if !ok {
		return nil, ErrInvalidImage
	}

	return &ImageMeta{
		SizeBytes: info.Size,
		MimeType:  mimeType,
		Width:     uint32(info.Width),
		Height:    uint32(info.Height),
	}, nil
}

func isQiniuTestDomain(cdnDomain string) bool {
	return strings.Contains(strings.ToLower(cdnDomain), ".clouddn.com")
}

// inspectRawImage 直接请求原图并解析图片文件头（支持 JPEG/PNG/WebP），防止忽略参数的测试域名缓存 ?imageInfo JSON。
func (s *QiniuStorage) inspectRawImage(ctx context.Context, objectKey string) (*ImageMeta, error) {
	targetURL := buildCDNURL(s.cdnDomain, objectKey, "")
	if targetURL == "" {
		return nil, ErrInvalidImage
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.inspectTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("storage build raw inspect request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storage request raw image: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrObjectNotFound
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError:
		return nil, ErrInvalidImage
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("storage raw image unexpected status %d", resp.StatusCode)
	}

	head, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		return nil, fmt.Errorf("storage read raw image header: %w", err)
	}

	sizeBytes := resp.ContentLength
	if sizeBytes <= 0 {
		rest, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, 20<<20+1))
		sizeBytes = int64(len(head)) + rest
	}

	width, height, format, ok := decodeImageHeader(head)
	if !ok || sizeBytes <= 0 || width <= 0 || height <= 0 {
		return nil, ErrInvalidImage
	}
	mimeType, ok := imageInfoFormatToMime[format]
	if !ok {
		return nil, ErrInvalidImage
	}

	return &ImageMeta{
		SizeBytes: sizeBytes,
		MimeType:  mimeType,
		Width:     uint32(width),
		Height:    uint32(height),
	}, nil
}

func decodeImageHeader(data []byte) (width, height int, format string, ok bool) {
	if w, h, isWebP := decodeWebPHeader(data); isWebP {
		return w, h, "webp", true
	}
	cfg, fmtName, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0, "", false
	}
	return cfg.Width, cfg.Height, strings.ToLower(fmtName), true
}

func decodeWebPHeader(b []byte) (int, int, bool) {
	if len(b) < 30 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WEBP" {
		return 0, 0, false
	}
	chunk := string(b[12:16])
	switch chunk {
	case "VP8X":
		w := 1 + (int(b[24]) | int(b[25])<<8 | int(b[26])<<16)
		h := 1 + (int(b[27]) | int(b[28])<<8 | int(b[29])<<16)
		return w, h, true
	case "VP8 ":
		if b[23] != 0x9d || b[24] != 0x01 || b[25] != 0x2a {
			return 0, 0, false
		}
		w := int(binary.LittleEndian.Uint16(b[26:28]) & 0x3fff)
		h := int(binary.LittleEndian.Uint16(b[28:30]) & 0x3fff)
		return w, h, true
	case "VP8L":
		if b[20] != 0x2f {
			return 0, 0, false
		}
		bits := binary.LittleEndian.Uint32(b[21:25])
		w := int(bits&0x3fff) + 1
		h := int((bits>>14)&0x3fff) + 1
		return w, h, true
	default:
		return 0, 0, false
	}
}

// BuildThumbURL 组装带七牛 imageView2 缩略图参数的 CDN 地址。
func (s *QiniuStorage) BuildThumbURL(objectKey string) string {
	return buildCDNURL(s.cdnDomain, objectKey, qiniuThumbQuery)
}

// BuildRawURL 组装原图 CDN 地址。
func (s *QiniuStorage) BuildRawURL(objectKey string) string {
	return buildCDNURL(s.cdnDomain, objectKey, "")
}
