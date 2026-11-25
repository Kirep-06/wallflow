package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"wallpaper-feed/internal/config"
)

const localTokenScope = "wallflow-local-upload-v1:"

type localUploadClaims struct {
	Key     string `json:"k"`
	MaxSize int64  `json:"m"`
	Exp     int64  `json:"e"`
}

// LocalStorage 为本地磁盘对象存储实现，适用于离线开发与单机部署，与七牛直传契约完全同构。
type LocalStorage struct {
	rootDir    string
	publicURL  string
	uploadURL  string
	signKey    []byte
	now        func() time.Time
}

// NewLocalStorage 校验并初始化本地磁盘存储目录。
func NewLocalStorage(cfg config.StorageConfig) (*LocalStorage, error) {
	dir := strings.TrimSpace(cfg.Local.Dir)
	if dir == "" {
		dir = "uploads"
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("storage: resolve local dir: %w", err)
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: create local dir %q: %w", absDir, err)
	}

	secret := cfg.Local.SignSecret
	if len(secret) < 16 {
		return nil, errors.New("storage: local signing secret is required")
	}
	mac := sha256.Sum256([]byte(localTokenScope + secret))

	publicURL := strings.TrimRight(strings.TrimSpace(cfg.Local.PublicURL), "/")
	if publicURL == "" {
		publicURL = "/api/v1/uploads/files"
	}
	uploadURL := strings.TrimRight(strings.TrimSpace(cfg.Local.UploadURL), "/")
	if uploadURL == "" {
		uploadURL = "/api/v1/uploads/local"
	}

	return &LocalStorage{
		rootDir:   absDir,
		publicURL: publicURL,
		uploadURL: uploadURL,
		signKey:   mac[:],
		now:       time.Now,
	}, nil
}

// GenerateUploadToken 签发绑定 object_key 与体积上限的短时本地上传 HMAC 凭证。
func (s *LocalStorage) GenerateUploadToken(_ context.Context, userID uint64, scene UploadScene, contentType string) (*UploadCredential, error) {
	maxBytes, ok := MaxBytesForScene(scene)
	if !ok {
		return nil, fmt.Errorf("storage: invalid upload scene %q", scene)
	}
	ext, ok := ExtForContentType(contentType)
	if !ok {
		return nil, fmt.Errorf("storage: unsupported content type %q", contentType)
	}

	now := s.now()
	objectKey, err := generateObjectKey(scene, userID, ext, now)
	if err != nil {
		return nil, err
	}

	claims := localUploadClaims{
		Key:     objectKey,
		MaxSize: maxBytes,
		Exp:     now.Add(uploadTokenTTL).Unix(),
	}
	token, err := s.signClaims(claims)
	if err != nil {
		return nil, err
	}

	return &UploadCredential{
		UploadToken:  token,
		UploadURL:    s.uploadURL,
		ObjectKey:    objectKey,
		ExpiresIn:    int64(uploadTokenTTL / time.Second),
		MaxSizeBytes: maxBytes,
	}, nil
}

// SaveUpload 核验本地上传凭证、校验真实图片头并原子写入目标文件。
func (s *LocalStorage) SaveUpload(_ context.Context, token, objectKey string, body io.Reader) error {
	claims, err := s.verifyToken(token)
	if err != nil {
		return err
	}
	cleanKey := strings.TrimLeft(strings.TrimSpace(objectKey), "/")
	if cleanKey == "" || cleanKey != claims.Key {
		return ErrInvalidUploadToken
	}

	destPath, err := s.ResolveFilePath(cleanKey)
	if err != nil {
		return ErrInvalidUploadToken
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("storage: prepare target dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(destPath), ".upload-*.tmp")
	if err != nil {
		return fmt.Errorf("storage: create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanup := func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}

	written, err := io.Copy(tmpFile, io.LimitReader(body, claims.MaxSize+1))
	if err != nil {
		cleanup()
		return fmt.Errorf("storage: write upload file: %w", err)
	}
	if written == 0 {
		cleanup()
		return ErrInvalidImage
	}
	if written > claims.MaxSize {
		cleanup()
		return ErrUploadTooLarge
	}

	// 落盘前校验真实图片头，拒绝非图片或伪造后缀文件进入磁盘。
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return fmt.Errorf("storage: seek temp file: %w", err)
	}
	head, err := io.ReadAll(io.LimitReader(tmpFile, 256*1024))
	if err != nil {
		cleanup()
		return fmt.Errorf("storage: read upload header: %w", err)
	}
	width, height, format, ok := decodeImageHeader(head)
	if !ok || width <= 0 || height <= 0 || width > math.MaxUint32 || height > math.MaxUint32 {
		cleanup()
		return ErrInvalidImage
	}
	mimeType, ok := imageInfoFormatToMime[format]
	if !ok {
		cleanup()
		return ErrInvalidImage
	}
	if expectedMime, ok := MimeFromObjectKey(cleanKey); !ok || mimeType != expectedMime {
		cleanup()
		return ErrInvalidImage
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("storage: close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("storage: commit local file: %w", err)
	}
	return nil
}

// InspectImage 从本地磁盘直接读取图片头与文件大小，无需外部网络请求。
func (s *LocalStorage) InspectImage(_ context.Context, objectKey string) (*ImageMeta, error) {
	fullPath, err := s.ResolveFilePath(objectKey)
	if err != nil {
		return nil, ErrInvalidImage
	}

	stat, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("storage: stat local file: %w", err)
	}
	if stat.IsDir() || stat.Size() <= 0 {
		return nil, ErrInvalidImage
	}

	f, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("storage: open local file: %w", err)
	}
	defer f.Close()

	head, err := io.ReadAll(io.LimitReader(f, 256*1024))
	if err != nil {
		return nil, fmt.Errorf("storage: read local image header: %w", err)
	}
	width, height, format, ok := decodeImageHeader(head)
	if !ok || width <= 0 || height <= 0 || width > math.MaxUint32 || height > math.MaxUint32 {
		return nil, ErrInvalidImage
	}
	mimeType, ok := imageInfoFormatToMime[format]
	if !ok {
		return nil, ErrInvalidImage
	}

	return &ImageMeta{
		SizeBytes: stat.Size(),
		MimeType:  mimeType,
		Width:     uint32(width),
		Height:    uint32(height),
	}, nil
}

// ResolveFilePath 安全解析对象键对应的本地绝对路径，阻断路径穿越与非白名单后缀。
func (s *LocalStorage) ResolveFilePath(objectKey string) (string, error) {
	trimmed := strings.TrimLeft(strings.TrimSpace(objectKey), "/")
	if trimmed == "" || len(trimmed) > MaxObjectKeyLen || strings.Contains(trimmed, "\\") || strings.Contains(trimmed, "..") {
		return "", ErrInvalidImage
	}
	cleaned := path.Clean("/" + trimmed)
	rel := strings.TrimPrefix(cleaned, "/")
	if rel == "" || strings.HasPrefix(rel, "..") {
		return "", ErrInvalidImage
	}
	if _, ok := MimeFromObjectKey(rel); !ok {
		return "", ErrInvalidImage
	}

	fullPath := filepath.Join(s.rootDir, filepath.FromSlash(rel))
	relCheck, err := filepath.Rel(s.rootDir, fullPath)
	if err != nil || relCheck == "." || strings.HasPrefix(relCheck, "..") {
		return "", ErrInvalidImage
	}
	return fullPath, nil
}

// BuildThumbURL 组装本地图片访问地址。
func (s *LocalStorage) BuildThumbURL(objectKey string) string {
	return buildCDNURL(s.publicURL, objectKey, "")
}

// BuildRawURL 组装本地原图访问地址。
func (s *LocalStorage) BuildRawURL(objectKey string) string {
	return buildCDNURL(s.publicURL, objectKey, "")
}

func (s *LocalStorage) signClaims(claims localUploadClaims) (string, error) {
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("storage: marshal local token claims: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, s.signKey)
	_, _ = mac.Write([]byte(encoded))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + sig, nil
}

func (s *LocalStorage) verifyToken(token string) (*localUploadClaims, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, ErrInvalidUploadToken
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidUploadToken
	}
	mac := hmac.New(sha256.New, s.signKey)
	_, _ = mac.Write([]byte(parts[0]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, ErrInvalidUploadToken
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrInvalidUploadToken
	}
	var claims localUploadClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, ErrInvalidUploadToken
	}
	if claims.Key == "" || claims.MaxSize <= 0 || s.now().Unix() > claims.Exp {
		return nil, ErrInvalidUploadToken
	}
	return &claims, nil
}
