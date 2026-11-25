package storage

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"
	"time"

	"wallpaper-feed/internal/config"
)

func makeTestPNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	img.Set(0, 0, color.RGBA{R: 12, G: 116, B: 204, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode test png: %v", err)
	}
	return buf.Bytes()
}

func TestLocalStorage_UploadAndInspectFlow(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStorage(config.StorageConfig{
		Provider: config.StorageProviderLocal,
		Local: config.LocalStorageConfig{
			Dir:        dir,
			PublicURL:  "/api/v1/uploads/files",
			UploadURL:  "/api/v1/uploads/local",
			SignSecret: strings.Repeat("s", 32),
		},
	})
	if err != nil {
		t.Fatalf("NewLocalStorage failed: %v", err)
	}

	ctx := context.Background()
	cred, err := store.GenerateUploadToken(ctx, 7, SceneWallpaper, "image/png")
	if err != nil {
		t.Fatalf("GenerateUploadToken failed: %v", err)
	}
	if cred.UploadURL != "/api/v1/uploads/local" {
		t.Fatalf("unexpected upload url: %s", cred.UploadURL)
	}
	if !ValidateObjectKey(SceneWallpaper, 7, cred.ObjectKey) {
		t.Fatalf("generated object key failed validation: %s", cred.ObjectKey)
	}

	pngBytes := makeTestPNG(t, 320, 180)
	if err := store.SaveUpload(ctx, cred.UploadToken, cred.ObjectKey, bytes.NewReader(pngBytes)); err != nil {
		t.Fatalf("SaveUpload failed: %v", err)
	}

	meta, err := store.InspectImage(ctx, cred.ObjectKey)
	if err != nil {
		t.Fatalf("InspectImage failed: %v", err)
	}
	if meta.Width != 320 || meta.Height != 180 || meta.MimeType != "image/png" || meta.SizeBytes != int64(len(pngBytes)) {
		t.Fatalf("unexpected image meta: %+v", meta)
	}

	if got := store.BuildRawURL(cred.ObjectKey); got != "/api/v1/uploads/files/"+cred.ObjectKey {
		t.Fatalf("unexpected raw url: %s", got)
	}
}

func TestLocalStorage_SecurityGuards(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	store, err := NewLocalStorage(config.StorageConfig{
		Provider: config.StorageProviderLocal,
		Local: config.LocalStorageConfig{
			Dir:        dir,
			SignSecret: strings.Repeat("k", 32),
		},
	})
	if err != nil {
		t.Fatalf("NewLocalStorage failed: %v", err)
	}
	store.now = func() time.Time { return now }

	ctx := context.Background()
	cred, err := store.GenerateUploadToken(ctx, 3, SceneAvatar, "image/png")
	if err != nil {
		t.Fatalf("GenerateUploadToken failed: %v", err)
	}

	// 1. 伪造非图片内容应被拒绝
	if err := store.SaveUpload(ctx, cred.UploadToken, cred.ObjectKey, strings.NewReader("not-an-image")); !errors.Is(err, ErrInvalidImage) {
		t.Fatalf("expected ErrInvalidImage for bogus content, got %v", err)
	}

	// 2. 篡改 objectKey 应被拒绝
	if err := store.SaveUpload(ctx, cred.UploadToken, "avatars/3/20261125/ffffffffffffffffffffffffffffffff.png", bytes.NewReader(makeTestPNG(t, 120, 120))); !errors.Is(err, ErrInvalidUploadToken) {
		t.Fatalf("expected ErrInvalidUploadToken for mismatched key, got %v", err)
	}

	// 3. 路径穿越应被拒绝
	if _, err := store.ResolveFilePath("../secret.png"); !errors.Is(err, ErrInvalidImage) {
		t.Fatalf("expected ErrInvalidImage for traversal path, got %v", err)
	}

	// 4. 过期 token 应被拒绝
	store.now = func() time.Time { return now.Add(uploadTokenTTL + time.Minute) }
	if err := store.SaveUpload(ctx, cred.UploadToken, cred.ObjectKey, bytes.NewReader(makeTestPNG(t, 120, 120))); !errors.Is(err, ErrInvalidUploadToken) {
		t.Fatalf("expected ErrInvalidUploadToken for expired token, got %v", err)
	}
}
