package service

import (
	"context"
	"errors"
	"testing"

	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/errno"
	"wallpaper-feed/internal/storage"
)

var _ storage.StorageService = (*stubStorage)(nil)

type stubStorage struct {
	tokenFn   func(ctx context.Context, userID uint64, scene storage.UploadScene, contentType string) (*storage.UploadCredential, error)
	inspectFn func(ctx context.Context, objectKey string) (*storage.ImageMeta, error)
}

func (s *stubStorage) GenerateUploadToken(ctx context.Context, userID uint64, scene storage.UploadScene, contentType string) (*storage.UploadCredential, error) {
	if s.tokenFn != nil {
		return s.tokenFn(ctx, userID, scene, contentType)
	}
	maxBytes, _ := storage.MaxBytesForScene(scene)
	return &storage.UploadCredential{
		UploadToken:  "stub-token",
		UploadURL:    "https://upload.qiniup.com",
		ObjectKey:    "wallpapers/1/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp",
		ExpiresIn:    600,
		MaxSizeBytes: maxBytes,
	}, nil
}

func (s *stubStorage) InspectImage(ctx context.Context, objectKey string) (*storage.ImageMeta, error) {
	if s.inspectFn != nil {
		return s.inspectFn(ctx, objectKey)
	}
	return &storage.ImageMeta{
		SizeBytes: 1024 * 1024,
		MimeType:  "image/webp",
		Width:     3840,
		Height:    2160,
	}, nil
}

func (s *stubStorage) BuildThumbURL(objectKey string) string {
	return "https://cdn.example.com/" + objectKey + "?thumb"
}

func (s *stubStorage) BuildRawURL(objectKey string) string {
	return "https://cdn.example.com/" + objectKey
}

func TestUploadServiceCreateUploadToken(t *testing.T) {
	t.Run("valid wallpaper request", func(t *testing.T) {
		svc := NewUploadService(&stubStorage{})
		res, err := svc.CreateUploadToken(context.Background(), 10001, model.UploadTokenRequest{
			Scene:       "wallpaper",
			ContentType: "image/webp",
			FileSize:    4 << 20,
		})
		if err != nil {
			t.Fatalf("CreateUploadToken failed: %v", err)
		}
		if res.UploadToken != "stub-token" || res.MaxSizeBytes != storage.WallpaperMaxBytes {
			t.Fatalf("unexpected result: %+v", res)
		}
	})

	t.Run("validation errors return 10002", func(t *testing.T) {
		svc := NewUploadService(&stubStorage{})
		tests := []struct {
			name string
			req  model.UploadTokenRequest
		}{
			{name: "bad scene", req: model.UploadTokenRequest{Scene: "banner", ContentType: "image/png", FileSize: 100}},
			{name: "bad content type", req: model.UploadTokenRequest{Scene: "wallpaper", ContentType: "image/gif", FileSize: 100}},
			{name: "zero file size", req: model.UploadTokenRequest{Scene: "wallpaper", ContentType: "image/png", FileSize: 0}},
			{name: "negative file size", req: model.UploadTokenRequest{Scene: "wallpaper", ContentType: "image/png", FileSize: -1}},
			{name: "wallpaper over 20MiB", req: model.UploadTokenRequest{Scene: "wallpaper", ContentType: "image/png", FileSize: storage.WallpaperMaxBytes + 1}},
			{name: "avatar over 5MiB", req: model.UploadTokenRequest{Scene: "avatar", ContentType: "image/jpeg", FileSize: storage.AvatarMaxBytes + 1}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, err := svc.CreateUploadToken(context.Background(), 1, tc.req)
				if !errors.Is(err, errno.ErrInvalidParam) {
					t.Fatalf("err = %v, want ErrInvalidParam", err)
				}
			})
		}
	})

	t.Run("disabled storage maps to 10001", func(t *testing.T) {
		svc := NewUploadService(&stubStorage{
			tokenFn: func(context.Context, uint64, storage.UploadScene, string) (*storage.UploadCredential, error) {
				return nil, storage.ErrStorageDisabled
			},
		})
		_, err := svc.CreateUploadToken(context.Background(), 1, model.UploadTokenRequest{
			Scene:       "avatar",
			ContentType: "image/png",
			FileSize:    1024,
		})
		if !errors.Is(err, errno.ErrInternal) {
			t.Fatalf("err = %v, want ErrInternal", err)
		}
	})
}

func TestVerifyWallpaperObject(t *testing.T) {
	validKey := "wallpapers/10001/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp"

	tests := []struct {
		name    string
		key     string
		meta    *storage.ImageMeta
		inspect error
		wantErr error
	}{
		{name: "valid 4k", key: validKey, meta: &storage.ImageMeta{SizeBytes: 2 << 20, MimeType: "image/webp", Width: 3840, Height: 2160}},
		{name: "valid min boundary", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1, MimeType: "image/jpeg", Width: 1280, Height: 480}},
		{name: "valid portrait max aspect 4.0", key: validKey, meta: &storage.ImageMeta{SizeBytes: storage.WallpaperMaxBytes, MimeType: "image/png", Width: 500, Height: 2000}},
		{name: "wrong owner returns 10002", key: "wallpapers/999/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.webp", wantErr: errno.ErrInvalidParam},
		{name: "404 returns 30001", key: validKey, inspect: storage.ErrObjectNotFound, wantErr: errno.ErrUploadVerifyFailed},
		{name: "invalid image returns 30001", key: validKey, inspect: storage.ErrInvalidImage, wantErr: errno.ErrUploadVerifyFailed},
		{name: "upstream 5xx returns 10001", key: validKey, inspect: errors.New("502 bad gateway"), wantErr: errno.ErrInternal},
		{name: "noop disabled returns 10001", key: validKey, inspect: storage.ErrStorageDisabled, wantErr: errno.ErrInternal},
		{name: "size over 20MiB returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: storage.WallpaperMaxBytes + 1, MimeType: "image/png", Width: 1920, Height: 1080}, wantErr: errno.ErrUploadVerifyFailed},
		{name: "long edge below 1280 returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1024, MimeType: "image/png", Width: 1279, Height: 720}, wantErr: errno.ErrUploadVerifyFailed},
		{name: "short edge below 480 returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1024, MimeType: "image/png", Width: 1920, Height: 479}, wantErr: errno.ErrUploadVerifyFailed},
		{name: "aspect ratio over 4.0 returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1024, MimeType: "image/png", Width: 2001, Height: 500}, wantErr: errno.ErrUploadVerifyFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &stubStorage{
				inspectFn: func(context.Context, string) (*storage.ImageMeta, error) {
					return tc.meta, tc.inspect
				},
			}
			_, err := verifyWallpaperObject(context.Background(), store, 10001, tc.key)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyAvatarObject(t *testing.T) {
	validKey := "avatars/42/20261122/9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f.png"

	tests := []struct {
		name    string
		key     string
		meta    *storage.ImageMeta
		inspect error
		wantErr error
	}{
		{name: "valid min 100x100", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1024, MimeType: "image/png", Width: 100, Height: 100}},
		{name: "valid max 4096x4096", key: validKey, meta: &storage.ImageMeta{SizeBytes: storage.AvatarMaxBytes, MimeType: "image/jpeg", Width: 4096, Height: 4096}},
		{name: "loose m1 avatar key rejected with 10002", key: "avatars/42/my-avatar.png", wantErr: errno.ErrInvalidParam},
		{name: "size over 5MiB returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: storage.AvatarMaxBytes + 1, MimeType: "image/png", Width: 512, Height: 512}, wantErr: errno.ErrUploadVerifyFailed},
		{name: "width below 100 returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1024, MimeType: "image/png", Width: 99, Height: 200}, wantErr: errno.ErrUploadVerifyFailed},
		{name: "height above 4096 returns 30001", key: validKey, meta: &storage.ImageMeta{SizeBytes: 1024, MimeType: "image/png", Width: 512, Height: 4097}, wantErr: errno.ErrUploadVerifyFailed},
		{name: "missing object returns 30001", key: validKey, inspect: storage.ErrObjectNotFound, wantErr: errno.ErrUploadVerifyFailed},
		{name: "upstream error returns 10001", key: validKey, inspect: errors.New("timeout"), wantErr: errno.ErrInternal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &stubStorage{
				inspectFn: func(context.Context, string) (*storage.ImageMeta, error) {
					return tc.meta, tc.inspect
				},
			}
			err := verifyAvatarObject(context.Background(), store, 42, tc.key)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
