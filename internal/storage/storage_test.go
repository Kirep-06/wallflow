package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"wallpaper-feed/internal/config"
)

func TestValidateObjectKey(t *testing.T) {
	validHex := "9f3c2e8a4b1d4a7c8e5f1a2b3c4d5e6f"
	tests := []struct {
		name   string
		scene  UploadScene
		userID uint64
		key    string
		want   bool
	}{
		{name: "valid wallpaper webp", scene: SceneWallpaper, userID: 10001, key: "wallpapers/10001/20261122/" + validHex + ".webp", want: true},
		{name: "valid wallpaper jpg", scene: SceneWallpaper, userID: 10001, key: "wallpapers/10001/20261122/" + validHex + ".jpg", want: true},
		{name: "valid avatar png", scene: SceneAvatar, userID: 42, key: "avatars/42/20261122/" + validHex + ".png", want: true},
		{name: "wrong user id", scene: SceneWallpaper, userID: 10001, key: "wallpapers/10002/20261122/" + validHex + ".webp", want: false},
		{name: "cross scene mismatch", scene: SceneWallpaper, userID: 10001, key: "avatars/10001/20261122/" + validHex + ".webp", want: false},
		{name: "uppercase hex rejected", scene: SceneWallpaper, userID: 10001, key: "wallpapers/10001/20261122/" + strings.ToUpper(validHex) + ".webp", want: false},
		{name: "unsupported ext gif", scene: SceneWallpaper, userID: 10001, key: "wallpapers/10001/20261122/" + validHex + ".gif", want: false},
		{name: "path traversal rejected", scene: SceneWallpaper, userID: 10001, key: "wallpapers/10001/../10002/20261122/" + validHex + ".jpg", want: false},
		{name: "loose m1 avatar rejected", scene: SceneAvatar, userID: 42, key: "avatars/42/avatar.png", want: false},
		{name: "zero user id", scene: SceneAvatar, userID: 0, key: "avatars/0/20261122/" + validHex + ".png", want: false},
		{name: "empty key", scene: SceneWallpaper, userID: 1, key: "", want: false},
		{name: "too long key", scene: SceneWallpaper, userID: 1, key: "wallpapers/1/" + strings.Repeat("a", MaxObjectKeyLen), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ValidateObjectKey(tc.scene, tc.userID, tc.key); got != tc.want {
				t.Fatalf("ValidateObjectKey(%q, %d, %q) = %v, want %v", tc.scene, tc.userID, tc.key, got, tc.want)
			}
		})
	}
}

func TestZoneMappingAndURLs(t *testing.T) {
	zones := map[string]string{
		"z0":        "https://upload.qiniup.com",
		"z1":        "https://upload-z1.qiniup.com",
		"z2":        "https://upload-z2.qiniup.com",
		"na0":       "https://upload-na0.qiniup.com",
		"as0":       "https://upload-as0.qiniup.com",
		"cn-east-2": "https://upload-cn-east-2.qiniup.com",
	}

	for zone, wantURL := range zones {
		t.Run("zone_"+zone, func(t *testing.T) {
			s, err := New(config.StorageConfig{
				Provider:  config.StorageProviderQiniu,
				CDNDomain: "https://wp.example.com/",
				Qiniu: config.QiniuConfig{
					Bucket:    "wp-bucket",
					Zone:      zone,
					AccessKey: "test-ak",
					SecretKey: "test-sk",
				},
			})
			if err != nil {
				t.Fatalf("New(qiniu, %s) failed: %v", zone, err)
			}
			cred, err := s.GenerateUploadToken(context.Background(), 10001, SceneWallpaper, "image/webp")
			if err != nil {
				t.Fatalf("GenerateUploadToken failed: %v", err)
			}
			if cred.UploadURL != wantURL {
				t.Fatalf("UploadURL = %q, want %q", cred.UploadURL, wantURL)
			}
			if raw := s.BuildRawURL("/wallpapers/1/a.webp"); raw != "https://wp.example.com/wallpapers/1/a.webp" {
				t.Fatalf("BuildRawURL = %q", raw)
			}
			if thumb := s.BuildThumbURL("wallpapers/1/a.webp"); thumb != "https://wp.example.com/wallpapers/1/a.webp?"+qiniuThumbQuery {
				t.Fatalf("BuildThumbURL = %q", thumb)
			}
		})
	}

	t.Run("noop urls and disabled errors", func(t *testing.T) {
		s, err := New(config.StorageConfig{Provider: config.StorageProviderNoop, CDNDomain: "https://wp.example.com/"})
		if err != nil {
			t.Fatalf("New(noop) failed: %v", err)
		}
		if _, err := s.GenerateUploadToken(context.Background(), 1, SceneWallpaper, "image/png"); !errors.Is(err, ErrStorageDisabled) {
			t.Fatalf("GenerateUploadToken err = %v, want ErrStorageDisabled", err)
		}
		if _, err := s.InspectImage(context.Background(), "wallpapers/1/a.png"); !errors.Is(err, ErrStorageDisabled) {
			t.Fatalf("InspectImage err = %v, want ErrStorageDisabled", err)
		}
		if got := s.BuildThumbURL("wallpapers/1/a.png"); got != "https://wp.example.com/wallpapers/1/a.png" {
			t.Fatalf("noop BuildThumbURL = %q, want raw url", got)
		}
		if got := NewNoopStorage("").BuildRawURL("wallpapers/1/a.png"); got != "" {
			t.Fatalf("empty cdn BuildRawURL = %q, want empty", got)
		}
	})
}

func TestQiniuPutPolicy(t *testing.T) {
	s, err := NewQiniuStorage(config.StorageConfig{
		Provider:  config.StorageProviderQiniu,
		CDNDomain: "https://wp.example.com",
		Qiniu: config.QiniuConfig{
			Bucket:    "my-bucket",
			Zone:      "z0",
			AccessKey: "ak-123",
			SecretKey: "sk-456",
		},
	})
	if err != nil {
		t.Fatalf("NewQiniuStorage failed: %v", err)
	}

	before := time.Now().Unix()
	cred, err := s.GenerateUploadToken(context.Background(), 88, SceneAvatar, "image/jpeg")
	if err != nil {
		t.Fatalf("GenerateUploadToken failed: %v", err)
	}
	after := time.Now().Unix()

	if !ValidateObjectKey(SceneAvatar, 88, cred.ObjectKey) || !strings.HasSuffix(cred.ObjectKey, ".jpg") {
		t.Fatalf("unexpected generated object key: %q", cred.ObjectKey)
	}
	if cred.ExpiresIn != 600 || cred.MaxSizeBytes != AvatarMaxBytes {
		t.Fatalf("unexpected credential metadata: %+v", cred)
	}

	parts := strings.Split(cred.UploadToken, ":")
	if len(parts) != 3 || parts[0] != "ak-123" {
		t.Fatalf("unexpected token format: %q", cred.UploadToken)
	}
	policyBytes, err := base64.URLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode put policy: %v", err)
	}

	var decoded struct {
		Scope      string `json:"scope"`
		InsertOnly uint16 `json:"insertOnly"`
		FsizeMin   int64  `json:"fsizeMin"`
		FsizeLimit int64  `json:"fsizeLimit"`
		MimeLimit  string `json:"mimeLimit"`
		Deadline   int64  `json:"deadline"`
	}
	if err := json.Unmarshal(policyBytes, &decoded); err != nil {
		t.Fatalf("unmarshal put policy: %v", err)
	}
	if decoded.Scope != "my-bucket:"+cred.ObjectKey {
		t.Fatalf("scope = %q, want %q", decoded.Scope, "my-bucket:"+cred.ObjectKey)
	}
	if decoded.InsertOnly != 1 || decoded.FsizeMin != 1 || decoded.FsizeLimit != AvatarMaxBytes {
		t.Fatalf("unexpected policy limits: %+v", decoded)
	}
	if decoded.MimeLimit != allowedMimeLimit {
		t.Fatalf("mimeLimit = %q, want %q", decoded.MimeLimit, allowedMimeLimit)
	}
	if decoded.Deadline < before+600 || decoded.Deadline > after+600 {
		t.Fatalf("deadline = %d, expected around %d..%d", decoded.Deadline, before+600, after+600)
	}
}

func TestQiniuInspectImage(t *testing.T) {
	newTestStorage := func(t *testing.T, handler http.HandlerFunc, timeout time.Duration) *QiniuStorage {
		t.Helper()
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)

		s, err := NewQiniuStorage(config.StorageConfig{
			Provider:  config.StorageProviderQiniu,
			CDNDomain: srv.URL,
			Qiniu: config.QiniuConfig{
				Bucket:    "wp",
				Zone:      "z0",
				AccessKey: "ak",
				SecretKey: "sk",
			},
		})
		if err != nil {
			t.Fatalf("NewQiniuStorage failed: %v", err)
		}
		s.httpClient = srv.Client()
		if timeout > 0 {
			s.inspectTimeout = timeout
		}
		return s
	}

	t.Run("success", func(t *testing.T) {
		var gotPath, gotQuery string
		s := newTestStorage(t, func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"size":4194304,"format":"webp","width":3840,"height":2160}`))
		}, 0)

		meta, err := s.InspectImage(context.Background(), "wallpapers/1/20261122/abc.webp")
		if err != nil {
			t.Fatalf("InspectImage failed: %v", err)
		}
		if gotPath != "/wallpapers/1/20261122/abc.webp" || gotQuery != "imageInfo" {
			t.Fatalf("unexpected request: path=%q query=%q", gotPath, gotQuery)
		}
		if meta.SizeBytes != 4194304 || meta.MimeType != "image/webp" || meta.Width != 3840 || meta.Height != 2160 {
			t.Fatalf("unexpected meta: %+v", meta)
		}
	})

	t.Run("404 returns ErrObjectNotFound", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}, 0)
		if _, err := s.InspectImage(context.Background(), "wallpapers/1/missing.jpg"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("err = %v, want ErrObjectNotFound", err)
		}
	})

	t.Run("400 returns ErrInvalidImage", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}, 0)
		if _, err := s.InspectImage(context.Background(), "wallpapers/1/bad.jpg"); !errors.Is(err, ErrInvalidImage) {
			t.Fatalf("err = %v, want ErrInvalidImage", err)
		}
	})

	t.Run("500 returns internal error not sentinel", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}, 0)
		_, err := s.InspectImage(context.Background(), "wallpapers/1/err.jpg")
		if err == nil || errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrInvalidImage) {
			t.Fatalf("err = %v, want non-sentinel upstream error", err)
		}
	})

	t.Run("oversized body over 8KiB returns ErrInvalidImage", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat(" ", maxImageInfoBytes+10)))
		}, 0)
		if _, err := s.InspectImage(context.Background(), "wallpapers/1/huge.jpg"); !errors.Is(err, ErrInvalidImage) {
			t.Fatalf("err = %v, want ErrInvalidImage", err)
		}
	})

	t.Run("invalid json returns ErrInvalidImage", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{not-json`))
		}, 0)
		if _, err := s.InspectImage(context.Background(), "wallpapers/1/badjson.jpg"); !errors.Is(err, ErrInvalidImage) {
			t.Fatalf("err = %v, want ErrInvalidImage", err)
		}
	})

	t.Run("disallowed format returns ErrInvalidImage", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"size":1024,"format":"gif","width":1920,"height":1080}`))
		}, 0)
		if _, err := s.InspectImage(context.Background(), "wallpapers/1/anim.gif"); !errors.Is(err, ErrInvalidImage) {
			t.Fatalf("err = %v, want ErrInvalidImage", err)
		}
	})

	t.Run("timeout returns error not sentinel", func(t *testing.T) {
		s := newTestStorage(t, func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(300 * time.Millisecond):
				_, _ = w.Write([]byte(`{"size":1024,"format":"png","width":1920,"height":1080}`))
			case <-r.Context().Done():
			}
		}, 50*time.Millisecond)

		_, err := s.InspectImage(context.Background(), "wallpapers/1/slow.png")
		if err == nil || errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrInvalidImage) {
			t.Fatalf("err = %v, want timeout error", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
	})
}
