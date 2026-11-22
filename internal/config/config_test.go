package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoad(t *testing.T) {
	examplePath := filepath.Join("..", "..", "configs", "config.example.yaml")

	t.Setenv(EnvJWTSecret, "0123456789abcdef0123456789abcdef")
	t.Setenv(EnvQiniuAccessKey, "")
	t.Setenv(EnvQiniuSecretKey, "")

	cfg, err := Load(examplePath)
	if err != nil {
		t.Fatalf("Load(example) failed: %v", err)
	}
	if cfg.Server.Addr() != ":8080" || !cfg.IsDev() {
		t.Fatalf("unexpected server config: %+v", cfg.Server)
	}
	if cfg.Redis.Addr() != "127.0.0.1:6379" {
		t.Fatalf("unexpected redis addr: %q", cfg.Redis.Addr())
	}
	if dsn := cfg.MySQL.DSN(); !strings.Contains(dsn, "@tcp(127.0.0.1:3306)/wallpaper_feed?") || !strings.Contains(dsn, "loc=UTC") {
		t.Fatalf("unexpected mysql dsn: %q", dsn)
	}
	if cfg.Storage.Provider != StorageProviderNoop {
		t.Fatalf("unexpected default storage provider: %q", cfg.Storage.Provider)
	}

	t.Setenv(EnvJWTSecret, "short")
	if _, err := Load(examplePath); err == nil {
		t.Fatalf("expected error for short %s", EnvJWTSecret)
	}
}

func TestLoadStorageConfig(t *testing.T) {
	validJWT := "0123456789abcdef0123456789abcdef"

	t.Run("empty provider defaults to noop", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		t.Setenv(EnvQiniuAccessKey, "")
		t.Setenv(EnvQiniuSecretKey, "")
		path := writeTempConfig(t, "storage:\n  cdn_domain: https://cdn.example.com/\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.Storage.Provider != StorageProviderNoop {
			t.Fatalf("provider = %q, want %q", cfg.Storage.Provider, StorageProviderNoop)
		}
		if cfg.Storage.CDNDomain != "https://cdn.example.com" {
			t.Fatalf("cdn_domain = %q, want trimmed", cfg.Storage.CDNDomain)
		}
	})

	t.Run("unsupported provider fails", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		path := writeTempConfig(t, "storage:\n  provider: s3\n")
		if _, err := Load(path); err == nil {
			t.Fatal("expected error for unsupported storage provider")
		}
	})

	t.Run("qiniu missing credentials or fields fails", func(t *testing.T) {
		tests := []struct {
			name string
			yaml string
			ak   string
			sk   string
		}{
			{
				name: "missing ak",
				yaml: "storage:\n  provider: qiniu\n  cdn_domain: https://cdn.example.com\n  qiniu:\n    bucket: wp\n    zone: z0\n",
				sk:   "secret",
			},
			{
				name: "missing sk",
				yaml: "storage:\n  provider: qiniu\n  cdn_domain: https://cdn.example.com\n  qiniu:\n    bucket: wp\n    zone: z0\n",
				ak:   "access",
			},
			{
				name: "ak sk in yaml ignored by mapstructure",
				yaml: "storage:\n  provider: qiniu\n  cdn_domain: https://cdn.example.com\n  qiniu:\n    bucket: wp\n    zone: z0\n    access_key: yaml-ak\n    secret_key: yaml-sk\n",
			},
			{
				name: "missing bucket",
				yaml: "storage:\n  provider: qiniu\n  cdn_domain: https://cdn.example.com\n  qiniu:\n    zone: z0\n",
				ak:   "access",
				sk:   "secret",
			},
			{
				name: "missing cdn_domain",
				yaml: "storage:\n  provider: qiniu\n  qiniu:\n    bucket: wp\n    zone: z0\n",
				ak:   "access",
				sk:   "secret",
			},
			{
				name: "invalid zone without upload_url",
				yaml: "storage:\n  provider: qiniu\n  cdn_domain: https://cdn.example.com\n  qiniu:\n    bucket: wp\n    zone: bad-zone\n",
				ak:   "access",
				sk:   "secret",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(EnvJWTSecret, validJWT)
				t.Setenv(EnvQiniuAccessKey, tc.ak)
				t.Setenv(EnvQiniuSecretKey, tc.sk)
				path := writeTempConfig(t, tc.yaml)
				if _, err := Load(path); err == nil {
					t.Fatalf("expected error for case %q", tc.name)
				}
			})
		}
	})

	t.Run("qiniu valid with zone and env credentials", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		t.Setenv(EnvQiniuAccessKey, "test-ak")
		t.Setenv(EnvQiniuSecretKey, "test-sk")
		path := writeTempConfig(t, "storage:\n  provider: QINIU\n  cdn_domain: https://cdn.example.com/\n  qiniu:\n    bucket: wp-bucket\n    zone: cn-east-2\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.Storage.Provider != StorageProviderQiniu {
			t.Fatalf("provider = %q, want qiniu", cfg.Storage.Provider)
		}
		if cfg.Storage.Qiniu.AccessKey != "test-ak" || cfg.Storage.Qiniu.SecretKey != "test-sk" {
			t.Fatalf("unexpected credentials loaded")
		}
		u, err := cfg.Storage.Qiniu.ResolveUploadURL()
		if err != nil || u != "https://upload-cn-east-2.qiniup.com" {
			t.Fatalf("ResolveUploadURL() = (%q, %v)", u, err)
		}
	})

	t.Run("qiniu valid with explicit upload_url and empty zone", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		t.Setenv(EnvQiniuAccessKey, "test-ak")
		t.Setenv(EnvQiniuSecretKey, "test-sk")
		path := writeTempConfig(t, "storage:\n  provider: qiniu\n  cdn_domain: https://cdn.example.com\n  qiniu:\n    bucket: wp-bucket\n    upload_url: https://custom-up.example.com/\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		u, err := cfg.Storage.Qiniu.ResolveUploadURL()
		if err != nil || u != "https://custom-up.example.com" {
			t.Fatalf("ResolveUploadURL() = (%q, %v)", u, err)
		}
	})
}
