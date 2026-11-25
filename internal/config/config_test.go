package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
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
	if cfg.RabbitMQ.Enabled {
		t.Fatalf("expected default rabbitmq.enabled=false")
	}

	t.Setenv(EnvJWTSecret, "short")
	if _, err := Load(examplePath); err == nil {
		t.Fatalf("expected error for short %s", EnvJWTSecret)
	}
}

func TestLoadStorageConfig(t *testing.T) {
	validJWT := "0123456789abcdef0123456789abcdef"

	t.Run("local provider fills defaults and supports env override", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		t.Setenv(EnvStorageProvider, StorageProviderLocal)
		t.Setenv(EnvStorageLocalDir, "/tmp/wf-uploads")
		p := writeTempConfig(t, "storage:\n  provider: noop\n")
		cfg, err := Load(p)
		if err != nil {
			t.Fatalf("Load(local) failed: %v", err)
		}
		if cfg.Storage.Provider != StorageProviderLocal ||
			cfg.Storage.Local.Dir != "/tmp/wf-uploads" ||
			cfg.Storage.Local.PublicURL != "/api/v1/uploads/files" ||
			cfg.Storage.Local.UploadURL != "/api/v1/uploads/local" ||
			cfg.Storage.Local.SignSecret != validJWT {
			t.Fatalf("unexpected local storage config: %+v", cfg.Storage)
		}
	})

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

func TestLoadRabbitMQConfig(t *testing.T) {
	validJWT := "0123456789abcdef0123456789abcdef"

	t.Run("disabled skips validation", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		t.Setenv(EnvRabbitMQPassword, "")
		path := writeTempConfig(t, "rabbitmq:\n  enabled: false\n")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.RabbitMQ.Enabled {
			t.Fatal("expected rabbitmq disabled")
		}
	})

	t.Run("enabled validates required host user and port", func(t *testing.T) {
		cases := []struct {
			name string
			yaml string
		}{
			{name: "missing host", yaml: "rabbitmq:\n  enabled: true\n  user: guest\n"},
			{name: "missing user", yaml: "rabbitmq:\n  enabled: true\n  host: 127.0.0.1\n"},
			{name: "port out of range", yaml: "rabbitmq:\n  enabled: true\n  host: 127.0.0.1\n  user: guest\n  port: 70000\n"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv(EnvJWTSecret, validJWT)
				t.Setenv(EnvRabbitMQPassword, "")
				path := writeTempConfig(t, tc.yaml)
				if _, err := Load(path); err == nil {
					t.Fatalf("expected error for case %q", tc.name)
				}
			})
		}
	})

	t.Run("env password overrides yaml and URI escapes special chars", func(t *testing.T) {
		t.Setenv(EnvJWTSecret, validJWT)
		specialPass := "p@ss:w/rd#1?%20"
		t.Setenv(EnvRabbitMQPassword, specialPass)
		path := writeTempConfig(t, "rabbitmq:\n  enabled: true\n  host: mq.local\n  user: wp_user\n  password: yaml-password\n  vhost: /wallpaper\n")

		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.RabbitMQ.Port != 5672 {
			t.Fatalf("port = %d, want default 5672", cfg.RabbitMQ.Port)
		}
		if cfg.RabbitMQ.Password != specialPass {
			t.Fatalf("password not overridden by env")
		}

		rawURI := cfg.RabbitMQ.URI()
		if strings.Contains(rawURI, specialPass) {
			t.Fatalf("URI must percent-encode special characters, got %q", rawURI)
		}
		parsed, err := amqp.ParseURI(rawURI)
		if err != nil {
			t.Fatalf("amqp.ParseURI(%q) failed: %v", rawURI, err)
		}
		if parsed.Host != "mq.local" || parsed.Port != 5672 || parsed.Username != "wp_user" || parsed.Password != specialPass || parsed.Vhost != "/wallpaper" {
			t.Fatalf("round-trip parsed URI mismatch: %+v", parsed)
		}

		safe := cfg.RabbitMQ.SafeAddr()
		if safe != "mq.local:5672/wallpaper" || strings.Contains(safe, specialPass) || strings.Contains(safe, "wp_user") {
			t.Fatalf("unexpected SafeAddr: %q", safe)
		}
	})
}
