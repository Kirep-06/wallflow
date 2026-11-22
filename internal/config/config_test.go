package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad(t *testing.T) {
	examplePath := filepath.Join("..", "..", "configs", "config.example.yaml")

	t.Setenv(EnvJWTSecret, "0123456789abcdef0123456789abcdef")
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

	t.Setenv(EnvJWTSecret, "short")
	if _, err := Load(examplePath); err == nil {
		t.Fatalf("expected error for short %s", EnvJWTSecret)
	}
}
