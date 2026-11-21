// Package config 负责加载 YAML 配置并支持环境变量覆盖。
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"golang.org/x/crypto/bcrypt"
)

type Config struct {
	App     AppConfig     `mapstructure:"app"`
	HTTP    HTTPConfig    `mapstructure:"http"`
	MySQL   MySQLConfig   `mapstructure:"mysql"`
	Redis   RedisConfig   `mapstructure:"redis"`
	Cache   CacheConfig   `mapstructure:"cache"`
	Feed    FeedConfig    `mapstructure:"feed"`
	Auth    AuthConfig    `mapstructure:"auth"`
	User    UserConfig    `mapstructure:"user"`
	Storage StorageConfig `mapstructure:"storage"`
	Worker  WorkerConfig  `mapstructure:"worker"`
}

type AppConfig struct {
	Env string `mapstructure:"env"` // dev | prod
}

func (a AppConfig) IsDev() bool { return a.Env == "dev" }

type HTTPConfig struct {
	Addr                   string `mapstructure:"addr"`
	ShutdownTimeoutSeconds int    `mapstructure:"shutdown_timeout_seconds"`
}

type MySQLConfig struct {
	DSN          string `mapstructure:"dsn"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type CacheConfig struct {
	LocalSizeMB       int `mapstructure:"local_size_mb"`
	LocalDetailTTL    int `mapstructure:"local_detail_ttl"`
	LocalFirstPageTTL int `mapstructure:"local_first_page_ttl"`
	RedisHotTTL       int `mapstructure:"redis_hot_ttl"`
	RedisColdTTL      int `mapstructure:"redis_cold_ttl"`
	RedisNullTTL      int `mapstructure:"redis_null_ttl"`
}

type FeedConfig struct {
	LatestHotSize int `mapstructure:"latest_hot_size"`
	PageDefault   int `mapstructure:"page_default"`
	PageMax       int `mapstructure:"page_max"`
}

type AuthConfig struct {
	JWTSecret       string `mapstructure:"jwt_secret"`
	Issuer          string `mapstructure:"issuer"`
	AccessTokenTTL  int    `mapstructure:"access_token_ttl"`
	RefreshTokenTTL int    `mapstructure:"refresh_token_ttl"`
	BcryptCost      int    `mapstructure:"bcrypt_cost"`
}

// Validate 校验 API 鉴权配置；Worker 进程无需调用。
func (a AuthConfig) Validate() error {
	if len(a.JWTSecret) < 32 {
		return fmt.Errorf("auth.jwt_secret must be at least 32 bytes (set via WF_AUTH_JWT_SECRET)")
	}
	if strings.TrimSpace(a.Issuer) == "" {
		return fmt.Errorf("auth.issuer must not be empty")
	}
	if a.AccessTokenTTL <= 0 || a.RefreshTokenTTL <= 0 {
		return fmt.Errorf("auth token TTLs must be positive")
	}
	if a.AccessTokenTTL >= a.RefreshTokenTTL {
		return fmt.Errorf("auth.access_token_ttl (%d) must be less than auth.refresh_token_ttl (%d)", a.AccessTokenTTL, a.RefreshTokenTTL)
	}
	if a.BcryptCost < bcrypt.MinCost || a.BcryptCost > bcrypt.MaxCost {
		return fmt.Errorf("auth.bcrypt_cost (%d) out of range [%d, %d]", a.BcryptCost, bcrypt.MinCost, bcrypt.MaxCost)
	}
	return nil
}

type UserConfig struct {
	DefaultAvatarKey string `mapstructure:"default_avatar_key"`
	ProfileLocalTTL  int    `mapstructure:"profile_local_ttl"`
	ProfileRedisTTL  int    `mapstructure:"profile_redis_ttl"`
}

type StorageConfig struct {
	Provider           string `mapstructure:"provider"`
	Bucket             string `mapstructure:"bucket"`
	AccessKey          string `mapstructure:"access_key"`
	SecretKey          string `mapstructure:"secret_key"`
	CDNDomain          string `mapstructure:"cdn_domain"`
	UploadURL          string `mapstructure:"upload_url"`
	TokenExpireSeconds int    `mapstructure:"token_expire_seconds"`
	MaxFileSizeMB      int    `mapstructure:"max_file_size_mb"`
}

type WorkerConfig struct {
	ReconcileIntervalSeconds int `mapstructure:"reconcile_interval_seconds"`
}

// Load 读取配置文件；环境变量 WF_ 前缀可覆盖任意项，例如 WF_STORAGE_ACCESS_KEY。
func Load(path string) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	v.SetConfigFile(path)
	v.SetEnvPrefix("WF")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	_ = v.BindEnv("auth.jwt_secret", "WF_AUTH_JWT_SECRET")
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	// JWT 密钥仅允许通过 WF_AUTH_JWT_SECRET 环境变量注入，忽略配置文件中的静态值。
	cfg.Auth.JWTSecret = os.Getenv("WF_AUTH_JWT_SECRET")
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.env", "dev")
	v.SetDefault("http.addr", ":8080")
	v.SetDefault("http.shutdown_timeout_seconds", 10)
	v.SetDefault("mysql.max_open_conns", 20)
	v.SetDefault("mysql.max_idle_conns", 10)
	v.SetDefault("redis.addr", "127.0.0.1:6379")
	v.SetDefault("cache.local_size_mb", 64)
	v.SetDefault("cache.local_detail_ttl", 30)
	v.SetDefault("cache.local_first_page_ttl", 2)
	v.SetDefault("cache.redis_hot_ttl", 3600)
	v.SetDefault("cache.redis_cold_ttl", 600)
	v.SetDefault("cache.redis_null_ttl", 60)
	v.SetDefault("feed.latest_hot_size", 1000)
	v.SetDefault("feed.page_default", 20)
	v.SetDefault("feed.page_max", 50)
	v.SetDefault("auth.issuer", "wallpaper-feed")
	v.SetDefault("auth.access_token_ttl", 900)
	v.SetDefault("auth.refresh_token_ttl", 604800)
	v.SetDefault("auth.bcrypt_cost", 10)
	v.SetDefault("user.default_avatar_key", "avatars/default.png")
	v.SetDefault("user.profile_local_ttl", 30)
	v.SetDefault("user.profile_redis_ttl", 1800)
	v.SetDefault("storage.provider", "qiniu")
	v.SetDefault("storage.token_expire_seconds", 3600)
	v.SetDefault("storage.max_file_size_mb", 20)
	v.SetDefault("storage.upload_url", "https://up-z0.qiniup.com")
	v.SetDefault("worker.reconcile_interval_seconds", 60)
}
