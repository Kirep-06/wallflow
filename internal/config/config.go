// Package config 负责加载 YAML 配置并支持环境变量覆盖。
package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/viper"
	"golang.org/x/crypto/bcrypt"
)

type Config struct {
	App           AppConfig           `mapstructure:"app"`
	HTTP          HTTPConfig          `mapstructure:"http"`
	MySQL         MySQLConfig         `mapstructure:"mysql"`
	Redis         RedisConfig         `mapstructure:"redis"`
	RabbitMQ      RabbitMQConfig      `mapstructure:"rabbitmq"`
	Cache         CacheConfig         `mapstructure:"cache"`
	Feed          FeedConfig          `mapstructure:"feed"`
	FollowingFeed FollowingFeedConfig `mapstructure:"following_feed"`
	Outbox        OutboxConfig        `mapstructure:"outbox"`
	Auth          AuthConfig          `mapstructure:"auth"`
	User          UserConfig          `mapstructure:"user"`
	Follow        FollowConfig        `mapstructure:"follow"`
	Storage       StorageConfig       `mapstructure:"storage"`
	Worker        WorkerConfig        `mapstructure:"worker"`
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

type FollowConfig struct {
	MaxFollowing  int `mapstructure:"max_following"`
	StatsLocalTTL int `mapstructure:"stats_local_ttl"`
	StatsRedisTTL int `mapstructure:"stats_redis_ttl"`
}

// Validate 校验关注模块配置项均为正整数。
func (f FollowConfig) Validate() error {
	if f.MaxFollowing <= 0 {
		return fmt.Errorf("follow.max_following (%d) must be positive", f.MaxFollowing)
	}
	if f.StatsLocalTTL <= 0 {
		return fmt.Errorf("follow.stats_local_ttl (%d) must be positive", f.StatsLocalTTL)
	}
	if f.StatsRedisTTL <= 0 {
		return fmt.Errorf("follow.stats_redis_ttl (%d) must be positive", f.StatsRedisTTL)
	}
	return nil
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

type RabbitMQConfig struct {
	URL               string `mapstructure:"url"`
	Exchange          string `mapstructure:"exchange"`
	DLXExchange       string `mapstructure:"dlx_exchange"`
	FanoutQueue       string `mapstructure:"fanout_queue"`
	RetryQueue        string `mapstructure:"retry_queue"`
	DLQQueue          string `mapstructure:"dlq_queue"`
	RetryDelayMs      int    `mapstructure:"retry_delay_ms"`
	MaxConsumeRetries int    `mapstructure:"max_consume_retries"`
}

// Validate 校验 RabbitMQ 连接串格式、拓扑命名与重试参数。
func (r RabbitMQConfig) Validate() error {
	raw := strings.TrimSpace(r.URL)
	if raw == "" {
		return fmt.Errorf("rabbitmq.url must not be empty (set via WF_RABBITMQ_URL)")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "amqp" && u.Scheme != "amqps") || strings.TrimSpace(u.Host) == "" {
		return fmt.Errorf("rabbitmq.url must be a valid amqp:// or amqps:// URL")
	}
	if strings.TrimSpace(r.Exchange) == "" {
		return fmt.Errorf("rabbitmq.exchange must not be empty")
	}
	if strings.TrimSpace(r.DLXExchange) == "" {
		return fmt.Errorf("rabbitmq.dlx_exchange must not be empty")
	}
	if strings.TrimSpace(r.FanoutQueue) == "" {
		return fmt.Errorf("rabbitmq.fanout_queue must not be empty")
	}
	if strings.TrimSpace(r.RetryQueue) == "" {
		return fmt.Errorf("rabbitmq.retry_queue must not be empty")
	}
	if strings.TrimSpace(r.DLQQueue) == "" {
		return fmt.Errorf("rabbitmq.dlq_queue must not be empty")
	}
	if r.RetryDelayMs <= 0 {
		return fmt.Errorf("rabbitmq.retry_delay_ms (%d) must be positive", r.RetryDelayMs)
	}
	if r.MaxConsumeRetries <= 0 {
		return fmt.Errorf("rabbitmq.max_consume_retries (%d) must be positive", r.MaxConsumeRetries)
	}
	return nil
}

// SanitizedURL 返回剥离用户名与密码凭据的 AMQP 地址，仅用于日志输出。
func (r RabbitMQConfig) SanitizedURL() string {
	u, err := url.Parse(strings.TrimSpace(r.URL))
	if err != nil || u.Host == "" {
		return "<invalid-amqp-url>"
	}
	path := u.Path
	if path == "" {
		path = "/"
	}
	return u.Host + path
}

type FollowingFeedConfig struct {
	BigVThreshold         int `mapstructure:"bigv_threshold"`
	InboxHotSize          int `mapstructure:"inbox_hot_size"`
	OutboxHotSize         int `mapstructure:"outbox_hot_size"`
	InboxTTLSeconds       int `mapstructure:"inbox_ttl_seconds"`
	OutboxTTLSeconds      int `mapstructure:"outbox_ttl_seconds"`
	FollowingIDsLocalTTL  int `mapstructure:"following_ids_local_ttl"`
	FollowingIDsRedisTTL  int `mapstructure:"following_ids_redis_ttl"`
	BigVLocalTTL          int `mapstructure:"bigv_local_ttl"`
	MaxSyncOutboxRebuilds int `mapstructure:"max_sync_outbox_rebuilds"`
	AuthorInChunkSize     int `mapstructure:"author_in_chunk_size"`
}

// Validate 校验关注流配置项均为正整数。
func (f FollowingFeedConfig) Validate() error {
	checks := []struct {
		name string
		val  int
	}{
		{"following_feed.bigv_threshold", f.BigVThreshold},
		{"following_feed.inbox_hot_size", f.InboxHotSize},
		{"following_feed.outbox_hot_size", f.OutboxHotSize},
		{"following_feed.inbox_ttl_seconds", f.InboxTTLSeconds},
		{"following_feed.outbox_ttl_seconds", f.OutboxTTLSeconds},
		{"following_feed.following_ids_local_ttl", f.FollowingIDsLocalTTL},
		{"following_feed.following_ids_redis_ttl", f.FollowingIDsRedisTTL},
		{"following_feed.bigv_local_ttl", f.BigVLocalTTL},
		{"following_feed.max_sync_outbox_rebuilds", f.MaxSyncOutboxRebuilds},
		{"following_feed.author_in_chunk_size", f.AuthorInChunkSize},
	}
	for _, c := range checks {
		if c.val <= 0 {
			return fmt.Errorf("%s (%d) must be positive", c.name, c.val)
		}
	}
	return nil
}

type OutboxConfig struct {
	PollIntervalMs   int `mapstructure:"poll_interval_ms"`
	BatchSize        int `mapstructure:"batch_size"`
	LeaseSeconds     int `mapstructure:"lease_seconds"`
	PublishTimeoutMs int `mapstructure:"publish_timeout_ms"`
	MaxRetries       int `mapstructure:"max_retries"`
	RetentionDays    int `mapstructure:"retention_days"`
}

// Validate 校验 Outbox 投递与清理配置项均为正整数。
func (o OutboxConfig) Validate() error {
	checks := []struct {
		name string
		val  int
	}{
		{"outbox.poll_interval_ms", o.PollIntervalMs},
		{"outbox.batch_size", o.BatchSize},
		{"outbox.lease_seconds", o.LeaseSeconds},
		{"outbox.publish_timeout_ms", o.PublishTimeoutMs},
		{"outbox.max_retries", o.MaxRetries},
		{"outbox.retention_days", o.RetentionDays},
	}
	for _, c := range checks {
		if c.val <= 0 {
			return fmt.Errorf("%s (%d) must be positive", c.name, c.val)
		}
	}
	return nil
}

type WorkerConfig struct {
	ReconcileIntervalSeconds     int `mapstructure:"reconcile_interval_seconds"`
	OutboxCleanupIntervalSeconds int `mapstructure:"outbox_cleanup_interval_seconds"`
	FanoutPrefetch               int `mapstructure:"fanout_prefetch"`
	FanoutConcurrency            int `mapstructure:"fanout_concurrency"`
	FanoutBatchSize              int `mapstructure:"fanout_batch_size"`
}

// Validate 校验 Worker 配置项均为正整数。
func (w WorkerConfig) Validate() error {
	checks := []struct {
		name string
		val  int
	}{
		{"worker.reconcile_interval_seconds", w.ReconcileIntervalSeconds},
		{"worker.outbox_cleanup_interval_seconds", w.OutboxCleanupIntervalSeconds},
		{"worker.fanout_prefetch", w.FanoutPrefetch},
		{"worker.fanout_concurrency", w.FanoutConcurrency},
		{"worker.fanout_batch_size", w.FanoutBatchSize},
	}
	for _, c := range checks {
		if c.val <= 0 {
			return fmt.Errorf("%s (%d) must be positive", c.name, c.val)
		}
	}
	return nil
}

// Load 读取配置文件；环境变量 WF_ 前缀可覆盖任意项，例如 WF_STORAGE_ACCESS_KEY、WF_RABBITMQ_URL。
func Load(path string) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	v.SetConfigFile(path)
	v.SetEnvPrefix("WF")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	_ = v.BindEnv("auth.jwt_secret", "WF_AUTH_JWT_SECRET")
	_ = v.BindEnv("rabbitmq.url", "WF_RABBITMQ_URL")
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	// JWT 密钥仅允许通过 WF_AUTH_JWT_SECRET 环境变量注入，忽略配置文件中的静态值。
	cfg.Auth.JWTSecret = os.Getenv("WF_AUTH_JWT_SECRET")
	if err := cfg.Follow.Validate(); err != nil {
		return nil, fmt.Errorf("invalid follow config: %w", err)
	}
	if err := cfg.FollowingFeed.Validate(); err != nil {
		return nil, fmt.Errorf("invalid following_feed config: %w", err)
	}
	if err := cfg.Outbox.Validate(); err != nil {
		return nil, fmt.Errorf("invalid outbox config: %w", err)
	}
	if err := cfg.Worker.Validate(); err != nil {
		return nil, fmt.Errorf("invalid worker config: %w", err)
	}
	if err := cfg.RabbitMQ.Validate(); err != nil {
		return nil, fmt.Errorf("invalid rabbitmq config: %w", err)
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.env", "dev")
	v.SetDefault("http.addr", ":8080")
	v.SetDefault("http.shutdown_timeout_seconds", 10)
	v.SetDefault("mysql.max_open_conns", 20)
	v.SetDefault("mysql.max_idle_conns", 10)
	v.SetDefault("redis.addr", "127.0.0.1:6379")
	v.SetDefault("rabbitmq.url", "")
	v.SetDefault("rabbitmq.exchange", "wallpaper.events")
	v.SetDefault("rabbitmq.dlx_exchange", "wallpaper.dlx")
	v.SetDefault("rabbitmq.fanout_queue", "feed.following.fanout")
	v.SetDefault("rabbitmq.retry_queue", "feed.following.fanout.retry")
	v.SetDefault("rabbitmq.dlq_queue", "feed.following.fanout.dlq")
	v.SetDefault("rabbitmq.retry_delay_ms", 5000)
	v.SetDefault("rabbitmq.max_consume_retries", 5)
	v.SetDefault("cache.local_size_mb", 64)
	v.SetDefault("cache.local_detail_ttl", 30)
	v.SetDefault("cache.local_first_page_ttl", 2)
	v.SetDefault("cache.redis_hot_ttl", 3600)
	v.SetDefault("cache.redis_cold_ttl", 600)
	v.SetDefault("cache.redis_null_ttl", 60)
	v.SetDefault("feed.latest_hot_size", 1000)
	v.SetDefault("feed.page_default", 20)
	v.SetDefault("feed.page_max", 50)
	v.SetDefault("following_feed.bigv_threshold", 5000)
	v.SetDefault("following_feed.inbox_hot_size", 500)
	v.SetDefault("following_feed.outbox_hot_size", 200)
	v.SetDefault("following_feed.inbox_ttl_seconds", 604800)
	v.SetDefault("following_feed.outbox_ttl_seconds", 604800)
	v.SetDefault("following_feed.following_ids_local_ttl", 15)
	v.SetDefault("following_feed.following_ids_redis_ttl", 600)
	v.SetDefault("following_feed.bigv_local_ttl", 30)
	v.SetDefault("following_feed.max_sync_outbox_rebuilds", 20)
	v.SetDefault("following_feed.author_in_chunk_size", 200)
	v.SetDefault("outbox.poll_interval_ms", 2000)
	v.SetDefault("outbox.batch_size", 50)
	v.SetDefault("outbox.lease_seconds", 30)
	v.SetDefault("outbox.publish_timeout_ms", 5000)
	v.SetDefault("outbox.max_retries", 10)
	v.SetDefault("outbox.retention_days", 7)
	v.SetDefault("auth.issuer", "wallpaper-feed")
	v.SetDefault("auth.access_token_ttl", 900)
	v.SetDefault("auth.refresh_token_ttl", 604800)
	v.SetDefault("auth.bcrypt_cost", 10)
	v.SetDefault("user.default_avatar_key", "avatars/default.png")
	v.SetDefault("user.profile_local_ttl", 30)
	v.SetDefault("user.profile_redis_ttl", 1800)
	v.SetDefault("follow.max_following", 2000)
	v.SetDefault("follow.stats_local_ttl", 15)
	v.SetDefault("follow.stats_redis_ttl", 600)
	v.SetDefault("storage.provider", "qiniu")
	v.SetDefault("storage.token_expire_seconds", 3600)
	v.SetDefault("storage.max_file_size_mb", 20)
	v.SetDefault("storage.upload_url", "https://up-z0.qiniup.com")
	v.SetDefault("worker.reconcile_interval_seconds", 60)
	v.SetDefault("worker.outbox_cleanup_interval_seconds", 3600)
	v.SetDefault("worker.fanout_prefetch", 8)
	v.SetDefault("worker.fanout_concurrency", 4)
	v.SetDefault("worker.fanout_batch_size", 500)
}
