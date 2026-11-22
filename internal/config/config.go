package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

const (
	DefaultConfigPath = "configs/config.local.yaml"
	EnvConfigFile     = "WF_CONFIG_FILE"
	EnvJWTSecret      = "WF_AUTH_JWT_SECRET"
	MinJWTSecretLen   = 32
)

// Config 为应用全局配置结构。
type Config struct {
	App     AppConfig     `mapstructure:"app"`
	Server  ServerConfig  `mapstructure:"server"`
	MySQL   MySQLConfig   `mapstructure:"mysql"`
	Redis   RedisConfig   `mapstructure:"redis"`
	Auth    AuthConfig    `mapstructure:"auth"`
	Cache   CacheConfig   `mapstructure:"cache"`
	Storage StorageConfig `mapstructure:"storage"`
	User    UserConfig    `mapstructure:"user"`
}

type AppConfig struct {
	Name     string `mapstructure:"name"`
	Env      string `mapstructure:"env"`
	LogLevel string `mapstructure:"log_level"`
}

type ServerConfig struct {
	Addr             string        `mapstructure:"addr"`
	ReadTimeout      time.Duration `mapstructure:"read_timeout"`
	WriteTimeout     time.Duration `mapstructure:"write_timeout"`
	IdleTimeout      time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout  time.Duration `mapstructure:"shutdown_timeout"`
	CORSAllowOrigins []string      `mapstructure:"cors_allow_origins"`
}

type MySQLConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type RedisConfig struct {
	Addr         string        `mapstructure:"addr"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type AuthConfig struct {
	// JWTSecret 仅从环境变量 WF_AUTH_JWT_SECRET 读取，严禁从配置文件加载。
	JWTSecret  string        `mapstructure:"-"`
	Issuer     string        `mapstructure:"issuer"`
	AccessTTL  time.Duration `mapstructure:"access_ttl"`
	RefreshTTL time.Duration `mapstructure:"refresh_ttl"`
	BcryptCost int           `mapstructure:"bcrypt_cost"`
}

type CacheConfig struct {
	L1CleanupInterval  time.Duration `mapstructure:"l1_cleanup_interval"`
	UserProfileL1TTL   time.Duration `mapstructure:"user_profile_l1_ttl"`
	UserProfileL2TTL   time.Duration `mapstructure:"user_profile_l2_ttl"`
	UserProfileNullTTL time.Duration `mapstructure:"user_profile_null_ttl"`
	JitterRatio        float64       `mapstructure:"jitter_ratio"`
}

type StorageConfig struct {
	CDNDomain string `mapstructure:"cdn_domain"`
}

type UserConfig struct {
	DefaultAvatarKey string `mapstructure:"default_avatar_key"`
}

// Load 加载配置文件与 WF_ 前缀环境变量，并执行必要校验。
// path 为空时依次尝试 WF_CONFIG_FILE 与默认路径 configs/config.local.yaml。
func Load(path string) (*Config, error) {
	// 本地开发存在 .env 时按需加载（不覆盖已有系统环境变量）。
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("load .env: %w", err)
	}

	v := viper.New()
	setDefaults(v)

	v.SetEnvPrefix("WF")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	explicit := true
	cfgPath := strings.TrimSpace(path)
	if cfgPath == "" {
		if envPath := strings.TrimSpace(os.Getenv(EnvConfigFile)); envPath != "" {
			cfgPath = envPath
		} else {
			cfgPath = DefaultConfigPath
			explicit = false
		}
	}

	v.SetConfigFile(cfgPath)
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if explicit || (!errors.As(err, &notFound) && !errors.Is(err, os.ErrNotExist)) {
			return nil, fmt.Errorf("read config %q: %w", cfgPath, err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// 强制仅从环境变量读取 JWT 签名密钥，忽略配置文件中的任何静态值。
	cfg.Auth.JWTSecret = os.Getenv(EnvJWTSecret)

	// 支持逗号分隔的 WF_SERVER_CORS_ALLOW_ORIGINS 覆盖。
	if rawOrigins := strings.TrimSpace(os.Getenv("WF_SERVER_CORS_ALLOW_ORIGINS")); rawOrigins != "" {
		parts := strings.Split(rawOrigins, ",")
		origins := make([]string, 0, len(parts))
		for _, p := range parts {
			if s := strings.TrimSpace(p); s != "" {
				origins = append(origins, s)
			}
		}
		cfg.Server.CORSAllowOrigins = origins
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "wallpaper-feed")
	v.SetDefault("app.env", "dev")
	v.SetDefault("app.log_level", "info")

	v.SetDefault("server.addr", ":8080")
	v.SetDefault("server.read_timeout", 10*time.Second)
	v.SetDefault("server.write_timeout", 15*time.Second)
	v.SetDefault("server.idle_timeout", 60*time.Second)
	v.SetDefault("server.shutdown_timeout", 10*time.Second)
	v.SetDefault("server.cors_allow_origins", []string{
		"http://localhost:5173",
		"http://127.0.0.1:5173",
	})

	v.SetDefault("mysql.dsn", "")
	v.SetDefault("mysql.max_open_conns", 50)
	v.SetDefault("mysql.max_idle_conns", 10)
	v.SetDefault("mysql.conn_max_lifetime", 30*time.Minute)

	v.SetDefault("redis.addr", "127.0.0.1:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.dial_timeout", 3*time.Second)
	v.SetDefault("redis.read_timeout", 2*time.Second)
	v.SetDefault("redis.write_timeout", 2*time.Second)

	v.SetDefault("auth.issuer", "wallpaper-feed")
	v.SetDefault("auth.access_ttl", 7200*time.Second)
	v.SetDefault("auth.refresh_ttl", 604800*time.Second)
	v.SetDefault("auth.bcrypt_cost", 10)

	v.SetDefault("cache.l1_cleanup_interval", time.Minute)
	v.SetDefault("cache.user_profile_l1_ttl", 30*time.Second)
	v.SetDefault("cache.user_profile_l2_ttl", 1800*time.Second)
	v.SetDefault("cache.user_profile_null_ttl", 60*time.Second)
	v.SetDefault("cache.jitter_ratio", 0.1)

	v.SetDefault("storage.cdn_domain", "")
	v.SetDefault("user.default_avatar_key", "avatars/default.png")
}

// Validate 校验启动必需配置项的合法性。
func (c *Config) Validate() error {
	if len(c.Auth.JWTSecret) < MinJWTSecretLen {
		return fmt.Errorf("config: env %s must be at least %d bytes", EnvJWTSecret, MinJWTSecretLen)
	}
	if strings.TrimSpace(c.Server.Addr) == "" {
		return errors.New("config: server.addr is required")
	}
	if strings.TrimSpace(c.MySQL.DSN) == "" {
		return errors.New("config: mysql.dsn is required")
	}
	if strings.TrimSpace(c.Redis.Addr) == "" {
		return errors.New("config: redis.addr is required")
	}
	if strings.TrimSpace(c.Auth.Issuer) == "" {
		return errors.New("config: auth.issuer is required")
	}
	if c.Auth.AccessTTL <= 0 {
		return errors.New("config: auth.access_ttl must be positive")
	}
	if c.Auth.RefreshTTL <= 0 {
		return errors.New("config: auth.refresh_ttl must be positive")
	}
	if c.Auth.BcryptCost < 4 || c.Auth.BcryptCost > 31 {
		return errors.New("config: auth.bcrypt_cost must be between 4 and 31")
	}
	if c.Cache.JitterRatio < 0 || c.Cache.JitterRatio > 0.5 {
		return errors.New("config: cache.jitter_ratio must be between 0 and 0.5")
	}
	return nil
}

// IsDev 判断当前是否为开发运行环境。
func (c *Config) IsDev() bool {
	return strings.EqualFold(c.App.Env, "dev") || strings.EqualFold(c.App.Env, "development")
}
