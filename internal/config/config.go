package config

import (
	"cmp"
	"fmt"
	"os"
	"strings"

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
	Server    ServerConfig  `mapstructure:"server"`
	MySQL     MySQLConfig   `mapstructure:"mysql"`
	Redis     RedisConfig   `mapstructure:"redis"`
	Storage   StorageConfig `mapstructure:"storage"`
	JWTSecret string        `mapstructure:"-"`
}

// ServerConfig 为 HTTP 服务运行配置。
type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Env  string `mapstructure:"env"`
}

// MySQLConfig 为 MySQL 连接与连接池配置。
type MySQLConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	DBName       string `mapstructure:"dbname"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
}

// RedisConfig 为 Redis 连接配置。
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// StorageConfig 为对象存储与 CDN 域名配置。
type StorageConfig struct {
	CDNDomain string `mapstructure:"cdn_domain"`
}

// Load 加载 YAML 配置文件（path -> WF_CONFIG_FILE -> configs/config.local.yaml）并注入 JWT 密钥。
func Load(path string) (*Config, error) {
	_ = godotenv.Load()

	v := viper.New()
	v.SetConfigFile(cmp.Or(path, os.Getenv(EnvConfigFile), DefaultConfigPath))
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// JWT 签名密钥仅从环境变量读取，严禁写入配置文件。
	cfg.JWTSecret = os.Getenv(EnvJWTSecret)
	if len(cfg.JWTSecret) < MinJWTSecretLen {
		return nil, fmt.Errorf("config: env %s must be at least %d bytes", EnvJWTSecret, MinJWTSecretLen)
	}
	return &cfg, nil
}

// IsDev 判断当前是否为开发运行环境。
func (c Config) IsDev() bool {
	return c.Server.Env == "" || strings.EqualFold(c.Server.Env, "dev") || strings.EqualFold(c.Server.Env, "development")
}

// Addr 组装 Redis 连接地址。
func (r RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// Addr 组装 HTTP 服务监听地址。
func (s ServerConfig) Addr() string {
	return fmt.Sprintf(":%d", s.Port)
}

// DSN 组装 MySQL 连接串（统一 utf8mb4、parseTime 与 UTC 时区）。
func (m MySQLConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=UTC",
		m.User, m.Password, m.Host, m.Port, m.DBName)
}
