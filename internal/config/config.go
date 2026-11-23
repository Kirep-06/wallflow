package config

import (
	"cmp"
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

const (
	DefaultConfigPath    = "configs/config.local.yaml"
	EnvConfigFile        = "WF_CONFIG_FILE"
	EnvJWTSecret         = "WF_AUTH_JWT_SECRET"
	EnvQiniuAccessKey    = "WF_STORAGE_QINIU_ACCESS_KEY"
	EnvQiniuSecretKey    = "WF_STORAGE_QINIU_SECRET_KEY"
	EnvRabbitMQPassword  = "WF_RABBITMQ_PASSWORD"
	MinJWTSecretLen      = 32
	StorageProviderNoop  = "noop"
	StorageProviderQiniu = "qiniu"
	defaultRabbitMQPort  = 5672
	defaultRabbitMQVhost = "/"
)

var qiniuZoneUploadURLs = map[string]string{
	"z0":        "https://upload.qiniup.com",
	"z1":        "https://upload-z1.qiniup.com",
	"z2":        "https://upload-z2.qiniup.com",
	"na0":       "https://upload-na0.qiniup.com",
	"as0":       "https://upload-as0.qiniup.com",
	"cn-east-2": "https://upload-cn-east-2.qiniup.com",
}

// Config 为应用全局配置结构。
type Config struct {
	Server    ServerConfig   `mapstructure:"server"`
	MySQL     MySQLConfig    `mapstructure:"mysql"`
	Redis     RedisConfig    `mapstructure:"redis"`
	RabbitMQ  RabbitMQConfig `mapstructure:"rabbitmq"`
	Storage   StorageConfig  `mapstructure:"storage"`
	JWTSecret string         `mapstructure:"-"`
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

// RabbitMQConfig 为 RabbitMQ 连接配置（支持 enabled 开关与 WF_RABBITMQ_PASSWORD 环境变量覆盖）。
type RabbitMQConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Vhost    string `mapstructure:"vhost"`
}

// StorageConfig 为对象存储与 CDN 域名配置。
type StorageConfig struct {
	Provider  string      `mapstructure:"provider"`
	CDNDomain string      `mapstructure:"cdn_domain"`
	Qiniu     QiniuConfig `mapstructure:"qiniu"`
}

// QiniuConfig 为七牛云对象存储配置（AK/SK 仅从环境变量注入）。
type QiniuConfig struct {
	Bucket    string `mapstructure:"bucket"`
	Zone      string `mapstructure:"zone"`
	UploadURL string `mapstructure:"upload_url"`
	AccessKey string `mapstructure:"-"`
	SecretKey string `mapstructure:"-"`
}

// Load 加载 YAML 配置文件（path -> WF_CONFIG_FILE -> configs/config.local.yaml）并注入环境变量密钥。
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

	if err := normalizeAndValidateStorage(&cfg.Storage); err != nil {
		return nil, err
	}
	if err := normalizeAndValidateRabbitMQ(&cfg.RabbitMQ); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func normalizeAndValidateRabbitMQ(r *RabbitMQConfig) error {
	r.Host = strings.TrimSpace(r.Host)
	r.User = strings.TrimSpace(r.User)
	r.Vhost = strings.TrimSpace(r.Vhost)
	if envPass, ok := os.LookupEnv(EnvRabbitMQPassword); ok && envPass != "" {
		r.Password = envPass
	}

	if !r.Enabled {
		return nil
	}
	if r.Host == "" {
		return fmt.Errorf("config: rabbitmq.host is required when rabbitmq.enabled=true")
	}
	if r.User == "" {
		return fmt.Errorf("config: rabbitmq.user is required when rabbitmq.enabled=true")
	}
	if r.Port == 0 {
		r.Port = defaultRabbitMQPort
	}
	if r.Port < 0 || r.Port > 65535 {
		return fmt.Errorf("config: invalid rabbitmq.port %d", r.Port)
	}
	if r.Vhost == "" {
		r.Vhost = defaultRabbitMQVhost
	}
	return nil
}

func normalizeAndValidateStorage(s *StorageConfig) error {
	s.Provider = strings.ToLower(strings.TrimSpace(s.Provider))
	if s.Provider == "" {
		s.Provider = StorageProviderNoop
	}
	s.CDNDomain = strings.TrimRight(strings.TrimSpace(s.CDNDomain), "/")
	s.Qiniu.Bucket = strings.TrimSpace(s.Qiniu.Bucket)
	s.Qiniu.Zone = strings.ToLower(strings.TrimSpace(s.Qiniu.Zone))
	s.Qiniu.UploadURL = strings.TrimRight(strings.TrimSpace(s.Qiniu.UploadURL), "/")
	// 七牛 AK/SK 仅从环境变量读取，严禁写入配置文件。
	s.Qiniu.AccessKey = strings.TrimSpace(os.Getenv(EnvQiniuAccessKey))
	s.Qiniu.SecretKey = strings.TrimSpace(os.Getenv(EnvQiniuSecretKey))

	switch s.Provider {
	case StorageProviderNoop:
		return nil
	case StorageProviderQiniu:
		if s.Qiniu.AccessKey == "" {
			return fmt.Errorf("config: env %s is required when storage.provider=qiniu", EnvQiniuAccessKey)
		}
		if s.Qiniu.SecretKey == "" {
			return fmt.Errorf("config: env %s is required when storage.provider=qiniu", EnvQiniuSecretKey)
		}
		if s.Qiniu.Bucket == "" {
			return fmt.Errorf("config: storage.qiniu.bucket is required when storage.provider=qiniu")
		}
		if s.CDNDomain == "" {
			return fmt.Errorf("config: storage.cdn_domain is required when storage.provider=qiniu")
		}
		if _, err := s.Qiniu.ResolveUploadURL(); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("config: unsupported storage.provider %q", s.Provider)
	}
}

// ResolveUploadURL 根据显式 upload_url 或 zone 映射解析七牛 HTTPS 上传域名。
func (q QiniuConfig) ResolveUploadURL() (string, error) {
	if u := strings.TrimRight(strings.TrimSpace(q.UploadURL), "/"); u != "" {
		return u, nil
	}
	zone := strings.ToLower(strings.TrimSpace(q.Zone))
	if u, ok := qiniuZoneUploadURLs[zone]; ok {
		return u, nil
	}
	return "", fmt.Errorf("config: unsupported storage.qiniu.zone %q", q.Zone)
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

// URI 使用 amqp.URI 规范编码拼装 RabbitMQ 连接串（严禁明文字符串拼接密码）。
func (r RabbitMQConfig) URI() string {
	port := r.Port
	if port <= 0 {
		port = defaultRabbitMQPort
	}
	vhost := r.Vhost
	if vhost == "" {
		vhost = defaultRabbitMQVhost
	}
	return (amqp.URI{
		Scheme:   "amqp",
		Host:     r.Host,
		Port:     port,
		Username: r.User,
		Password: r.Password,
		Vhost:    vhost,
	}).String()
}

// SafeAddr 返回不含用户名与密码的脱敏 RabbitMQ 地址，专供日志输出。
func (r RabbitMQConfig) SafeAddr() string {
	port := r.Port
	if port <= 0 {
		port = defaultRabbitMQPort
	}
	vhost := strings.TrimPrefix(r.Vhost, "/")
	return fmt.Sprintf("%s:%d/%s", r.Host, port, vhost)
}
