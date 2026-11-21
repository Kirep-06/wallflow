// Package app 负责初始化基础设施并装配各层依赖。
package app

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/consumer"
	"wallpaper-feed/internal/handler"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
)

type App struct {
	Cfg        *config.Config
	DB         *gorm.DB
	Redis      *redis.Client
	AuthRedis  *redis.Client // 关闭自动重试，专供会话轮换等非幂等 Lua 命令使用
	Rebuilder  *service.LatestRebuilder
	Router     *gin.Engine
	Reconciler *consumer.LatestReconciler
}

// NewAPI 初始化 API 进程所需的全部依赖（含 L1 gocache、认证与 HTTP 路由）。
func NewAPI(ctx context.Context, cfg *config.Config) (*App, error) {
	if err := cfg.Auth.Validate(); err != nil {
		return nil, fmt.Errorf("invalid auth config: %w", err)
	}
	if err := cfg.Follow.Validate(); err != nil {
		return nil, fmt.Errorf("invalid follow config: %w", err)
	}
	a, postRepo, feedRepo, err := newBase(ctx, cfg)
	if err != nil {
		return nil, err
	}
	authRedis, err := openRedisNoRetry(ctx, cfg.Redis)
	if err != nil {
		_ = a.Close()
		return nil, err
	}
	a.AuthRedis = authRedis

	local := gocache.NewCache(cfg.Cache.LocalSizeMB << 20)
	detail := cache.NewMultiLevel(local, a.Redis, cache.Options{
		Prefix:   "post:detail:",
		LocalTTL: time.Duration(cfg.Cache.LocalDetailTTL) * time.Second,
		NullTTL:  time.Duration(cfg.Cache.RedisNullTTL) * time.Second,
	})
	profileCache := cache.NewMultiLevel(local, a.Redis, cache.Options{
		Prefix:   "user:profile:",
		LocalTTL: time.Duration(cfg.User.ProfileLocalTTL) * time.Second,
		NullTTL:  time.Duration(cfg.Cache.RedisNullTTL) * time.Second,
	})
	followStatsCache := cache.NewMultiLevel(local, a.Redis, cache.Options{
		Prefix:   "follow:stats:",
		LocalTTL: time.Duration(cfg.Follow.StatsLocalTTL) * time.Second,
		NullTTL:  time.Duration(cfg.Cache.RedisNullTTL) * time.Second,
	})
	st, err := newStorage(cfg.Storage)
	if err != nil {
		_ = a.Close()
		return nil, err
	}
	userRepo := repository.NewUserRepo(a.DB)
	followRepo := repository.NewFollowRepo(a.DB)
	sessionRepo := repository.NewAuthSessionRepo(a.AuthRedis)

	postSvc := service.NewPostService(postRepo, feedRepo, st, detail, cfg.Cache)
	feedSvc := service.NewFeedService(postRepo, feedRepo, postSvc, a.Rebuilder, local, cfg.Feed, cfg.Cache.LocalFirstPageTTL)
	userSvc := service.NewUserService(userRepo, postRepo, postSvc, st, profileCache, cfg.User)
	followSvc := service.NewFollowService(followRepo, userSvc, followStatsCache, cfg.Follow)
	userSvc.SetFollowReader(followSvc)
	authSvc, err := service.NewAuthService(userRepo, sessionRepo, st, userSvc, cfg.Auth, cfg.User)
	if err != nil {
		_ = a.Close()
		return nil, err
	}
	a.Router = handler.NewRouter(
		cfg.App.IsDev(),
		authSvc,
		handler.NewAuthHandler(authSvc),
		handler.NewUserHandler(userSvc),
		handler.NewFollowHandler(followSvc),
		handler.NewPostHandler(postSvc),
		handler.NewFeedHandler(feedSvc),
	)
	return a, nil
}

// NewWorker 初始化 Worker 进程所需的依赖（不分配 L1 本地缓存）。
func NewWorker(ctx context.Context, cfg *config.Config) (*App, error) {
	a, _, _, err := newBase(ctx, cfg)
	if err != nil {
		return nil, err
	}
	interval := time.Duration(cfg.Worker.ReconcileIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	a.Reconciler = consumer.NewLatestReconciler(a.Rebuilder, interval)
	return a, nil
}

func newBase(ctx context.Context, cfg *config.Config) (*App, *repository.PostRepo, *repository.LatestFeedRepo, error) {
	db, err := openMySQL(ctx, cfg.MySQL)
	if err != nil {
		return nil, nil, nil, err
	}
	rdb, err := openRedis(ctx, cfg.Redis)
	if err != nil {
		if sqlDB, serr := db.DB(); serr == nil {
			_ = sqlDB.Close()
		}
		return nil, nil, nil, err
	}
	postRepo := repository.NewPostRepo(db)
	feedRepo := repository.NewLatestFeedRepo(rdb, cfg.Feed.LatestHotSize)
	return &App{
		Cfg:       cfg,
		DB:        db,
		Redis:     rdb,
		Rebuilder: service.NewLatestRebuilder(postRepo, feedRepo),
	}, postRepo, feedRepo, nil
}

func (a *App) Close() error {
	var firstErr error
	if a.AuthRedis != nil {
		if err := a.AuthRedis.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if a.Redis != nil {
		if err := a.Redis.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if a.DB != nil {
		if sqlDB, err := a.DB.DB(); err == nil {
			if err := sqlDB.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// NewGormConfig 构造启用参数化 SQL 日志的 GORM 配置，保留错误、耗时和 SQL 模板，避免在日志中泄露密码哈希等敏感实参。
func NewGormConfig(w io.Writer, slowThreshold time.Duration) *gorm.Config {
	if w == nil {
		w = os.Stderr
	}
	if slowThreshold <= 0 {
		slowThreshold = 200 * time.Millisecond
	}
	return &gorm.Config{
		Logger: logger.New(
			log.New(w, "", log.LstdFlags),
			logger.Config{
				SlowThreshold:             slowThreshold,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: true,
				ParameterizedQueries:      true,
				Colorful:                  false,
			},
		),
	}
}

func openMySQL(ctx context.Context, cfg config.MySQLConfig) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(cfg.DSN), NewGormConfig(os.Stderr, 200*time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("mysql sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func openRedis(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	return openRedisWithOptions(ctx, &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}

// openRedisNoRetry 创建禁用自动重试的 Redis 客户端，避免刷新轮换脚本在结果未知时被重发。
func openRedisNoRetry(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	return openRedisWithOptions(ctx, &redis.Options{
		Addr:       cfg.Addr,
		Password:   cfg.Password,
		DB:         cfg.DB,
		MaxRetries: -1,
	})
}

func openRedisWithOptions(ctx context.Context, opt *redis.Options) (*redis.Client, error) {
	rdb := redis.NewClient(opt)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return rdb, nil
}

func newStorage(cfg config.StorageConfig) (storage.Service, error) {
	switch cfg.Provider {
	case "qiniu":
		return storage.NewQiniu(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported storage provider %q", cfg.Provider)
	}
}
