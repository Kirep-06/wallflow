package app

import (
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"log/slog"
	"os"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/handler"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/service"
)

const (
	serviceName          = "wallpaper-feed"
	startupPingTimeout   = 5 * time.Second
	mysqlDefaultMaxOpen  = 50
	mysqlDefaultMaxIdle  = 10
	mysqlConnMaxLifetime = 30 * time.Minute
	redisDialTimeout     = 3 * time.Second
	redisReadTimeout     = 2 * time.Second
	redisWriteTimeout    = 2 * time.Second

	jwtIssuer        = "wallpaper-feed"
	accessTokenTTL   = 2 * time.Hour
	refreshTokenTTL  = 7 * 24 * time.Hour
	bcryptCost       = 10
	l1CacheSizeBytes = 64 * 1024 * 1024
	userProfileL1TTL = 30 * time.Second
	userProfileL2TTL = 30 * time.Minute
	defaultAvatarKey = "avatars/default.png"
)

// App 持有后端核心依赖句柄与 HTTP 路由引擎。
type App struct {
	Config  *config.Config
	Logger  *slog.Logger
	DB      *gorm.DB
	Redis   *redis.Client
	L1Cache *gocache.Cache
	Router  *gin.Engine
}

// NewLogger 按运行环境初始化全局 slog.Logger。
func NewLogger(isDev bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	var h slog.Handler
	if isDev {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}

	logger := slog.New(h).With(slog.String("service", serviceName))
	slog.SetDefault(logger)
	return logger
}

// New 装配 MySQL、Redis、L1 gocache、领域服务与 HTTP 路由。
func New(ctx context.Context, cfg *config.Config) (*App, error) {
	logger := NewLogger(cfg.IsDev())

	db, err := openMySQL(ctx, cfg)
	if err != nil {
		return nil, err
	}

	rdb, err := openRedis(ctx, cfg.Redis)
	if err != nil {
		closeDB(db)
		return nil, err
	}

	jwtManager, err := jwtutil.NewManager(cfg.JWTSecret, jwtIssuer, accessTokenTTL)
	if err != nil {
		_ = rdb.Close()
		closeDB(db)
		return nil, fmt.Errorf("init jwt manager: %w", err)
	}

	l1 := gocache.NewCache(l1CacheSizeBytes)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](
		l1, rdb, userProfileL1TTL, userProfileL2TTL,
	)

	userRepo := repository.NewUserRepository(db)
	sessionRepo := repository.NewAuthSessionRepository(rdb)

	userSvc := service.NewUserService(
		userRepo,
		profileCache,
		cfg.Storage.CDNDomain,
		defaultAvatarKey,
		logger,
	)

	authSvc, err := service.NewAuthService(
		userRepo,
		sessionRepo,
		userSvc,
		jwtManager,
		refreshTokenTTL,
		bcryptCost,
		logger,
	)
	if err != nil {
		_ = rdb.Close()
		closeDB(db)
		return nil, fmt.Errorf("init auth service: %w", err)
	}

	authHandler := handler.NewAuthHandler(authSvc)
	userHandler := handler.NewUserHandler(userSvc)

	router := handler.NewRouter(handler.RouterDeps{
		Config:      cfg,
		AuthService: authSvc,
		AuthHandler: authHandler,
		UserHandler: userHandler,
	})

	return &App{
		Config:  cfg,
		Logger:  logger,
		DB:      db,
		Redis:   rdb,
		L1Cache: l1,
		Router:  router,
	}, nil
}

// Close 优雅释放 Redis 与 MySQL 连接池资源。
func (a *App) Close() error {
	var errs []error
	if a.Redis != nil {
		if err := a.Redis.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close redis: %w", err))
		}
	}
	if a.DB != nil {
		if sqlDB, err := a.DB.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close mysql: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

func openMySQL(ctx context.Context, cfg *config.Config) (*gorm.DB, error) {
	logLevel := gormlogger.Warn
	if !cfg.IsDev() {
		logLevel = gormlogger.Error
	}

	dbLogger := gormlogger.New(
		stdlog.New(os.Stdout, "\r\n", stdlog.LstdFlags),
		gormlogger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			// 强制日志参数化，防止慢查询或错误日志泄露 password_hash 等敏感字段。
			ParameterizedQueries: true,
			Colorful:             false,
		},
	)

	db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN()), &gorm.Config{
		TranslateError: true,
		Logger:         dbLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("connect mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get mysql sql.DB: %w", err)
	}

	maxOpen := cfg.MySQL.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = mysqlDefaultMaxOpen
	}
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(min(mysqlDefaultMaxIdle, maxOpen))
	sqlDB.SetConnMaxLifetime(mysqlConnMaxLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, startupPingTimeout)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func openRedis(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr(),
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  redisDialTimeout,
		ReadTimeout:  redisReadTimeout,
		WriteTimeout: redisWriteTimeout,
	})

	pingCtx, cancel := context.WithTimeout(ctx, startupPingTimeout)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return rdb, nil
}

func closeDB(db *gorm.DB) {
	if db == nil {
		return
	}
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}