package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Kirep-06/gocache"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/handler"
	"wallpaper-feed/internal/infra"
	"wallpaper-feed/internal/model"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
)

const (
	serviceName              = "wallpaper-feed"
	jwtIssuer                = "wallpaper-feed"
	accessTokenTTL           = 2 * time.Hour
	l1CacheSizeBytes         = 64 * 1024 * 1024
	userProfileL1TTL         = 30 * time.Second
	userProfileL2TTL         = 30 * time.Minute
	wallpaperDetailL1TTL     = 30 * time.Second
	wallpaperDetailColdL2TTL = 10 * time.Minute
)

// App 持有后端运行期与关闭释放阶段所需的依赖句柄和 HTTP 路由引擎。
type App struct {
	Logger *slog.Logger
	DB     *gorm.DB
	Redis  *redis.Client
	Router *gin.Engine
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

// New 装配纯内存组件（fail-fast）、MySQL、Redis、领域服务与 HTTP 路由。
func New(ctx context.Context, cfg *config.Config) (*App, error) {
	logger := NewLogger(cfg.IsDev())

	// 纯内存校验型组件前置初始化，配置异常时 fail-fast，避免无效建连与回滚清理。
	jwtManager, err := jwtutil.NewManager(cfg.JWTSecret, jwtIssuer, accessTokenTTL)
	if err != nil {
		return nil, fmt.Errorf("init jwt manager: %w", err)
	}
	store, err := storage.New(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("init storage: %w", err)
	}

	db, err := infra.OpenMySQL(ctx, cfg.MySQL, cfg.IsDev())
	if err != nil {
		return nil, err
	}

	rdb, err := infra.OpenRedis(ctx, cfg.Redis)
	if err != nil {
		_ = infra.CloseMySQL(db)
		return nil, err
	}

	l1 := gocache.NewCache(l1CacheSizeBytes)
	profileCache := cache.NewMultiLevel[model.UserProfileCache](
		l1, rdb, userProfileL1TTL, userProfileL2TTL,
	)
	wallpaperCache := cache.NewMultiLevel(
		l1, rdb, wallpaperDetailL1TTL, wallpaperDetailColdL2TTL,
		cache.WithL2TTLFunc(service.WallpaperDetailL2TTL),
	)
	latestFeed := cache.NewLatestFeed(rdb)

	userRepo := repository.NewUserRepository(db)
	sessionRepo := repository.NewAuthSessionRepository(rdb)
	metaRepo := repository.NewMetaRepository(db)
	wallpaperRepo := repository.NewWallpaperRepository(db)

	userSvc := service.NewUserService(userRepo, wallpaperRepo, profileCache, store)
	authSvc := service.NewAuthService(userRepo, sessionRepo, userSvc, jwtManager)
	uploadSvc := service.NewUploadService(store)
	wallpaperSvc := service.NewWallpaperService(metaRepo, wallpaperRepo, userSvc, wallpaperCache, latestFeed, store)

	router := handler.NewRouter(handler.RouterDeps{
		IsDev:            cfg.IsDev(),
		AuthService:      authSvc,
		AuthHandler:      handler.NewAuthHandler(authSvc),
		UserHandler:      handler.NewUserHandler(userSvc),
		MetaHandler:      handler.NewMetaHandler(wallpaperSvc),
		UploadHandler:    handler.NewUploadHandler(uploadSvc),
		WallpaperHandler: handler.NewWallpaperHandler(wallpaperSvc),
	})

	return &App{
		Logger: logger,
		DB:     db,
		Redis:  rdb,
		Router: router,
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
	if err := infra.CloseMySQL(a.DB); err != nil {
		errs = append(errs, fmt.Errorf("close mysql: %w", err))
	}
	return errors.Join(errs...)
}
