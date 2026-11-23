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
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/pkg/jwtutil"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/service"
	"wallpaper-feed/internal/storage"
	"wallpaper-feed/internal/worker"
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
	followStatsL1TTL         = 15 * time.Second
	followStatsL2TTL         = 10 * time.Minute
	followingIDsL1TTL        = 15 * time.Second
	followingIDsL2TTL        = 10 * time.Minute
)

// App 持有后端运行期与关闭释放阶段所需的依赖句柄、后台 OutboxRelay 与 HTTP 路由引擎。
type App struct {
	Logger      *slog.Logger
	DB          *gorm.DB
	Redis       *redis.Client
	MQConn      *mq.ConnManager
	MQPublisher *mq.ConfirmPublisher
	OutboxRelay *worker.OutboxRelay
	Router      *gin.Engine
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
	followStatsCache := cache.NewMultiLevel[model.FollowStatsCache](
		l1, rdb, followStatsL1TTL, followStatsL2TTL,
	)
	followingIDsCache := cache.NewMultiLevel[[]uint64](
		l1, rdb, followingIDsL1TTL, followingIDsL2TTL,
	)
	latestFeed := cache.NewLatestFeed(rdb)
	bigVSet := cache.NewBigVSet(rdb, cache.BigVL1TTL)
	followingFeed := cache.NewFollowingFeed(rdb)

	userRepo := repository.NewUserRepository(db)
	sessionRepo := repository.NewAuthSessionRepository(rdb)
	metaRepo := repository.NewMetaRepository(db)
	wallpaperRepo := repository.NewWallpaperRepository(db)
	followRepo := repository.NewFollowRepository(db)

	var (
		mqConn      *mq.ConnManager
		mqPublisher *mq.ConfirmPublisher
		outboxRelay *worker.OutboxRelay
	)
	if cfg.RabbitMQ.Enabled {
		mqConn = mq.NewConnManager(cfg.RabbitMQ)
		mqPublisher = mq.NewConfirmPublisher(mqConn)
		outboxRepo := repository.NewOutboxRepository(db)
		outboxRelay = worker.NewOutboxRelay(outboxRepo, mqPublisher)
	}

	userSvc := service.NewUserService(service.UserServiceDeps{
		UserRepo:          userRepo,
		WallpaperRepo:     wallpaperRepo,
		FollowRepo:        followRepo,
		ProfileCache:      profileCache,
		FollowStatsCache:  followStatsCache,
		FollowingIDsCache: followingIDsCache,
		Store:             store,
	})
	authSvc := service.NewAuthService(userRepo, sessionRepo, userSvc, jwtManager)
	uploadSvc := service.NewUploadService(store)

	wpOpts := []service.WallpaperOption{
		service.WithWallpaperFollowingFeed(followingFeed),
	}
	if outboxRelay != nil {
		wpOpts = append(wpOpts, service.WithOutboxNotifier(outboxRelay))
	}
	wallpaperSvc := service.NewWallpaperService(metaRepo, wallpaperRepo, userSvc, wallpaperCache, latestFeed, store, wpOpts...)
	followSvc := service.NewFollowService(
		followRepo,
		userSvc,
		followStatsCache,
		followingIDsCache,
		service.WithFollowFeedCaches(followingFeed, bigVSet),
	)
	feedSvc := service.NewFeedService(service.FeedServiceDeps{
		FollowRepo:        followRepo,
		WallpaperRepo:     wallpaperRepo,
		FollowingIDsCache: followingIDsCache,
		BigVSet:           bigVSet,
		FollowingFeed:     followingFeed,
		WallpaperSvc:      wallpaperSvc,
	})

	router := handler.NewRouter(handler.RouterDeps{
		IsDev:            cfg.IsDev(),
		AuthService:      authSvc,
		AuthHandler:      handler.NewAuthHandler(authSvc),
		UserHandler:      handler.NewUserHandler(userSvc),
		FollowHandler:    handler.NewFollowHandler(followSvc),
		FeedHandler:      handler.NewFeedHandler(feedSvc),
		MetaHandler:      handler.NewMetaHandler(wallpaperSvc),
		UploadHandler:    handler.NewUploadHandler(uploadSvc),
		WallpaperHandler: handler.NewWallpaperHandler(wallpaperSvc),
	})

	return &App{
		Logger:      logger,
		DB:          db,
		Redis:       rdb,
		MQConn:      mqConn,
		MQPublisher: mqPublisher,
		OutboxRelay: outboxRelay,
		Router:      router,
	}, nil
}

// Close 按依赖逆序优雅释放 RabbitMQ、Redis 与 MySQL 资源。
func (a *App) Close() error {
	var errs []error
	if a.MQPublisher != nil {
		if err := a.MQPublisher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mq publisher: %w", err))
		}
	}
	if a.MQConn != nil {
		if err := a.MQConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mq connection: %w", err))
		}
	}
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
