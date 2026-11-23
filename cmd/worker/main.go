package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"wallpaper-feed/internal/app"
	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/infra"
	"wallpaper-feed/internal/mq"
	"wallpaper-feed/internal/repository"
	"wallpaper-feed/internal/worker"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to yaml config file (default: WF_CONFIG_FILE or configs/config.local.yaml)")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("load config failed", slog.Any("err", err))
		os.Exit(1)
	}

	logger := app.NewLogger(cfg.IsDev()).With(slog.String("component", "worker"))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := infra.OpenMySQL(ctx, cfg.MySQL, cfg.IsDev())
	if err != nil {
		logger.Error("open mysql failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := infra.CloseMySQL(db); err != nil {
			logger.Error("close mysql failed", slog.Any("err", err))
		}
	}()

	rdb, err := infra.OpenRedis(ctx, cfg.Redis)
	if err != nil {
		logger.Error("open redis failed", slog.Any("err", err))
		_ = infra.CloseMySQL(db)
		os.Exit(1)
	}
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Error("close redis failed", slog.Any("err", err))
		}
	}()

	wallpaperRepo := repository.NewWallpaperRepository(db)
	followRepo := repository.NewFollowRepository(db)
	interactionRepo := repository.NewInteractionRepository(db)
	latestFeed := cache.NewLatestFeed(rdb)
	bigVSet := cache.NewBigVSet(rdb, 0)
	followingFeed := cache.NewFollowingFeed(rdb)
	hotRank := cache.NewHotRank(rdb)

	latestReconciler := worker.NewLatestFeedReconciler(wallpaperRepo, latestFeed)
	bigVReconciler := worker.NewBigVReconciler(followRepo, bigVSet)
	statsReconciler := worker.NewStatsReconciler(wallpaperRepo, interactionRepo, rdb)
	hotRankReconciler := worker.NewHotRankReconciler(wallpaperRepo, hotRank)

	var (
		mqClient            *mq.Client
		fanoutConsumer      *worker.FollowingFanoutConsumer
		interactionConsumer *worker.InteractionAggConsumer
	)
	if cfg.RabbitMQ.Enabled {
		mqClient = mq.NewClient(cfg.RabbitMQ)
		fanoutConsumer = worker.NewFollowingFanoutConsumer(wallpaperRepo, followRepo, followingFeed)
		interactionConsumer = worker.NewInteractionAggConsumer(interactionRepo, rdb)
	} else {
		logger.Info("rabbitmq disabled, mq consumers skipped")
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		latestReconciler.Run(gctx)
		return nil
	})
	g.Go(func() error {
		bigVReconciler.Run(gctx)
		return nil
	})
	g.Go(func() error {
		statsReconciler.Run(gctx)
		return nil
	})
	g.Go(func() error {
		hotRankReconciler.Run(gctx)
		return nil
	})
	if fanoutConsumer != nil {
		g.Go(func() error {
			return fanoutConsumer.Run(gctx, mqClient)
		})
	}
	if interactionConsumer != nil {
		g.Go(func() error {
			return interactionConsumer.Run(gctx, mqClient)
		})
	}

	logger.Info("worker started",
		slog.String("env", cfg.Server.Env),
		slog.Bool("rabbitmq_enabled", cfg.RabbitMQ.Enabled),
	)
	if err := g.Wait(); err != nil {
		logger.Error("worker task exited with error", slog.Any("err", err))
	}

	// 任务全部优雅排空退出后，再逆序释放 RabbitMQ 连接（defer 继续释放 Redis 与 MySQL）。
	if mqClient != nil {
		if err := mqClient.Close(); err != nil {
			logger.Error("close mq client failed", slog.Any("err", err))
		}
	}
	logger.Info("shutdown signal received, worker stopped")
}
