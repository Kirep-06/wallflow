package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"wallpaper-feed/internal/app"
	"wallpaper-feed/internal/cache"
	"wallpaper-feed/internal/config"
	"wallpaper-feed/internal/infra"
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

	reconciler := worker.NewLatestFeedReconciler(
		repository.NewWallpaperRepository(db),
		cache.NewLatestFeed(rdb),
	)

	logger.Info("worker started", slog.String("env", cfg.Server.Env))
	reconciler.Run(ctx)
	logger.Info("shutdown signal received, worker stopped")
}
