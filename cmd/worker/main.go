package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"wallpaper-feed/internal/app"
	"wallpaper-feed/internal/config"
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

	logger := app.NewLogger(cfg.App).With(slog.String("component", "worker"))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("worker placeholder started (M1 skeleton, awaiting M3/M5 jobs)",
		slog.String("env", cfg.App.Env),
	)

	<-ctx.Done()
	logger.Info("shutdown signal received, worker stopped")
}
