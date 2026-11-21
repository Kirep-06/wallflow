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
	cfgPath := flag.String("config", "configs/config.local.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("load config failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.NewWorker(ctx, cfg)
	if err != nil {
		slog.Error("init worker failed", "err", err)
		os.Exit(1)
	}
	defer func() { _ = a.Close() }()

	slog.Info("worker started", "reconcile_interval_sec", cfg.Worker.ReconcileIntervalSeconds)
	a.Reconciler.Run(ctx)
	slog.Info("worker stopped")
}
