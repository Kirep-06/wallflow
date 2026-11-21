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
	defer func() {
		if err := a.Close(); err != nil {
			slog.Warn("close worker resources failed", "err", err)
		}
	}()

	slog.Info("worker started",
		"reconcile_interval_sec", cfg.Worker.ReconcileIntervalSeconds,
		"outbox_cleanup_interval_sec", cfg.Worker.OutboxCleanupIntervalSeconds,
		"fanout_prefetch", cfg.Worker.FanoutPrefetch,
		"fanout_concurrency", cfg.Worker.FanoutConcurrency,
		"fanout_batch_size", cfg.Worker.FanoutBatchSize,
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		a.Reconciler.Run(gctx)
		return nil
	})
	g.Go(func() error {
		return a.FollowingFanout.Run(gctx)
	})
	g.Go(func() error {
		a.FollowingMaintainer.Run(gctx)
		return nil
	})

	if err := g.Wait(); err != nil {
		slog.Error("worker exited with error", "err", err)
	}
	slog.Info("worker stopped")
}
