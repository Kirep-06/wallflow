package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	a, err := app.NewAPI(ctx, cfg)
	if err != nil {
		slog.Error("init api failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := a.Close(); err != nil {
			slog.Warn("close api resources failed", "err", err)
		}
	}()

	// 启动时若热窗口尚未就绪则异步重建
	a.Rebuilder.TriggerAsync()

	// 启动进程内 OutboxRelay（先停 HTTP Server，再停 Relay 排空收尾，避免停机前新发帖遗漏唤醒）
	relayCtx, relayCancel := context.WithCancel(context.Background())
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		a.OutboxRelay.Run(relayCtx)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           a.Router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("api listening", "addr", cfg.HTTP.Addr, "env", cfg.App.Env)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("api shutting down")
	case err := <-errCh:
		slog.Error("api server error", "err", err)
	}

	timeout := time.Duration(cfg.HTTP.ShutdownTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("api graceful shutdown failed", "err", err)
	}

	relayCancel()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		slog.Warn("outbox relay shutdown timeout")
	}

	slog.Info("api stopped")
}
