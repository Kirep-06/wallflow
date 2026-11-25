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

const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpWriteTimeout      = 60 * time.Second
	httpIdleTimeout       = 60 * time.Second
	httpShutdownTimeout   = 10 * time.Second
	relayShutdownTimeout  = 10 * time.Second
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

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.New(rootCtx, cfg)
	if err != nil {
		slog.Error("initialize application failed", slog.Any("err", err))
		os.Exit(1)
	}

	var (
		relayCancel context.CancelFunc
		relayDone   chan struct{}
	)
	if application.OutboxRelay != nil {
		var relayCtx context.Context
		relayCtx, relayCancel = context.WithCancel(context.Background())
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			application.Logger.Info("outbox relay started")
			application.OutboxRelay.Run(relayCtx)
		}()
	}

	stopRelay := func() {
		if relayCancel == nil {
			return
		}
		relayCancel()
		timer := time.NewTimer(relayShutdownTimeout)
		defer timer.Stop()
		select {
		case <-relayDone:
		case <-timer.C:
			application.Logger.Warn("outbox relay drain timed out", slog.Duration("timeout", relayShutdownTimeout))
		}
	}

	srv := &http.Server{
		Addr:              cfg.Server.Addr(),
		Handler:           application.Router,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		application.Logger.Info("http api server starting",
			slog.String("addr", cfg.Server.Addr()),
			slog.String("env", cfg.Server.Env),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-rootCtx.Done():
		application.Logger.Info("shutdown signal received, draining http server")
	case err := <-errCh:
		if err != nil {
			application.Logger.Error("http server exited unexpectedly", slog.Any("err", err))
			stopRelay()
			_ = application.Close()
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()

	// 严格按序排空：先停 HTTP 接收新请求 -> 再停并排空 OutboxRelay -> 最后释放 MQ/Redis/MySQL。
	if err := srv.Shutdown(shutdownCtx); err != nil {
		application.Logger.Error("http server graceful shutdown failed", slog.Any("err", err))
	}
	stopRelay()
	if err := application.Close(); err != nil {
		application.Logger.Error("close application resources failed", slog.Any("err", err))
	}
	application.Logger.Info("http api server stopped")
}
