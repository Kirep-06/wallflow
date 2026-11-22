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
	httpReadTimeout       = 10 * time.Second
	httpWriteTimeout      = 15 * time.Second
	httpIdleTimeout       = 60 * time.Second
	httpShutdownTimeout   = 10 * time.Second
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
			_ = application.Close()
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		application.Logger.Error("http server graceful shutdown failed", slog.Any("err", err))
	}
	if err := application.Close(); err != nil {
		application.Logger.Error("close application resources failed", slog.Any("err", err))
	}
	application.Logger.Info("http api server stopped")
}
