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
		Addr:              cfg.Server.Addr,
		Handler:           application.Router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		application.Logger.Info("http api server starting",
			slog.String("addr", cfg.Server.Addr),
			slog.String("env", cfg.App.Env),
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		application.Logger.Error("http server graceful shutdown failed", slog.Any("err", err))
	}
	if err := application.Close(); err != nil {
		application.Logger.Error("close application resources failed", slog.Any("err", err))
	}
	application.Logger.Info("http api server stopped")
}
