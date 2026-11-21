package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	fmt.Println("Starting Wallpaper Feed API Service...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Wait for shutdown signal
	<-ctx.Done()
	fmt.Println("Shutting down Wallpaper Feed API Service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Cleanup logic here
	_ = shutdownCtx
	fmt.Println("Wallpaper Feed API Service stopped.")
}
