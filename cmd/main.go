// Package main provides the entry point for the Aether KV key-value store application.
// It initializes the logger, loads configuration, creates the storage engine,
// and starts the command-line interface.
package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/jassi-singh/aether-kv/internal/cli"
	"github.com/jassi-singh/aether-kv/internal/config"
	"github.com/jassi-singh/aether-kv/internal/engine"
)

func main() {
	// Initialize structured logger
	// Use JSON handler for production, or TextHandler for development
	slogHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Change to LevelDebug for verbose logging
	})
	logger := slog.New(slogHandler)
	slog.SetDefault(logger)

	// Load configuration
	slog.Info("main: loading configuration")
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("main: failed to load configuration",
			"error", err)
		log.Fatalf("Failed to load config: %v", err)
	}
	slog.Info("main: configuration loaded successfully",
		"data_dir", cfg.DATA_DIR,
		"header_size", cfg.HEADER_SIZE,
		"batch_size", cfg.BATCH_SIZE,
		"sync_interval", cfg.SYNC_INTERVAL,
	)

	// Initialize KV engine with dependency injection
	kv, err := engine.NewKVEngine(cfg)
	if err != nil {
		slog.Error("main: failed to initialize KV engine",
			"error", err)
		log.Fatalf("Failed to create KV engine: %v", err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			slog.Error("main: error closing KV engine",
				"error", err)
		}
	}()

	slog.Info("main: Aether KV started successfully")

	// Start CLI handler
	cliHandler := cli.NewHandler(kv)
	if err := cliHandler.Run(); err != nil {
		slog.Error("main: CLI handler error",
			"error", err)
		log.Fatalf("CLI error: %v", err)
	}
}
