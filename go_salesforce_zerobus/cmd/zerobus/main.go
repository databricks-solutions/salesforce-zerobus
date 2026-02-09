package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/databricks-solutions/go-salesforce-zerobus/internal/config"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/pipeline"
)

func main() {
	// Set up structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load .env file (optional, for local development)
	if err := godotenv.Load(); err != nil {
		logger.Debug("No .env file found, using environment variables")
	}

	// Load and validate configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Error("Configuration error", "error", err)
		os.Exit(1)
	}

	logger.Info("Starting salesforce-zerobus (Go)",
		"object", cfg.ObjectName(),
		"topic", cfg.Topic(),
		"table", cfg.DBTableName,
	)

	// Set up graceful shutdown via OS signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Run the pipeline (blocks until shutdown or fatal error)
	if err := pipeline.Run(ctx, cfg, logger); err != nil {
		logger.Error("Pipeline exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("salesforce-zerobus shut down cleanly")
}
