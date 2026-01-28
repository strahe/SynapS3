package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	// Load configuration.
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Set up structured logging.
	logger := setupLogger(cfg.Logging)
	slog.SetDefault(logger)

	logger.Info("starting SynapS3",
		"port", cfg.Server.Port,
		"network", cfg.Filecoin.Network,
		"db_driver", cfg.Database.Driver,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialise database.
	database, err := db.New(cfg.Database)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	if err := db.Migrate(ctx, database); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	// Initialise local cache.
	localCache, err := cache.NewFilesystem(cfg.Cache.Dir)
	if err != nil {
		return fmt.Errorf("initialising cache: %w", err)
	}

	// Build state machine.
	sm := state.NewObjectStateMachine()

	// Create backend.
	be := backend.New(database, localCache, sm, logger)

	// Set up IAM (simple root-only for now).
	rootCfg := middlewares.RootUserConfig{
		Access: cfg.S3.AccessKey,
		Secret: cfg.S3.SecretKey,
	}

	// Create VersityGW S3 server.
	srv, err := s3api.New(
		be,
		rootCfg,
		cfg.S3.Region,
		&auth.IAMServiceInternal{},
		nil, // audit logger
		nil, // admin logger
		nil, // event sender
		nil, // metrics manager
		s3api.WithHealth("/health"),
	)
	if err != nil {
		return fmt.Errorf("creating S3 server: %w", err)
	}

	// Start background workers.
	wm := worker.NewManager(logger,
		worker.NewUploader(database, cfg.Worker.Upload.Concurrency, cfg.Worker.Upload.PollInterval, logger),
		worker.NewOnChain(database, cfg.Worker.OnChain.Concurrency, cfg.Worker.OnChain.PollInterval, logger),
		worker.NewEvictor(database, localCache, cfg.Worker.Evictor.Interval, logger),
	)
	go wm.Start(ctx)

	// Start S3 server.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("S3 server listening", "port", cfg.Server.Port)
		errCh <- srv.ServeMultiPort([]string{cfg.Server.Port})
	}()

	// Wait for shutdown signal or server error.
	select {
	case <-ctx.Done():
		logger.Info("received shutdown signal")
	case err := <-errCh:
		return fmt.Errorf("S3 server error: %w", err)
	}

	// Graceful shutdown.
	logger.Info("shutting down...")
	if err := srv.ShutDown(); err != nil {
		logger.Error("error shutting down S3 server", "error", err)
	}
	be.Shutdown()

	logger.Info("SynapS3 stopped")
	return nil
}

func setupLogger(cfg config.LoggingConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch cfg.Format {
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default:
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}
