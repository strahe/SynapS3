package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/buildinfo"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/uptrace/bun"
	"github.com/urfave/cli/v3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
)

func main() {
	root := &cli.Command{
		Name:        "synaps3",
		Usage:       "S3-compatible gateway to Filecoin",
		HideVersion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "path to config file",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unknown command %q, run --help for available commands", cmd.Args().First())
			}
			return runServe(ctx, cmd.String("config"))
		},
		Commands: []*cli.Command{
			serveCommand(),
			migrateCommand(),
			providerCommand(),
			versionCommand(),
		},
	}

	if err := root.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func serveCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "start the S3 gateway server",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unexpected argument %q, serve takes no positional arguments", cmd.Args().First())
			}
			return runServe(ctx, cmd.Root().String("config"))
		},
	}
}

func migrateCommand() *cli.Command {
	return &cli.Command{
		Name:  "migrate",
		Usage: "run database migrations and exit",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unexpected argument %q, migrate takes no positional arguments", cmd.Args().First())
			}
			return runMigrate(ctx, cmd.Root().String("config"))
		},
	}
}

func versionCommand() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "print version information",
		Action: func(_ context.Context, _ *cli.Command) error {
			fmt.Println(buildinfo.String())
			return nil
		},
	}
}

// loadConfigAndDB is the shared setup used by serve and migrate.
// It loads config and opens the database but does NOT run full config validation,
// so that commands like "migrate" only need DB-related config (not S3 credentials etc.).
func loadConfigAndDB(ctx context.Context, configPath string) (*config.Config, *bun.DB, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("loading config: %w", err)
	}

	database, err := db.New(cfg.Database)
	if err != nil {
		return nil, nil, fmt.Errorf("opening database: %w", err)
	}

	if err := db.Ping(ctx, database); err != nil {
		_ = database.Close()
		return nil, nil, fmt.Errorf("pinging database: %w", err)
	}

	return cfg, database, nil
}

func runMigrate(ctx context.Context, configPath string) error {
	_, database, err := loadConfigAndDB(ctx, configPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	slog.Info("running database migrations")
	if err := db.RunMigrations(ctx, database); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}
	slog.Info("migrations completed successfully")
	return nil
}

func runServe(ctx context.Context, configPath string) error {
	cfg, database, err := loadConfigAndDB(ctx, configPath)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	// Full config validation — only required for serve, not migrate.
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	// Set up structured logging so migration and runtime logs use the configured level/format.
	logger := setupLogger(cfg.Logging)
	slog.SetDefault(logger)

	logger.Info("starting SynapS3",
		"version", buildinfo.Version,
		"port", cfg.Server.Port,
		"network", cfg.Filecoin.Network,
		"db_driver", cfg.Database.Driver,
	)

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Run migrations on startup.
	if err := db.RunMigrations(ctx, database); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	// Build repositories.
	repos := repository.NewRepositories(database)

	// Initialise local cache.
	maxCacheBytes := int64(cfg.Cache.MaxSizeGB) * 1024 * 1024 * 1024
	localCache, err := cache.NewFilesystem(cfg.Cache.Dir, maxCacheBytes)
	if err != nil {
		return fmt.Errorf("initialising cache: %w", err)
	}

	// Build state machine.
	sm := state.NewObjectStateMachine()

	// Initialize Filecoin SDK clients (optional — nil when private key is not configured).
	var storageClient synapse.StorageClient
	var walletQuerier synapse.WalletQuerier
	if cfg.Filecoin.PrivateKey != "" {
		client, sdkErr := synapse.NewClient(ctx, synapse.ClientConfig{
			PrivateKey:           cfg.Filecoin.PrivateKey,
			RPCURL:               cfg.Filecoin.RPCURL,
			Source:               cfg.Filecoin.Source,
			WithCDN:              cfg.Filecoin.WithCDN,
			AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
			Logger:               logger,
		})
		if sdkErr != nil {
			return fmt.Errorf("initializing Filecoin SDK: %w", sdkErr)
		}
		defer func() { _ = client.Close() }()
		storageClient = client.Storage()
		walletQuerier = synapse.NewWalletQuerier(client.Payments(), client.Address(), client.Chain())
	} else {
		logger.Warn("Filecoin private key not configured, SDK features disabled (uploads will not work)")
	}

	// Create backend.
	be := backend.New(repos, localCache, sm, storageClient, logger)

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
		s3api.WithConcurrencyLimiter(cfg.Server.MaxConnections, cfg.Server.MaxRequests),
	)
	if err != nil {
		return fmt.Errorf("creating S3 server: %w", err)
	}

	// Start background workers.
	autoEvict := isAutoEvictEnabled(cfg.Cache.EvictionPolicy)
	wm := worker.NewManager(repos, logger, autoEvict,
		worker.NewUploader(repos, localCache, storageClient, walletQuerier, sm, autoEvict,
			cfg.Worker.Upload.Concurrency, cfg.Worker.Upload.PollInterval, logger),
		worker.NewEvictor(repos, localCache, sm,
			cfg.Worker.Evictor.Concurrency, cfg.Worker.Evictor.PollInterval, logger),
	)
	go wm.Start(ctx)

	// Start admin server (healthz + metrics).
	adminSrv := admin.New(cfg.Admin.Addr, database, localCache, maxCacheBytes, repos, wm, walletQuerier, logger)
	errCh := make(chan error, 2)
	go func() {
		if err := adminSrv.Run(ctx); err != nil {
			errCh <- fmt.Errorf("admin server error: %w", err)
		}
	}()

	// Start S3 server.
	go func() {
		logger.Info("S3 server listening", "port", cfg.Server.Port)
		if err := srv.ServeMultiPort([]string{cfg.Server.Port}); err != nil {
			errCh <- fmt.Errorf("S3 server error: %w", err)
		}
	}()

	// Wait for shutdown signal or server error.
	select {
	case <-ctx.Done():
		logger.Info("received shutdown signal")
	case err := <-errCh:
		return err
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

func isAutoEvictEnabled(policy string) bool {
	return strings.EqualFold(strings.TrimSpace(policy), "lru")
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
