package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/buildinfo"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/s3access"
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/uptrace/bun"
	"github.com/urfave/cli/v3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3log"
)

const defaultS3MultipartMaxParts = 10000

func main() {
	if err := newRootCommand().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cli.Command {
	return &cli.Command{
		Name:        "synaps3",
		Usage:       "S3-compatible gateway to Filecoin",
		HideVersion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "path to TOML config file; defaults to ~/.synaps3/config.toml",
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unknown command %q, run --help for available commands", cmd.Args().First())
			}
			return cli.ShowRootCommandHelp(cmd)
		},
		Commands: []*cli.Command{
			initCommand(),
			serveCommand(),
			migrateCommand(),
			providerCommand(),
			adminCommand(),
			versionCommand(),
		},
	}
}

func initCommand() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "initialize config and runtime directories",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "dir",
				Usage: "app data directory to initialize; defaults to ~/.synaps3",
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unexpected argument %q, init takes no positional arguments", cmd.Args().First())
			}
			if cmd.Root().IsSet("config") {
				return fmt.Errorf("init does not use --config; use --dir to choose the app data directory")
			}

			result, err := config.InitAppDataDir(config.InitOptions{Dir: cmd.String("dir")})
			if err != nil {
				return err
			}

			return writeInitResult(cmd, result)
		},
	}
}

func writeInitResult(cmd *cli.Command, result config.InitResult) error {
	output := fmt.Sprintf(
		"Initialized SynapS3 app data directory: %s\nConfig: %s\nSet filecoin.private_key in the config file or SYNAPS3_FILECOIN_PRIVATE_KEY before serving.\n",
		result.Dir,
		result.ConfigPath,
	)
	if result.DefaultDir {
		output += "Next: synaps3 serve\n"
	} else {
		output += fmt.Sprintf("Next: synaps3 serve --config %s\n", result.ConfigPath)
	}

	n, err := cmd.Root().Writer.Write([]byte(output))
	if err != nil {
		return fmt.Errorf("writing init output: %w", err)
	}
	if n != len(output) {
		return io.ErrShortWrite
	}
	return nil
}

func serveCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "start the S3 gateway server",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Len() > 0 {
				return fmt.Errorf("unexpected argument %q, serve takes no positional arguments", cmd.Args().First())
			}
			src, err := configSourceFromCommand(cmd)
			if err != nil {
				return err
			}
			return runServe(ctx, src)
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
			src, err := configSourceFromCommand(cmd)
			if err != nil {
				return err
			}
			return runMigrate(ctx, src)
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

func configSourceFromCommand(cmd *cli.Command) (config.Source, error) {
	root := cmd.Root()
	return config.ResolveSource(root.String("config"), root.IsSet("config"))
}

// loadConfigAndDB is the shared setup used by serve and migrate.
// It loads config and opens the database but does NOT run full config validation,
// so that commands like "migrate" only need DB-related config (not S3 credentials etc.).
func loadConfigAndDB(ctx context.Context, src config.Source) (*config.Config, *bun.DB, error) {
	cfg, err := config.LoadSource(src)
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

func runMigrate(ctx context.Context, src config.Source) error {
	_, database, err := loadConfigAndDB(ctx, src)
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

func s3ServerOptions(cfg config.ServerConfig) ([]s3api.Option, error) {
	opts := []s3api.Option{
		s3api.WithHealth("/health"),
		s3api.WithConcurrencyLimiter(cfg.MaxConnections, cfg.MaxRequests),
		s3api.WithMpMaxParts(defaultS3MultipartMaxParts),
		s3api.WithQuiet(),
	}
	if cfg.TLS.Enabled {
		cs := utils.NewCertStorage()
		if err := cs.SetCertificate(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil {
			return nil, fmt.Errorf("loading S3 TLS certificate: %w", err)
		}
		opts = append(opts, s3api.WithTLS(cs))
	}
	return opts, nil
}

func newS3IAMService(ctx context.Context, repos *repository.Repositories) (auth.IAMService, auth.Account, error) {
	iam := s3iam.NewService(repos)
	root, err := iam.EnsureRootAccount(ctx)
	if err != nil {
		return nil, auth.Account{}, fmt.Errorf("initializing S3 IAM: %w", err)
	}
	return iam, root, nil
}

func runServe(ctx context.Context, src config.Source) error {
	cfg, database, err := loadConfigAndDB(ctx, src)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	settingsSvc, err := admin.NewSettingsService(cfg, src)
	if err != nil {
		return fmt.Errorf("initialising settings service: %w", err)
	}

	// Full config validation — only required for serve, not migrate.
	if fieldErrs := cfg.FieldValidationErrors(); len(fieldErrs) > 0 {
		if shouldStartSetupMode(fieldErrs) {
			return runSetupMode(ctx, cfg, settingsSvc, fieldErrs)
		}
		return fmt.Errorf("validating config: %w", joinFieldErrors(fieldErrs))
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
	adminEvents := admin.NewEventHub()

	// Initialize Filecoin SDK clients.
	client, err := synapse.NewClient(ctx, synapse.ClientConfig{
		PrivateKey:           cfg.Filecoin.PrivateKey,
		RPCURL:               cfg.Filecoin.RPCURL,
		Source:               cfg.Filecoin.Source,
		WithCDN:              cfg.Filecoin.WithCDN,
		AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
		Logger:               logger,
	})
	if err != nil {
		return fmt.Errorf("initializing Filecoin SDK: %w", err)
	}
	defer func() { _ = client.Close() }()
	storageClient := synapse.AdaptStorageService(client.Storage())
	walletQuerier := synapse.NewWalletQuerier(client.Payments(), client.Address(), client.Chain())
	walletOperator := synapse.NewWalletOperator(client.Payments(), client.Chain())
	walletReceiptClient, err := ethclient.DialContext(ctx, cfg.Filecoin.RPCURL)
	if err != nil {
		return fmt.Errorf("creating wallet receipt client: %w", err)
	}
	defer walletReceiptClient.Close()

	autoEvict := isAutoEvictEnabled(cfg.Cache.EvictionPolicy)

	// Create backend.
	be := backend.New(repos, localCache, sm, storageClient, logger,
		backend.WithUploadMaxRetries(cfg.Worker.Upload.MaxRetries),
		backend.WithEvictMaxRetries(cfg.Worker.Evictor.MaxRetries),
		backend.WithAutoEvict(autoEvict),
	)

	// Set up DB-backed IAM for root auth plus persisted non-root S3 users.
	iamSvc, rootAccount, err := newS3IAMService(ctx, repos)
	if err != nil {
		return err
	}
	defer func() {
		if err := iamSvc.Shutdown(); err != nil {
			logger.Error("error shutting down S3 IAM service", "error", err)
		}
	}()
	rootCfg := middlewares.RootUserConfig{
		Access: rootAccount.Access,
		Secret: rootAccount.Secret,
	}

	s3Opts, err := s3ServerOptions(cfg.Server)
	if err != nil {
		return err
	}

	// Create VersityGW S3 server.
	srv, err := s3api.New(
		be,
		rootCfg,
		cfg.S3.Region,
		iamSvc,
		s3AccessLogger(logger, cfg.Logging.S3Access),
		nil, // admin logger
		nil, // event sender
		nil, // metrics manager
		s3Opts...,
	)
	if err != nil {
		return fmt.Errorf("creating S3 server: %w", err)
	}

	// Start background workers.
	wm := worker.NewManager(repos, logger, autoEvict,
		worker.NewUploader(repos, localCache, storageClient, walletQuerier, sm, autoEvict,
			cfg.Worker.Upload.Concurrency, cfg.Worker.Upload.PollInterval, logger,
			worker.WithEvictMaxRetries(cfg.Worker.Evictor.MaxRetries),
			worker.WithTargetCopies(cfg.Filecoin.DefaultCopies),
			worker.WithEventPublisher(adminEvents)),
		worker.NewEvictor(repos, localCache, sm,
			cfg.Worker.Evictor.Concurrency, cfg.Worker.Evictor.PollInterval, logger),
		worker.NewWalletOperationRunner(repos, walletOperator, walletReceiptClient, 5*time.Second, logger,
			worker.WithWalletOperationEventPublisher(adminEvents)),
	).WithTaskMaxRetries(cfg.Worker.Upload.MaxRetries, cfg.Worker.Evictor.MaxRetries)
	go wm.Start(ctx)

	// Start admin server (healthz + metrics).
	adminSrv := admin.New(cfg.Admin.Addr, database, localCache, maxCacheBytes, repos, wm, walletQuerier, logger).
		WithEventHub(adminEvents).
		WithObjectStorage(storageClient).
		WithProviderIdentityResolver(admin.NewProviderIdentityResolver(client.SPRegistry(), cfg.Filecoin.RPCURL, logger)).
		WithSettings(settingsSvc).
		WithS3IAM(iamSvc, rootAccount.Access)
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

func runSetupMode(ctx context.Context, cfg *config.Config, settingsSvc *admin.SettingsService, fieldErrs []config.FieldError) error {
	logger := setupLogger(cfg.Logging)
	slog.SetDefault(logger)

	logger.Warn("starting SynapS3 admin setup mode", "validation_errors", len(fieldErrs), "admin_addr", cfg.Admin.Addr)

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	adminSrv := admin.NewSetup(cfg.Admin.Addr, settingsSvc, logger)
	if err := adminSrv.Run(ctx); err != nil {
		return fmt.Errorf("admin setup server error: %w", err)
	}
	return nil
}

func shouldStartSetupMode(errs []config.FieldError) bool {
	if len(errs) == 0 {
		return false
	}

	for _, fieldErr := range errs {
		if !setupModeAllowedField(fieldErr.Field) {
			return false
		}
	}
	return true
}

func setupModeAllowedField(field string) bool {
	switch field {
	case "server.port",
		"server.max_connections",
		"server.max_requests",
		"server.tls.cert_file",
		"server.tls.key_file",
		"s3.region",
		"cache.dir",
		"cache.max_size_gb",
		"cache.eviction_policy",
		"filecoin.network",
		"filecoin.rpc_url",
		"filecoin.private_key",
		"filecoin.source",
		"filecoin.default_copies",
		"worker.upload.concurrency",
		"worker.upload.poll_interval",
		"worker.upload.max_retries",
		"worker.evictor.concurrency",
		"worker.evictor.poll_interval",
		"worker.evictor.max_retries",
		"logging.level",
		"logging.format",
		"logging.s3_access.enabled",
		"logging.s3_access.level":
		return true
	default:
		return false
	}
}

func joinFieldErrors(fieldErrs []config.FieldError) error {
	errs := make([]error, 0, len(fieldErrs))
	for _, fieldErr := range fieldErrs {
		errs = append(errs, fieldErr)
	}
	return errors.Join(errs...)
}

func isAutoEvictEnabled(policy string) bool {
	return strings.EqualFold(strings.TrimSpace(policy), "lru")
}

func s3AccessLogger(logger *slog.Logger, cfg config.LoggingS3AccessConfig) s3log.AuditLogger {
	if !cfg.Enabled {
		return nil
	}
	return s3access.NewLogger(logger, cfg.Level)
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
