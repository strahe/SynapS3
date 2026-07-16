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

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/app"
	"github.com/strahe/synaps3/internal/buildinfo"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/provider"
	"github.com/strahe/synaps3/internal/synapse"
	sdk "github.com/strahe/synapse-go"
	"github.com/uptrace/bun"
	"github.com/urfave/cli/v3"
)

const configEnvVar = "SYNAPS3_CONFIG"

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
			walletCommand(),
			adminAuthCommand(),
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
	adminPasswordLine := ""
	if shouldPrintInitialAdminPassword(cmd.Root().Writer) {
		adminPasswordLine = fmt.Sprintf("Admin username: admin\nAdmin initial password: %s\n", result.AdminInitialPassword)
	} else {
		passwordPath, err := config.WriteAdminInitialPasswordFile(result.Dir, result.AdminInitialPassword)
		if err != nil {
			return err
		}
		adminPasswordLine = fmt.Sprintf("Admin username: admin\nAdmin initial password file: %s\n", passwordPath)
	}
	output := fmt.Sprintf(
		"Initialized SynapS3 app data directory: %s\nConfig: %s\n%sSet filecoin.private_key in the config file or SYNAPS3_FILECOIN_PRIVATE_KEY before serving.\n",
		result.Dir,
		result.ConfigPath,
		adminPasswordLine,
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

func shouldPrintInitialAdminPassword(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok || file != os.Stdout {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
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
	path, explicit := configPathFromCommand(cmd)
	return config.ResolveSource(path, explicit)
}

func configPathFromCommand(cmd *cli.Command) (string, bool) {
	root := cmd.Root()
	if root.IsSet("config") {
		return root.String("config"), true
	}
	if envPath := strings.TrimSpace(os.Getenv(configEnvVar)); envPath != "" {
		return envPath, true
	}
	return root.String("config"), false
}

func configSourceExplicitlyProvided(cmd *cli.Command) bool {
	_, explicit := configPathFromCommand(cmd)
	return explicit
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

func runServe(ctx context.Context, src config.Source) error {
	cfg, database, err := loadConfigAndDB(ctx, src)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	settingsSvc, err := admin.NewSettingsService(cfg, src)
	if err != nil {
		return fmt.Errorf("initializing settings service: %w", err)
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

	// Initialize Filecoin SDK clients.
	client, err := synapse.NewClient(ctx, synapse.ClientConfig{
		PrivateKey:           cfg.Filecoin.PrivateKey,
		RPCURL:               cfg.Filecoin.RPCURL,
		WithCDN:              cfg.Filecoin.WithCDN,
		AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
		Logger:               logger,
	})
	if err != nil {
		return fmt.Errorf("initializing Filecoin SDK: %w", err)
	}
	defer func() { _ = client.Close() }()
	resolvedAddresses := client.ResolvedAddresses()
	storageClient := synapse.AdaptStorageService(client.Storage())
	walletQuerier := synapse.NewWalletQuerier(client.Payments(), client.Address(), client.Chain(), resolvedAddresses)
	walletOperator := synapse.NewWalletOperator(client.Payments(), resolvedAddresses.USDFC)
	filecoinReadiness := synapse.NewReadinessChecker(
		synapse.ReadinessConfigFromFilecoinConfig(cfg.Filecoin),
		synapse.AdaptReadinessClient(client),
		synapse.WithReadinessLogger(logger),
	)
	observabilityChecker := newObservabilityChecker(cfg, client, logger)
	walletReceiptClient, err := ethclient.DialContext(ctx, cfg.Filecoin.RPCURL)
	if err != nil {
		return fmt.Errorf("creating wallet receipt client: %w", err)
	}
	defer walletReceiptClient.Close()

	runtime, err := app.NewRuntime(ctx, app.RuntimeOptions{
		Config:   cfg,
		Database: database,
		Settings: settingsSvc,
		Filecoin: app.FilecoinServices{
			Storage:       storageClient,
			WalletQuery:   walletQuerier,
			Wallet:        walletOperator,
			Receipts:      walletReceiptClient,
			Readiness:     filecoinReadiness,
			Observability: observabilityChecker,
		},
		ProviderIdentity: admin.NewProviderIdentityResolver(client.SPRegistry(), cfg.Filecoin.RPCURL, logger),
		Logger:           logger,
	})
	if err != nil {
		return fmt.Errorf("initializing application runtime: %w", err)
	}
	return runtime.Run(ctx)
}

func runSetupMode(ctx context.Context, cfg *config.Config, settingsSvc *admin.SettingsService, fieldErrs []config.FieldError) error {
	logger := setupLogger(cfg.Logging)
	slog.SetDefault(logger)

	logger.Warn("starting SynapS3 admin setup mode", "validation_errors", len(fieldErrs), "admin_addr", cfg.Admin.Addr)

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	filecoinReadiness := synapse.NewReadinessChecker(
		synapse.ReadinessConfigFromFilecoinConfig(cfg.Filecoin),
		nil,
		synapse.WithReadinessLogger(logger),
	)
	adminSrv := admin.NewSetup(cfg.Admin.Addr, settingsSvc, logger).
		WithFilecoinReadiness(filecoinReadiness)
	if err := adminSrv.WithTrustedProxies(cfg.Admin.TrustedProxies); err != nil {
		return fmt.Errorf("initializing admin trusted proxies: %w", err)
	}
	if err := adminSrv.WithAuthConfig(cfg.Admin.Auth); err != nil {
		return fmt.Errorf("initializing admin auth: %w", err)
	}
	if err := adminSrv.Run(ctx); err != nil {
		return fmt.Errorf("admin setup server error: %w", err)
	}
	return nil
}

func newObservabilityChecker(cfg *config.Config, client *sdk.Client, logger *slog.Logger) observability.RefreshChecker {
	providerHealth := provider.NewHealthChecker(nil)
	return observability.NewChecker(observability.CheckerOptions{
		ProviderSource: observability.NewRegistryProviderSource(provider.NewRegistryService(client.SPRegistry())),
		ProviderHealth: providerHealth.Check,
		DataSetScanner: observability.NewStorageDataSetScanner(client.Storage()),
		Timeout:        cfg.Filecoin.Observability.Timeout,
		Concurrency:    cfg.Filecoin.Observability.Concurrency,
		Logger:         logger,
	})
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
		"filecoin.default_copies",
		"filecoin.observability.interval",
		"filecoin.observability.timeout",
		"filecoin.observability.concurrency",
		"worker.upload.concurrency",
		"worker.upload.poll_interval",
		"worker.upload.max_retries",
		"worker.evictor.concurrency",
		"worker.evictor.poll_interval",
		"worker.evictor.max_retries",
		"worker.storage_cleanup.concurrency",
		"worker.storage_cleanup.poll_interval",
		"worker.storage_cleanup.max_retries",
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
