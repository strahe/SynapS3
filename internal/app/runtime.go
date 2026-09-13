// Package app owns the production application composition and lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/s3access"
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3log"
	"golang.org/x/sync/errgroup"
)

const (
	defaultS3MultipartMaxParts = 10000
	defaultShutdownTimeout     = 10 * time.Second
)

// ReadinessProbe provides runtime and draft Filecoin readiness checks.
type ReadinessProbe interface {
	CheckRuntime(context.Context) synapse.ReadinessResult
	CheckDraft(context.Context, synapse.ReadinessConfig) synapse.ReadinessResult
}

// FilecoinServices contains the externally owned Filecoin integrations used by
// the application runtime.
type FilecoinServices struct {
	Storage       synapse.StorageClient
	WalletQuery   synapse.WalletQuerier
	Wallet        synapse.WalletOperator
	Receipts      worker.WalletReceiptChecker
	Readiness     ReadinessProbe
	Observability observability.RefreshChecker
	// Terminator ends a replaced storage service. Epochs observes the chain so
	// the gateway can tell an accepted termination from a completed one.
	Terminator synapse.ServiceTerminator
	Epochs     synapse.ChainEpochReader
}

// RuntimeOptions configures the application composition root. Database,
// settings, and Filecoin services remain owned by the caller.
type RuntimeOptions struct {
	Config           *config.Config
	Database         *bun.DB
	Settings         *admin.SettingsService
	Filecoin         FilecoinServices
	ProviderIdentity *admin.ProviderIdentityResolver
	S3Addresses      []string
	Logger           *slog.Logger
	ShutdownTimeout  time.Duration
}

// Runtime owns all resources assembled by NewRuntime.
type Runtime struct {
	adminListener net.Listener
	adminServer   *admin.Server
	s3Server      *s3api.S3ApiServer
	backend       *backend.SynapseBackend
	cacheGate     *cacheaccess.Gate
	accessTracker *cacheaccess.Tracker
	iam           auth.IAMService
	workers       *worker.Manager
	s3Addresses   []string
	logger        *slog.Logger
	shutdown      time.Duration

	mu      sync.Mutex
	running bool
	closed  bool
}

// NewRuntime validates dependencies and assembles the complete application.
func NewRuntime(ctx context.Context, opts RuntimeOptions) (_ *Runtime, err error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	cfg := opts.Config
	logger := opts.Logger
	repos := repository.NewRepositories(opts.Database)
	maxCacheBytes := int64(cfg.Cache.MaxSizeGB) * 1024 * 1024 * 1024
	localCache, err := cache.NewFilesystem(cfg.Cache.Dir, maxCacheBytes)
	if err != nil {
		return nil, fmt.Errorf("initializing cache: %w", err)
	}

	evictionPolicy, ok := cache.ParseEvictionPolicy(cfg.Cache.EvictionPolicy)
	if !ok {
		return nil, fmt.Errorf("initializing cache eviction policy: unsupported value %q", cfg.Cache.EvictionPolicy)
	}
	cacheGate := cacheaccess.NewGate()
	accessTracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
	events := admin.NewEventHub()
	observabilityService := newObservabilityService(cfg, repos, opts.Filecoin.Observability)
	pdpStatusChecker := synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{
		Timeout:              15 * time.Second,
		AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
	})
	registry := taskengine.NewRegistry()
	handlers, err := worker.NewTaskHandlers(worker.TaskHandlerDependencies{
		Repositories:   repos,
		Events:         events,
		Cache:          localCache,
		CacheGate:      cacheGate,
		CacheTracker:   accessTracker,
		Storage:        opts.Filecoin.Storage,
		Wallet:         opts.Filecoin.Wallet,
		Receipts:       opts.Filecoin.Receipts,
		Terminator:     opts.Filecoin.Terminator,
		Epochs:         opts.Filecoin.Epochs,
		Observability:  observabilityService,
		CommitStatus:   pdpStatusChecker,
		ParkedPieces:   pdpStatusChecker,
		EvictionPolicy: evictionPolicy,
		MaxCacheBytes:  maxCacheBytes,
		LRUHighPercent: cfg.Cache.LRUHighWatermarkPercent,
		LRULowPercent:  cfg.Cache.LRULowWatermarkPercent,
		DefaultCopies:  cfg.Filecoin.DefaultCopies,
		MaxRetries:     cfg.Worker.Tasks.MaxRetries,
		Logger:         logger,
	})
	if err != nil {
		return nil, fmt.Errorf("initializing task handlers: %w", err)
	}
	if err := handlers.RegisterCore(registry); err != nil {
		return nil, fmt.Errorf("registering core task handlers: %w", err)
	}
	if err := handlers.RegisterStorage(registry); err != nil {
		return nil, fmt.Errorf("registering storage task handlers: %w", err)
	}
	if err := handlers.RegisterReplacement(registry); err != nil {
		return nil, fmt.Errorf("registering replacement task handlers: %w", err)
	}
	taskService, err := taskengine.NewService(registry, repos, cfg.Worker.Tasks.Retention)
	if err != nil {
		return nil, fmt.Errorf("initializing task service: %w", err)
	}
	handlers.SetTaskService(taskService)
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency:                    cfg.Worker.Tasks.Concurrency,
		PollInterval:                   cfg.Worker.Tasks.PollInterval,
		LeaseDuration:                  cfg.Worker.Tasks.LeaseDuration,
		Retention:                      cfg.Worker.Tasks.Retention,
		ProviderMutationConcurrency:    cfg.Worker.Tasks.ProviderMutationConcurrency,
		DestructiveMutationConcurrency: cfg.Worker.Tasks.DestructiveMutationConcurrency,
		OnTaskSettled: func(taskRow *model.Task, transition repository.TaskTransition) {
			publishUploadTaskSettlement(events, taskRow, transition)
		},
	}, repos, registry, logger)
	if err != nil {
		return nil, fmt.Errorf("initializing task engine: %w", err)
	}
	for _, recurring := range []taskengine.EnqueueRequest{
		{
			Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey,
			Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity",
		},
		{
			Type: model.TaskTypeObservabilityRefresh, IdempotencyKey: "system:observability-refresh",
			Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "observability",
		},
		{
			Type: model.TaskTypeGC, IdempotencyKey: "system:task-gc",
			Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "task-gc",
		},
	} {
		if _, _, err := taskService.Enqueue(ctx, recurring); err != nil {
			return nil, fmt.Errorf("seeding recurring task %s: %w", recurring.Type, err)
		}
	}
	appBackend := backend.New(repos, localCache, opts.Filecoin.Storage, cacheGate, accessTracker, logger,
		backend.WithTaskService(taskService),
		backend.WithEvictionPolicy(evictionPolicy),
		backend.WithDefaultCopies(cfg.Filecoin.DefaultCopies),
	)

	iamService := s3iam.NewService(repos)
	rootAccount, err := iamService.EnsureRootAccount(ctx)
	if err != nil {
		return nil, fmt.Errorf("initializing S3 IAM: %w", err)
	}
	cleanup := func() {
		appBackend.Shutdown()
		_ = iamService.Shutdown()
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	s3Options, err := s3ServerOptions(cfg.Server)
	if err != nil {
		return nil, err
	}
	s3Server, err := s3api.New(
		appBackend,
		middlewares.RootUserConfig{Access: rootAccount.Access, Secret: rootAccount.Secret},
		cfg.S3.Region,
		iamService,
		s3AccessLogger(logger, cfg.Logging.S3Access),
		nil,
		nil,
		nil,
		s3Options...,
	)
	if err != nil {
		return nil, fmt.Errorf("creating S3 server: %w", err)
	}

	manager := worker.NewManager(engine, logger)

	adminServer := admin.New(cfg.Admin.Addr, opts.Database, localCache, cacheGate, accessTracker, maxCacheBytes, repos, manager,
		opts.Filecoin.WalletQuery, cfg.Filecoin.DefaultCopies, logger).
		WithEventHub(events).
		WithObjectUploader(appBackend).
		WithObjectVersionRestorer(appBackend).
		WithObjectStorage(opts.Filecoin.Storage).
		WithSettings(opts.Settings).
		WithFilecoinReadiness(opts.Filecoin.Readiness).
		WithObservability(observabilityService).
		WithTaskService(taskService).
		WithS3IAM(iamService, rootAccount.Access)
	if opts.ProviderIdentity != nil {
		adminServer.WithProviderIdentityResolver(opts.ProviderIdentity)
	}
	if observabilityService != nil {
		// Replacement offers the same providers the storage topology reports,
		// which is the same service that reports them.
		adminServer.WithProviderReplacement(admin.NewStorageProviderSelector(observabilityService))
	}
	if err := adminServer.WithTrustedProxies(cfg.Admin.TrustedProxies); err != nil {
		return nil, fmt.Errorf("initializing admin trusted proxies: %w", err)
	}
	if err := adminServer.WithAuthConfig(cfg.Admin.Auth); err != nil {
		return nil, fmt.Errorf("initializing admin auth: %w", err)
	}

	adminListener, err := net.Listen("tcp", cfg.Admin.Addr)
	if err != nil {
		return nil, fmt.Errorf("listening for admin server on %s: %w", cfg.Admin.Addr, err)
	}
	shutdownTimeout := opts.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	s3Addresses := append([]string(nil), opts.S3Addresses...)
	if len(s3Addresses) == 0 {
		s3Addresses = []string{cfg.Server.Port}
	}

	return &Runtime{
		adminListener: adminListener,
		adminServer:   adminServer,
		s3Server:      s3Server,
		backend:       appBackend,
		cacheGate:     cacheGate,
		accessTracker: accessTracker,
		iam:           iamService,
		workers:       manager,
		s3Addresses:   s3Addresses,
		logger:        logger,
		shutdown:      shutdownTimeout,
	}, nil
}

func publishUploadTaskSettlement(events admin.EventPublisher, taskRow *model.Task, transition repository.TaskTransition) {
	if events == nil || taskRow == nil || transition.Status == model.TaskStatusPending || !uploadPipelineTask(taskRow.Type) {
		return
	}
	payload := map[string]any{
		"task_id":   taskRow.ID,
		"task_type": taskRow.Type,
		"status":    transition.Status,
	}
	if taskRow.SubjectType != nil {
		payload["subject_type"] = *taskRow.SubjectType
	}
	if taskRow.SubjectKey != nil {
		payload["subject_key"] = *taskRow.SubjectKey
	}
	events.Publish("upload_state_changed", payload)
}

func uploadPipelineTask(taskType model.TaskType) bool {
	switch taskType {
	case model.TaskTypeUploadPlan,
		model.TaskTypeStorageDataSetEnsure,
		model.TaskTypeStorageTransferPlan,
		model.TaskTypeStorageStore,
		model.TaskTypeStoragePull,
		model.TaskTypeStorageCommitCoordinate,
		model.TaskTypeStorageCommit,
		model.TaskTypeProviderReplacementCoordinate,
		model.TaskTypeStorageDataSetRetire:
		return true
	default:
		return false
	}
}

func validateOptions(opts RuntimeOptions) error {
	var missing []string
	dependencies := []struct {
		name  string
		value any
	}{
		{name: "config", value: opts.Config},
		{name: "database", value: opts.Database},
		{name: "settings", value: opts.Settings},
		{name: "logger", value: opts.Logger},
		{name: "filecoin storage", value: opts.Filecoin.Storage},
		{name: "filecoin wallet query", value: opts.Filecoin.WalletQuery},
		{name: "filecoin wallet operator", value: opts.Filecoin.Wallet},
		{name: "filecoin receipts", value: opts.Filecoin.Receipts},
		{name: "filecoin readiness", value: opts.Filecoin.Readiness},
		{name: "filecoin observability", value: opts.Filecoin.Observability},
		{name: "filecoin service terminator", value: opts.Filecoin.Terminator},
		{name: "filecoin chain epochs", value: opts.Filecoin.Epochs},
	}
	for _, dependency := range dependencies {
		if isNil(dependency.value) {
			missing = append(missing, dependency.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("runtime dependencies are required: %s", strings.Join(missing, ", "))
	}
	if strings.TrimSpace(opts.Config.Server.Port) == "" {
		return errors.New("runtime S3 address is required")
	}
	for _, address := range opts.S3Addresses {
		if strings.TrimSpace(address) == "" {
			return errors.New("runtime S3 addresses must not be empty")
		}
	}
	if strings.TrimSpace(opts.Config.Admin.Addr) == "" {
		return errors.New("runtime admin address is required")
	}
	fieldErrors := opts.Config.FieldValidationErrors()
	if len(fieldErrors) > 0 {
		errorsToJoin := make([]error, 0, len(fieldErrors))
		for _, fieldError := range fieldErrors {
			errorsToJoin = append(errorsToJoin, fieldError)
		}
		return fmt.Errorf("runtime config is invalid: %w", errors.Join(errorsToJoin...))
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// AdminAddr returns the actual bound admin address, including an allocated
// dynamic port when configured with port zero.
func (r *Runtime) AdminAddr() string {
	if r == nil || r.adminListener == nil {
		return ""
	}
	return r.adminListener.Addr().String()
}

// Run serves all components and blocks until ctx is cancelled or a component
// fails. It may be called once.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("runtime is nil")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("runtime is closed")
	}
	if r.running {
		r.mu.Unlock()
		return errors.New("runtime is already running")
	}
	r.running = true
	r.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	group, groupCtx := errgroup.WithContext(runCtx)
	group.Go(func() error {
		r.workers.Start(groupCtx)
		if groupCtx.Err() == nil {
			return errors.New("workers stopped unexpectedly")
		}
		return nil
	})
	group.Go(func() error {
		r.accessTracker.Run(groupCtx, r.cacheGate, r.logger)
		return nil
	})
	group.Go(func() error {
		err := r.adminServer.Serve(groupCtx, r.adminListener)
		if err == nil && groupCtx.Err() == nil {
			return errors.New("admin server stopped unexpectedly")
		}
		if groupCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("admin server: %w", err)
	})
	group.Go(func() error {
		r.logger.Info("S3 server listening", "addresses", r.s3Addresses)
		err := r.s3Server.ServeMultiPort(r.s3Addresses)
		if err == nil && groupCtx.Err() == nil {
			return errors.New("S3 server stopped unexpectedly")
		}
		if groupCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("S3 server: %w", err)
	})

	select {
	case <-ctx.Done():
	case <-groupCtx.Done():
	}
	cancel()

	var shutdownErrors []error
	if err := r.s3Server.ShutDown(); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("shutting down S3 server: %w", err))
	}

	wait := make(chan error, 1)
	go func() { wait <- group.Wait() }()
	timer := time.NewTimer(r.shutdown)
	select {
	case err := <-wait:
		if err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
		shutdownErrors = append(shutdownErrors, fmt.Errorf("runtime shutdown exceeded %s", r.shutdown))
	}

	r.backend.Shutdown()
	if err := r.iam.Shutdown(); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("shutting down S3 IAM: %w", err))
	}
	r.mu.Lock()
	r.running = false
	r.closed = true
	r.mu.Unlock()
	r.logger.Info("SynapS3 stopped")
	return errors.Join(shutdownErrors...)
}

// Close releases a runtime that was constructed but never started. A running
// runtime must be stopped by cancelling the context passed to Run.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return errors.New("cannot close a running runtime; cancel its context")
	}
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	var closeErrors []error
	if err := r.adminListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		closeErrors = append(closeErrors, fmt.Errorf("closing admin listener: %w", err))
	}
	r.backend.Shutdown()
	if err := r.iam.Shutdown(); err != nil {
		closeErrors = append(closeErrors, fmt.Errorf("shutting down S3 IAM: %w", err))
	}
	return errors.Join(closeErrors...)
}

func newObservabilityService(cfg *config.Config, repos *repository.Repositories, checker observability.RefreshChecker) *observability.Service {
	return observability.NewService(observability.ServiceOptions{
		Checker: checker,
		LocalDataSets: observability.LocalDataSetSourceFunc(func(ctx context.Context) ([]observability.LocalDataSet, error) {
			summaries, err := repos.Contents.ListDataSetSummaries(ctx, 0)
			if err != nil {
				return nil, err
			}
			out := make([]observability.LocalDataSet, 0, len(summaries))
			for _, summary := range summaries {
				out = append(out, observability.LocalDataSet{
					ID: summary.ID, BucketID: summary.BucketID, BucketName: summary.BucketName,
					CopyIndex: summary.CopyIndex, ProviderID: summary.ProviderID, DataSetID: summary.DataSetID,
					ClientDataSetID: summary.ClientDataSetID, Status: summary.Status,
				})
			}
			return out, nil
		}),
		LocalDataSetCount: observability.LocalDataSetCountSourceFunc(func(ctx context.Context) (int, error) {
			return repos.Buckets.CountStorageDataSets(ctx)
		}),
		Store:           repos.Observability,
		RefreshInterval: cfg.Filecoin.Observability.Interval,
	})
}

func s3ServerOptions(cfg config.ServerConfig) ([]s3api.Option, error) {
	opts := []s3api.Option{
		s3api.WithHealth("/health"),
		s3api.WithConcurrencyLimiter(cfg.MaxConnections, cfg.MaxRequests),
		s3api.WithMpMaxParts(defaultS3MultipartMaxParts),
		s3api.WithQuiet(),
	}
	if cfg.TLS.Enabled {
		certificates := utils.NewCertStorage()
		if err := certificates.SetCertificate(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil {
			return nil, fmt.Errorf("loading S3 TLS certificate: %w", err)
		}
		opts = append(opts, s3api.WithTLS(certificates))
	}
	return opts, nil
}

func s3AccessLogger(logger *slog.Logger, cfg config.LoggingS3AccessConfig) s3log.AuditLogger {
	if !cfg.Enabled {
		return nil
	}
	return s3access.NewLogger(logger, cfg.Level)
}
