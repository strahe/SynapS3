package task

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/task/binding"
	"github.com/strahe/synaps3/internal/task/bucketprovision"
	cachetask "github.com/strahe/synaps3/internal/task/cache"
	"github.com/strahe/synaps3/internal/task/cleanup"
	"github.com/strahe/synaps3/internal/task/commit"
	"github.com/strahe/synaps3/internal/task/dataset"
	providertask "github.com/strahe/synaps3/internal/task/provider"
	"github.com/strahe/synaps3/internal/task/replacement"
	"github.com/strahe/synaps3/internal/task/transfer"
	"github.com/strahe/synaps3/internal/task/uploadplan"
	"github.com/strahe/synaps3/internal/task/wallet"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type EventPublisher interface {
	Publish(string, map[string]any)
}

type Dependencies struct {
	Repositories           *repository.Repositories
	Events                 EventPublisher
	Cache                  cache.Cache
	CacheGate              *cacheaccess.Gate
	CacheTracker           *cacheaccess.Tracker
	Storage                synapse.StorageClient
	Wallet                 synapse.WalletOperator
	Receipts               wallet.WalletReceiptChecker
	WalletBroadcastTimeout time.Duration
	WalletReceiptTimeout   time.Duration
	Terminator             synapse.ServiceTerminator
	Epochs                 synapse.ChainEpochReader
	Observability          *observability.Service
	UploadSpeedProbe       providerbenchmark.UploadProbe
	UploadConcurrency      int
	ParkedPieces           synapse.ParkedPieceChecker
	CommitNonces           synapse.CommitNonceReader
	EvictionPolicy         cache.EvictionPolicy
	MaxCacheBytes          int64
	MaxWriteBytes          int64 // largest single cache write; 0 means no room is kept for it
	LRUHighPercent         int
	LRULowPercent          int
	AnchorProviderTier     providerselect.Tier
	DefaultCopies          int

	// CommitMaxPieces, CommitMaxWait and CommitMaxBacklog shape how transferred
	// copies of one data set are registered together.
	CommitMaxPieces           int
	CommitMaxWait             time.Duration
	CommitMaxBacklog          int
	CommitSealOnCachePressure bool
	// LegacyPieceStorageIDLimit is the first data set ID of the deployment's
	// compact piece layout; zero applies no legacy piece limit.
	LegacyPieceStorageIDLimit uint64
	Logger                    *slog.Logger
}

func normalizeDependencies(deps Dependencies) (Dependencies, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return deps, errors.New("task handlers require repositories")
	}
	if deps.WalletBroadcastTimeout < 0 || deps.WalletReceiptTimeout < 0 {
		return deps, errors.New("wallet timeouts cannot be negative")
	}
	if deps.WalletBroadcastTimeout == 0 {
		deps.WalletBroadcastTimeout = 2 * time.Minute
	}
	if deps.WalletReceiptTimeout == 0 {
		deps.WalletReceiptTimeout = 15 * time.Second
	}
	if deps.EvictionPolicy == cache.EvictionPolicyLRU &&
		(deps.MaxCacheBytes <= 0 || deps.LRULowPercent < 0 || deps.LRULowPercent > 100 ||
			deps.LRUHighPercent < 0 || deps.LRUHighPercent > 100 || deps.LRUHighPercent <= deps.LRULowPercent) {
		return deps, errors.New("LRU cache capacity requires valid size and watermarks")
	}
	if deps.CommitMaxPieces == 0 {
		deps.CommitMaxPieces = config.DefaultCommitMaxPieces
	}
	if deps.CommitMaxBacklog == 0 {
		deps.CommitMaxBacklog = config.DefaultCommitMaxBacklog
	}
	if deps.CommitMaxPieces < 1 || deps.CommitMaxWait < 0 || deps.CommitMaxBacklog < deps.CommitMaxPieces {
		return deps, errors.New("batch limits are invalid")
	}
	if deps.AnchorProviderTier == "" {
		deps.AnchorProviderTier = providerselect.TierApproved
	}
	if !deps.AnchorProviderTier.Valid() {
		return deps, errors.New("invalid provider tier")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return deps, nil
}

type EntryPoints struct {
	AdminReplacement     *taskengine.Messenger
	AdminCopyRetry       *taskengine.Messenger
	BackendCacheEviction *taskengine.Messenger
}

func Register(registry *taskengine.Registry, service *taskengine.Service, deps Dependencies) (EntryPoints, error) {
	var points EntryPoints
	if registry == nil || service == nil {
		return points, errors.New("task registry and service are required")
	}
	deps, err := normalizeDependencies(deps)
	if err != nil {
		return points, err
	}
	transferScheduler, err := service.Scheduler("transfer", model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
	if err != nil {
		return points, fmt.Errorf("creating transfer scheduler: %w", err)
	}
	commitScheduler, err := service.Scheduler("commit", model.TaskTypeStorageCommit, model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
	if err != nil {
		return points, fmt.Errorf("creating commit scheduler: %w", err)
	}
	cacheScheduler, err := service.Scheduler("cache", model.TaskTypeCacheCapacityReconcile, model.TaskTypeCacheEvict, model.TaskTypeCacheReconcileDurability)
	if err != nil {
		return points, fmt.Errorf("creating cache scheduler: %w", err)
	}
	providerScheduler, err := service.Scheduler("provider", model.TaskTypeObservabilityRefresh, model.TaskTypeApprovedProviderRefresh, model.TaskTypeEndorsedProviderRefresh, model.TaskTypeProviderUploadSpeedTest)
	if err != nil {
		return points, fmt.Errorf("creating provider scheduler: %w", err)
	}
	datasetScheduler, err := service.Scheduler("dataset", model.TaskTypeStorageDataSetEnsure, model.TaskTypeStorageDataSetRetire)
	if err != nil {
		return points, fmt.Errorf("creating dataset scheduler: %w", err)
	}
	bucketScheduler, err := service.Scheduler("bucket", model.TaskTypeBucketProvision)
	if err != nil {
		return points, fmt.Errorf("creating bucket scheduler: %w", err)
	}
	uploadScheduler, err := service.Scheduler("upload", model.TaskTypeUploadPlan)
	if err != nil {
		return points, fmt.Errorf("creating upload scheduler: %w", err)
	}
	replacementScheduler, err := service.Scheduler("replacement", model.TaskTypeStorageDataSetEnsure, model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull, model.TaskTypeStorageCommit)
	if err != nil {
		return points, fmt.Errorf("creating replacement scheduler: %w", err)
	}
	transferMessages, err := registry.Messenger("transfer", []string{storagepipeline.MessageJoinCommit}, []string{})
	if err != nil {
		return points, fmt.Errorf("creating transfer messenger: %w", err)
	}
	commitMessages, err := registry.Messenger("commit", []string{storagepipeline.MessageStartCopyTransfer, storagepipeline.MessageFailCopy}, []string{storagepipeline.MessageContentCommitted, storagepipeline.MessageContentDurabilityReached})
	if err != nil {
		return points, fmt.Errorf("creating commit messenger: %w", err)
	}
	datasetMessages, err := registry.Messenger("dataset", []string{}, []string{storagepipeline.MessageDataSetReady})
	if err != nil {
		return points, fmt.Errorf("creating dataset messenger: %w", err)
	}
	bucketMessages, err := registry.Messenger("bucket", []string{storagepipeline.MessageEnsureDataSet}, []string{bucketlifecycle.MessageBucketReady})
	if err != nil {
		return points, fmt.Errorf("creating bucket messenger: %w", err)
	}
	uploadMessages, err := registry.Messenger("upload", []string{storagepipeline.MessageEnsureDataSet, storagepipeline.MessageStartCopyTransfer}, []string{})
	if err != nil {
		return points, fmt.Errorf("creating upload messenger: %w", err)
	}
	replacementMessages, err := registry.Messenger("replacement", []string{storagepipeline.MessageEnsureDataSet, storagepipeline.MessageStartCopyTransfer, storagepipeline.MessageJoinCommit, storagereplacement.MessageRetireDataSet}, []string{storagereplacement.MessageReplacementActivated})
	if err != nil {
		return points, fmt.Errorf("creating replacement messenger: %w", err)
	}
	adminReplacementMessages, err := registry.Messenger("adminReplacement", []string{storagepipeline.MessageEnsureDataSet}, []string{storagepipeline.MessageDataSetReady})
	if err != nil {
		return points, fmt.Errorf("creating adminReplacement messenger: %w", err)
	}
	adminCopyMessages, err := registry.Messenger("adminCopy", []string{storagepipeline.MessageStartCopyTransfer}, []string{})
	if err != nil {
		return points, fmt.Errorf("creating adminCopy messenger: %w", err)
	}
	backendMessages, err := registry.Messenger("backend", []string{storagepipeline.MessageEvictCacheContent}, []string{})
	if err != nil {
		return points, fmt.Errorf("creating backend messenger: %w", err)
	}
	points = EntryPoints{AdminReplacement: adminReplacementMessages, AdminCopyRetry: adminCopyMessages, BackendCacheEviction: backendMessages}
	state := cachetask.NewState()
	resolver := binding.NewResolver(binding.ResolverDependencies{Repositories: deps.Repositories, Storage: deps.Storage})
	selector, err := binding.NewSelector(binding.SelectorDependencies{Repositories: deps.Repositories, Storage: deps.Storage, Observability: deps.Observability, AnchorProviderTier: deps.AnchorProviderTier, Resolver: resolver})
	if err != nil {
		return points, fmt.Errorf("creating binding selector: %w", err)
	}
	copies, err := transfer.NewCopyCoordinator(transfer.CoordinatorDependencies{CacheGate: deps.CacheGate, Repositories: deps.Repositories, Scheduler: transferScheduler, Messenger: transferMessages})
	if err != nil {
		return points, fmt.Errorf("creating copy coordinator: %w", err)
	}
	commitHandler, err := commit.NewHandler(commit.Dependencies{Repositories: deps.Repositories, CommitNonces: deps.CommitNonces, ParkedPieces: deps.ParkedPieces, Cache: deps.Cache, EvictionPolicy: deps.EvictionPolicy, CommitSealOnCachePressure: deps.CommitSealOnCachePressure, CommitMaxPieces: deps.CommitMaxPieces, CommitMaxWait: deps.CommitMaxWait, LegacyPieceStorageIDLimit: deps.LegacyPieceStorageIDLimit, Logger: deps.Logger, Scheduler: commitScheduler, RetryPolicy: service, Messenger: commitMessages, Resolver: resolver, Pressure: state})
	if err != nil {
		return points, fmt.Errorf("constructing commitHandler: %w", err)
	}
	capacity, err := cachetask.NewCapacityHandler(cachetask.CapacityDependencies{Repositories: deps.Repositories, Cache: deps.Cache, CacheTracker: deps.CacheTracker, EvictionPolicy: deps.EvictionPolicy, MaxCacheBytes: deps.MaxCacheBytes, MaxWriteBytes: deps.MaxWriteBytes, LRUHighPercent: deps.LRUHighPercent, LRULowPercent: deps.LRULowPercent, CommitSealOnCachePressure: deps.CommitSealOnCachePressure, Scheduler: cacheScheduler, State: state, PressureListener: commitHandler})
	if err != nil {
		return points, fmt.Errorf("constructing capacity: %w", err)
	}
	evict, err := cachetask.NewEvictHandler(cachetask.EvictDependencies{Repositories: deps.Repositories, Cache: deps.Cache, CacheGate: deps.CacheGate, CacheTracker: deps.CacheTracker, EvictionPolicy: deps.EvictionPolicy, MaxCacheBytes: deps.MaxCacheBytes, MaxWriteBytes: deps.MaxWriteBytes, LRULowPercent: deps.LRULowPercent, State: state})
	if err != nil {
		return points, fmt.Errorf("constructing evict: %w", err)
	}
	durability, err := cachetask.NewDurabilityHandler(cachetask.DurabilityDependencies{Repositories: deps.Repositories, EvictionPolicy: deps.EvictionPolicy, Scheduler: cacheScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing durability: %w", err)
	}
	plan, err := transfer.NewPlanHandler(transfer.PlanDependencies{Cache: deps.Cache, Coordinator: copies})
	if err != nil {
		return points, fmt.Errorf("constructing plan: %w", err)
	}
	store, err := transfer.NewStoreHandler(transfer.StoreDependencies{Cache: deps.Cache, CacheGate: deps.CacheGate, Events: deps.Events, ParkedPieces: deps.ParkedPieces, CommitMaxBacklog: deps.CommitMaxBacklog, UploadConcurrency: deps.UploadConcurrency, Logger: deps.Logger, Coordinator: copies, Resolver: resolver})
	if err != nil {
		return points, fmt.Errorf("constructing store: %w", err)
	}
	pull, err := transfer.NewPullHandler(transfer.PullDependencies{Cache: deps.Cache, ParkedPieces: deps.ParkedPieces, CommitMaxBacklog: deps.CommitMaxBacklog, Coordinator: copies, Resolver: resolver})
	if err != nil {
		return points, fmt.Errorf("constructing pull: %w", err)
	}
	ensure, err := dataset.NewEnsureHandler(dataset.EnsureDependencies{Repositories: deps.Repositories, Storage: deps.Storage, Logger: deps.Logger, Messenger: datasetMessages})
	if err != nil {
		return points, fmt.Errorf("constructing ensure: %w", err)
	}
	retire, err := dataset.NewRetireHandler(dataset.RetireDependencies{Repositories: deps.Repositories, Terminator: deps.Terminator, Epochs: deps.Epochs, Logger: deps.Logger})
	if err != nil {
		return points, fmt.Errorf("constructing retire: %w", err)
	}
	bucketHandler, err := bucketprovision.NewHandler(bucketprovision.Dependencies{Repositories: deps.Repositories, Storage: deps.Storage, DefaultCopies: deps.DefaultCopies, Logger: deps.Logger, Selector: selector, Messenger: bucketMessages})
	if err != nil {
		return points, fmt.Errorf("constructing bucketHandler: %w", err)
	}
	uploadHandler, err := uploadplan.NewHandler(uploadplan.Dependencies{Repositories: deps.Repositories, Storage: deps.Storage, Observability: deps.Observability, Logger: deps.Logger, Selector: selector, Messenger: uploadMessages})
	if err != nil {
		return points, fmt.Errorf("constructing uploadHandler: %w", err)
	}
	replacementHandler, err := replacement.NewHandler(replacement.Dependencies{Repositories: deps.Repositories, Messenger: replacementMessages, RetryPolicy: service, Scheduler: replacementScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing replacementHandler: %w", err)
	}
	cleanupHandler, err := cleanup.NewHandler(cleanup.Dependencies{Repositories: deps.Repositories, Storage: deps.Storage, Receipts: deps.Receipts, WalletReceiptTimeout: deps.WalletReceiptTimeout, Cache: deps.Cache, CacheGate: deps.CacheGate, CacheTracker: deps.CacheTracker, Logger: deps.Logger})
	if err != nil {
		return points, fmt.Errorf("constructing cleanupHandler: %w", err)
	}
	walletHandler, err := wallet.NewHandler(wallet.Dependencies{Repositories: deps.Repositories, Wallet: deps.Wallet, Receipts: deps.Receipts, WalletBroadcastTimeout: deps.WalletBroadcastTimeout, WalletReceiptTimeout: deps.WalletReceiptTimeout})
	if err != nil {
		return points, fmt.Errorf("constructing walletHandler: %w", err)
	}
	observabilityHandler, err := providertask.NewObservabilityHandler(providertask.ObservabilityDependencies{Repositories: deps.Repositories, Observability: deps.Observability, UploadSpeedProbe: deps.UploadSpeedProbe, Events: deps.Events, Scheduler: providerScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing observabilityHandler: %w", err)
	}
	approved, err := providertask.NewApprovedRefreshHandler(providertask.TierRefreshDependencies{Observability: deps.Observability, Events: deps.Events})
	if err != nil {
		return points, fmt.Errorf("constructing approved: %w", err)
	}
	endorsed, err := providertask.NewEndorsedRefreshHandler(providertask.TierRefreshDependencies{Observability: deps.Observability, Events: deps.Events})
	if err != nil {
		return points, fmt.Errorf("constructing endorsed: %w", err)
	}
	speed, err := providertask.NewUploadSpeedTestHandler(providertask.UploadSpeedTestDependencies{Observability: deps.Observability, UploadSpeedProbe: deps.UploadSpeedProbe, Logger: deps.Logger})
	if err != nil {
		return points, fmt.Errorf("constructing speed: %w", err)
	}
	for _, handler := range []taskengine.Handler{commitHandler, capacity, evict, durability, plan, store, pull, ensure, retire, bucketHandler, uploadHandler, replacementHandler, cleanupHandler, walletHandler, observabilityHandler, approved, endorsed, speed} {
		if err := registry.Register(handler); err != nil {
			return points, err
		}
	}
	callbackOwnHandover0, err := dataset.NewEnsureDataSetReceiver(dataset.EnsureReceiverDependencies{Scheduler: datasetScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing dataset.ensure: %w", err)
	}
	if err := registry.OwnHandover("dataset.ensure", storagepipeline.MessageEnsureDataSet, callbackOwnHandover0); err != nil {
		return points, err
	}
	callbackOwnHandover1, err := dataset.NewRetireDataSetReceiver(dataset.RetireReceiverDependencies{Scheduler: datasetScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing dataset.retire: %w", err)
	}
	if err := registry.OwnHandover("dataset.retire", storagereplacement.MessageRetireDataSet, callbackOwnHandover1); err != nil {
		return points, err
	}
	callbackOwnHandover2, err := transfer.NewStartCopyTransferReceiver(copies)
	if err != nil {
		return points, fmt.Errorf("constructing transfer.start: %w", err)
	}
	if err := registry.OwnHandover("transfer.start", storagepipeline.MessageStartCopyTransfer, callbackOwnHandover2); err != nil {
		return points, err
	}
	callbackOwnHandover3, err := transfer.NewFailCopyReceiver(copies)
	if err != nil {
		return points, fmt.Errorf("constructing transfer.fail: %w", err)
	}
	if err := registry.OwnHandover("transfer.fail", storagepipeline.MessageFailCopy, callbackOwnHandover3); err != nil {
		return points, err
	}
	callbackOwnHandover4, err := commit.NewJoinCommitReceiver(commitHandler)
	if err != nil {
		return points, fmt.Errorf("constructing commit.join: %w", err)
	}
	if err := registry.OwnHandover("commit.join", storagepipeline.MessageJoinCommit, callbackOwnHandover4); err != nil {
		return points, err
	}
	callbackOwnHandover5, err := cachetask.NewEvictCacheContentReceiver(cachetask.EvictionDependencies{Scheduler: cacheScheduler, EvictionPolicy: deps.EvictionPolicy})
	if err != nil {
		return points, fmt.Errorf("constructing cache.evict: %w", err)
	}
	if err := registry.OwnHandover("cache.evict", storagepipeline.MessageEvictCacheContent, callbackOwnHandover5); err != nil {
		return points, err
	}
	callbackSubscribe0, err := transfer.NewDataSetReadySubscriber(copies)
	if err != nil {
		return points, fmt.Errorf("constructing 10.transfer.dataset-ready: %w", err)
	}
	if err := registry.Subscribe("10.transfer.dataset-ready", storagepipeline.MessageDataSetReady, callbackSubscribe0); err != nil {
		return points, err
	}
	callbackSubscribe1, err := commit.NewDataSetReadySubscriber(commitHandler)
	if err != nil {
		return points, fmt.Errorf("constructing 20.commit.dataset-ready: %w", err)
	}
	if err := registry.Subscribe("20.commit.dataset-ready", storagepipeline.MessageDataSetReady, callbackSubscribe1); err != nil {
		return points, err
	}
	callbackSubscribe2, err := bucketprovision.NewDataSetReadyPromoteSubscriber(bucketprovision.ReadinessDependencies{Messenger: bucketMessages})
	if err != nil {
		return points, fmt.Errorf("constructing 30.bucketprovision.dataset-ready-promote: %w", err)
	}
	if err := registry.Subscribe("30.bucketprovision.dataset-ready-promote", storagepipeline.MessageDataSetReady, callbackSubscribe2); err != nil {
		return points, err
	}
	callbackSubscribe3, err := uploadplan.NewDataSetReadySubscriber(uploadplan.WakeDependencies{Scheduler: uploadScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing 40.uploadplan.dataset-ready: %w", err)
	}
	if err := registry.Subscribe("40.uploadplan.dataset-ready", storagepipeline.MessageDataSetReady, callbackSubscribe3); err != nil {
		return points, err
	}
	callbackSubscribe4, err := bucketprovision.NewDataSetReadyWakeSubscriber(bucketprovision.WakeDependencies{Scheduler: bucketScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing 50.bucketprovision.dataset-ready-wake: %w", err)
	}
	if err := registry.Subscribe("50.bucketprovision.dataset-ready-wake", storagepipeline.MessageDataSetReady, callbackSubscribe4); err != nil {
		return points, err
	}
	callbackSubscribe5, err := bucketprovision.NewReplacementActivatedPromoteSubscriber(bucketprovision.ReadinessDependencies{Messenger: bucketMessages})
	if err != nil {
		return points, fmt.Errorf("constructing 30.bucketprovision.replacement-promote: %w", err)
	}
	if err := registry.Subscribe("30.bucketprovision.replacement-promote", storagereplacement.MessageReplacementActivated, callbackSubscribe5); err != nil {
		return points, err
	}
	callbackSubscribe6, err := uploadplan.NewReplacementActivatedSubscriber(uploadplan.WakeDependencies{Scheduler: uploadScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing 40.uploadplan.replacement: %w", err)
	}
	if err := registry.Subscribe("40.uploadplan.replacement", storagereplacement.MessageReplacementActivated, callbackSubscribe6); err != nil {
		return points, err
	}
	callbackSubscribe7, err := bucketprovision.NewReplacementActivatedWakeSubscriber(bucketprovision.WakeDependencies{Scheduler: bucketScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing 50.bucketprovision.replacement-wake: %w", err)
	}
	if err := registry.Subscribe("50.bucketprovision.replacement-wake", storagereplacement.MessageReplacementActivated, callbackSubscribe7); err != nil {
		return points, err
	}
	callbackSubscribe8, err := providertask.NewBucketReadySubscriber(providertask.BucketReadyDependencies{Scheduler: providerScheduler})
	if err != nil {
		return points, fmt.Errorf("constructing provider.bucket-ready: %w", err)
	}
	if err := registry.Subscribe("provider.bucket-ready", bucketlifecycle.MessageBucketReady, callbackSubscribe8); err != nil {
		return points, err
	}
	callbackSubscribe9, err := transfer.NewContentCommittedSubscriber(copies)
	if err != nil {
		return points, fmt.Errorf("constructing transfer.content-committed: %w", err)
	}
	if err := registry.Subscribe("transfer.content-committed", storagepipeline.MessageContentCommitted, callbackSubscribe9); err != nil {
		return points, err
	}
	callbackSubscribe10, err := cachetask.NewContentDurabilityReachedSubscriber(cachetask.EvictionDependencies{Scheduler: cacheScheduler, EvictionPolicy: deps.EvictionPolicy})
	if err != nil {
		return points, fmt.Errorf("constructing cache.content-durability: %w", err)
	}
	if err := registry.Subscribe("cache.content-durability", storagepipeline.MessageContentDurabilityReached, callbackSubscribe10); err != nil {
		return points, err
	}
	return points, nil
}
