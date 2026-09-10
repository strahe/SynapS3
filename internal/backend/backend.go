package backend

import (
	"log/slog"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/versity/versitygw/backend"
)

// SynapseBackend implements the VersityGW backend.Backend interface,
// bridging S3 operations to Filecoin via the Synapse SDK.
type SynapseBackend struct {
	backend.BackendUnsupported // provides ErrNotImplemented for unimplemented methods

	repos              *repository.Repositories
	cache              cache.Cache
	objectReader       *objectreader.Reader
	cacheGate          *cacheaccess.Gate
	cacheAccessTracker *cacheaccess.Tracker
	bucketLifecycle    *bucketlifecycle.Service
	storage            synapse.StorageClient
	taskService        *taskengine.Service
	evictionPolicy     cache.EvictionPolicy
	defaultCopies      int
	logger             *slog.Logger
}

// Option configures SynapseBackend runtime behavior.
type Option func(*SynapseBackend)

// WithTaskService configures the sole task creation boundary.
func WithTaskService(service *taskengine.Service) Option {
	return func(b *SynapseBackend) {
		b.taskService = service
	}
}

// WithDefaultCopies sets the replica target recorded on content the first time
// its bytes are written. A bucket override still wins.
func WithDefaultCopies(copies int) Option {
	return func(b *SynapseBackend) {
		b.defaultCopies = copies
	}
}

// WithEvictionPolicy configures automatic local cache eviction.
func WithEvictionPolicy(policy cache.EvictionPolicy) Option {
	return func(b *SynapseBackend) {
		b.evictionPolicy = policy
	}
}

// New creates a new SynapseBackend.
func New(
	repos *repository.Repositories,
	c cache.Cache,
	sc synapse.StorageClient,
	cacheGate *cacheaccess.Gate,
	cacheAccessTracker *cacheaccess.Tracker,
	logger *slog.Logger,
	opts ...Option,
) *SynapseBackend {
	if cacheGate == nil {
		panic("backend requires a cache access gate")
	}
	if cacheAccessTracker == nil {
		panic("backend requires a cache access tracker")
	}
	b := &SynapseBackend{
		repos:              repos,
		cache:              c,
		cacheGate:          cacheGate,
		cacheAccessTracker: cacheAccessTracker,
		storage:            sc,
		evictionPolicy:     cache.EvictionPolicyNone,
		logger:             logger,
	}
	for _, opt := range opts {
		opt(b)
	}
	b.bucketLifecycle = bucketlifecycle.New(repos, c, b.defaultCopies, logger)
	b.bucketLifecycle.SetTaskService(b.taskService)
	b.objectReader = objectreader.New(
		repos,
		c,
		sc,
		cacheGate,
		cacheAccessTracker,
		logger,
	)
	return b
}

func (b *SynapseBackend) String() string {
	return "SynapS3/Filecoin"
}

func (b *SynapseBackend) Shutdown() {
	b.logger.Info("shutting down SynapS3 backend")
}
