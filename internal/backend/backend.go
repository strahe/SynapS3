package backend

import (
	"log/slog"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/versity/versitygw/backend"
)

// SynapseBackend implements the VersityGW backend.Backend interface,
// bridging S3 operations to Filecoin via the Synapse SDK.
type SynapseBackend struct {
	backend.BackendUnsupported // provides ErrNotImplemented for unimplemented methods

	repos            *repository.Repositories
	cache            cache.Cache
	objectReader     *objectreader.Reader
	bucketLifecycle  *bucketlifecycle.Service
	stateMachine     *state.Machine
	storage          synapse.StorageClient
	uploadMaxRetries int
	evictMaxRetries  int
	autoEvict        bool
	logger           *slog.Logger
}

const (
	defaultUploadMaxRetries = 5
	defaultEvictMaxRetries  = 3
)

// Option configures SynapseBackend runtime behavior.
type Option func(*SynapseBackend)

// WithUploadMaxRetries configures max retries for newly-created upload tasks.
func WithUploadMaxRetries(maxRetries int) Option {
	return func(b *SynapseBackend) {
		b.uploadMaxRetries = maxRetries
	}
}

// WithEvictMaxRetries configures max retries for newly-created cache eviction tasks.
func WithEvictMaxRetries(maxRetries int) Option {
	return func(b *SynapseBackend) {
		b.evictMaxRetries = maxRetries
	}
}

// WithAutoEvict configures whether stored objects enqueue cache eviction tasks.
func WithAutoEvict(autoEvict bool) Option {
	return func(b *SynapseBackend) {
		b.autoEvict = autoEvict
	}
}

// New creates a new SynapseBackend.
func New(repos *repository.Repositories, c cache.Cache, sm *state.Machine, sc synapse.StorageClient, logger *slog.Logger, opts ...Option) *SynapseBackend {
	b := &SynapseBackend{
		repos:            repos,
		cache:            c,
		objectReader:     objectreader.New(repos, c, sc, logger),
		bucketLifecycle:  bucketlifecycle.New(repos, c, logger),
		stateMachine:     sm,
		storage:          sc,
		uploadMaxRetries: defaultUploadMaxRetries,
		evictMaxRetries:  defaultEvictMaxRetries,
		logger:           logger,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *SynapseBackend) String() string {
	return "SynapS3/Filecoin"
}

func (b *SynapseBackend) Shutdown() {
	b.logger.Info("shutting down SynapS3 backend")
}
