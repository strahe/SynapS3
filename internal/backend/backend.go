package backend

import (
	"log/slog"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/state"
	"github.com/versity/versitygw/backend"
)

// SynapseBackend implements the VersityGW backend.Backend interface,
// bridging S3 operations to Filecoin via the Synapse SDK.
type SynapseBackend struct {
	backend.BackendUnsupported // provides ErrNotImplemented for unimplemented methods

	repos        *repository.Repositories
	cache        cache.Cache
	stateMachine *state.Machine
	logger       *slog.Logger
}

// New creates a new SynapseBackend.
func New(repos *repository.Repositories, c cache.Cache, sm *state.Machine, logger *slog.Logger) *SynapseBackend {
	return &SynapseBackend{
		repos:        repos,
		cache:        c,
		stateMachine: sm,
		logger:       logger,
	}
}

func (b *SynapseBackend) String() string {
	return "SynapS3/Filecoin"
}

func (b *SynapseBackend) Shutdown() {
	b.logger.Info("shutting down SynapS3 backend")
}
