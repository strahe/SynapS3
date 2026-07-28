package cacheaccess

import (
	"context"
	"hash/fnv"
	"io"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/cache"
)

const (
	// DefaultPersistenceInterval bounds foreground cache-access writes while
	// keeping the durable LRU order reasonably current across restarts.
	DefaultPersistenceInterval = time.Minute
	coordinatorShardCount      = 256
)

// PersistFunc records one cache access in durable object metadata.
type PersistFunc func(context.Context, string, time.Time) error

// OpenedCacheEntry is a cache entry opened while serialized with cache deletion.
type OpenedCacheEntry struct {
	Body         io.ReadCloser
	Info         *cache.ObjectInfo
	PersistError error
}

type accessState struct {
	lastAccess  time.Time
	lastAttempt time.Time
}

type coordinatorShard struct {
	mu      sync.Mutex
	entries map[string]accessState
}

// Coordinator serializes foreground cache opens with final cache deletion and
// coalesces repeated durable access-time writes for each object version.
type Coordinator struct {
	persistenceInterval time.Duration
	now                 func() time.Time
	shards              [coordinatorShardCount]coordinatorShard
}

// NewCoordinator creates a shared cache-access coordinator.
func NewCoordinator(persistenceInterval time.Duration) *Coordinator {
	if persistenceInterval < 0 {
		persistenceInterval = 0
	}
	return &Coordinator{
		persistenceInterval: persistenceInterval,
		now:                 time.Now,
	}
}

// Open serializes a successful cache open and its in-memory access record with
// final cache deletion. Durable access metadata is written at most once per
// persistence interval for the same version.
func (c *Coordinator) Open(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	open func() (io.ReadCloser, *cache.ObjectInfo, error),
	persist PersistFunc,
) (*OpenedCacheEntry, error) {
	shard := c.shard(versionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	body, info, err := open()
	if err != nil {
		return nil, err
	}

	state := shard.entries[versionID]
	now := c.now()
	accessedAt := nextAccessTime(now, state.lastAccess, durableAccess)
	state.lastAccess = accessedAt
	var persistErr error
	if persist != nil &&
		(state.lastAttempt.IsZero() ||
			c.persistenceInterval == 0 ||
			now.Sub(state.lastAttempt) >= c.persistenceInterval) {
		persistErr = persist(ctx, versionID, accessedAt)
		state.lastAttempt = now
	}
	if shard.entries == nil {
		shard.entries = make(map[string]accessState)
	}
	shard.entries[versionID] = state

	return &OpenedCacheEntry{
		Body:         body,
		Info:         info,
		PersistError: persistErr,
	}, nil
}

// Record persists an access immediately after a cache entry is committed, and
// records it in memory so an already-planned LRU task cannot remove it.
func (c *Coordinator) Record(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	persist PersistFunc,
) error {
	shard := c.shard(versionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	state := shard.entries[versionID]
	now := c.now()
	accessedAt := nextAccessTime(now, state.lastAccess, durableAccess)
	state.lastAccess = accessedAt
	var err error
	if persist != nil {
		err = persist(ctx, versionID, accessedAt)
		state.lastAttempt = now
	}
	if shard.entries == nil {
		shard.entries = make(map[string]accessState)
	}
	shard.entries[versionID] = state
	return err
}

func nextAccessTime(now, inMemory time.Time, durable *time.Time) time.Time {
	previous := inMemory
	if durable != nil && durable.After(previous) {
		previous = *durable
	}
	if !previous.IsZero() && !now.After(previous) {
		return previous.Add(time.Microsecond)
	}
	return now
}

// GuardDeletion runs the final checks and deletion while cache opens for
// the same version are blocked. Returning true forgets the removed entry.
func (c *Coordinator) GuardDeletion(versionID string, remove func(lastAccess time.Time) bool) {
	shard := c.shard(versionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	lastAccess := shard.entries[versionID].lastAccess
	if remove(lastAccess) {
		delete(shard.entries, versionID)
	}
}

func (c *Coordinator) shard(versionID string) *coordinatorShard {
	if c == nil {
		panic("nil cache access coordinator")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(versionID))
	return &c.shards[h.Sum32()%coordinatorShardCount]
}
