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

type accessEntry struct {
	gate    sync.RWMutex
	stateMu sync.Mutex
	state   accessState

	// refs and retired are protected by the owning shard mutex.
	refs    int
	retired bool
}

type coordinatorShard struct {
	mu      sync.Mutex
	entries map[string]*accessEntry
}

// Coordinator coordinates foreground cache opens with final cache deletion for
// the same object version and coalesces repeated durable access-time writes.
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

// Open coordinates a successful cache open and its in-memory access record with
// final cache deletion for the same version. Durable access metadata is written
// at most once per persistence interval for the same version.
func (c *Coordinator) Open(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	open func() (io.ReadCloser, *cache.ObjectInfo, error),
	persist PersistFunc,
) (*OpenedCacheEntry, error) {
	shard, entry := c.acquire(versionID)
	defer c.release(shard, versionID, entry)

	entry.gate.RLock()
	defer entry.gate.RUnlock()

	body, info, err := open()
	if err != nil {
		return nil, err
	}

	persistErr := c.recordAccess(ctx, entry, versionID, durableAccess, persist, false)
	c.markActive(shard, entry)

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
	shard, entry := c.acquire(versionID)
	defer c.release(shard, versionID, entry)

	entry.gate.RLock()
	defer entry.gate.RUnlock()

	err := c.recordAccess(ctx, entry, versionID, durableAccess, persist, true)
	c.markActive(shard, entry)
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
	shard, entry := c.acquire(versionID)
	defer c.release(shard, versionID, entry)

	entry.gate.Lock()
	defer entry.gate.Unlock()

	entry.stateMu.Lock()
	lastAccess := entry.state.lastAccess
	entry.stateMu.Unlock()
	if remove(lastAccess) {
		entry.stateMu.Lock()
		entry.state = accessState{}
		entry.stateMu.Unlock()
		c.markRetired(shard, entry)
	}
}

func (c *Coordinator) recordAccess(
	ctx context.Context,
	entry *accessEntry,
	versionID string,
	durableAccess *time.Time,
	persist PersistFunc,
	force bool,
) error {
	entry.stateMu.Lock()
	defer entry.stateMu.Unlock()

	now := c.now()
	accessedAt := nextAccessTime(now, entry.state.lastAccess, durableAccess)
	entry.state.lastAccess = accessedAt
	if persist == nil ||
		(!force &&
			!entry.state.lastAttempt.IsZero() &&
			c.persistenceInterval != 0 &&
			now.Sub(entry.state.lastAttempt) < c.persistenceInterval) {
		return nil
	}
	entry.state.lastAttempt = now
	return persist(ctx, versionID, accessedAt)
}

func (c *Coordinator) acquire(versionID string) (*coordinatorShard, *accessEntry) {
	shard := c.shard(versionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.entries == nil {
		shard.entries = make(map[string]*accessEntry)
	}
	entry := shard.entries[versionID]
	if entry == nil {
		entry = &accessEntry{retired: true}
		shard.entries[versionID] = entry
	}
	entry.refs++
	return shard, entry
}

func (c *Coordinator) release(shard *coordinatorShard, versionID string, entry *accessEntry) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry.refs--
	if entry.refs == 0 && entry.retired && shard.entries[versionID] == entry {
		delete(shard.entries, versionID)
	}
}

func (*Coordinator) markActive(shard *coordinatorShard, entry *accessEntry) {
	shard.mu.Lock()
	entry.retired = false
	shard.mu.Unlock()
}

func (*Coordinator) markRetired(shard *coordinatorShard, entry *accessEntry) {
	shard.mu.Lock()
	entry.retired = true
	shard.mu.Unlock()
}

func (c *Coordinator) shard(versionID string) *coordinatorShard {
	if c == nil {
		panic("nil cache access coordinator")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(versionID))
	return &c.shards[h.Sum32()%coordinatorShardCount]
}
