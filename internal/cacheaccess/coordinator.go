package cacheaccess

import (
	"context"
	"hash/fnv"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
)

const (
	// DefaultPersistenceInterval bounds foreground cache-access writes while
	// keeping the durable LRU order reasonably current across restarts.
	DefaultPersistenceInterval = time.Minute
	coordinatorShardCount      = 256
	defaultEntriesPerShard     = 64
)

// PersistFunc records one cache access in durable object metadata.
type PersistFunc func(context.Context, string, time.Time) error

// OpenedCacheEntry is a cache entry opened while coordinated with cache
// deletion. Closing Body releases the deletion guard.
type OpenedCacheEntry struct {
	Body         io.ReadCloser
	Info         *cache.ObjectInfo
	PersistError error
}

// CacheCommitResult reports a best-effort metadata persistence failure after
// the local cache commit itself succeeded.
type CacheCommitResult struct {
	PersistError error
}

type accessState struct {
	lastAccess    time.Time
	lastAttempt   time.Time
	lastPersisted time.Time
	lastTouched   time.Time
	persist       PersistFunc
}

type accessEntry struct {
	gate    sync.RWMutex
	stateMu sync.Mutex
	state   accessState

	// refs and retired are protected by the owning shard mutex. An open body
	// owns one ref until Close.
	refs    int
	retired bool
}

type coordinatorShard struct {
	mu      sync.Mutex
	entries map[string]*accessEntry
}

type acquiredEntry struct {
	shard     *coordinatorShard
	versionID string
	entry     *accessEntry
}

// Coordinator coordinates foreground cache reads with physical deletion and
// coalesces durable access-time writes. It must be shared by every cache reader
// and deleter in one process.
type Coordinator struct {
	persistenceInterval time.Duration
	sweepInterval       time.Duration
	idleRetention       time.Duration
	maxEntriesPerShard  int
	now                 func() time.Time
	shards              [coordinatorShardCount]coordinatorShard
}

// NewCoordinator creates a cache-access coordinator.
func NewCoordinator(persistenceInterval time.Duration) *Coordinator {
	if persistenceInterval < 0 {
		persistenceInterval = 0
	}
	sweepInterval := persistenceInterval
	if sweepInterval <= 0 {
		sweepInterval = DefaultPersistenceInterval
	}
	return &Coordinator{
		persistenceInterval: persistenceInterval,
		sweepInterval:       sweepInterval,
		idleRetention:       2 * sweepInterval,
		maxEntriesPerShard:  defaultEntriesPerShard,
		now:                 time.Now,
	}
}

// Run flushes coalesced access times and retires idle coordination entries.
func (c *Coordinator) Run(ctx context.Context, logger *slog.Logger) {
	if c == nil {
		panic("nil cache access coordinator")
	}
	if logger == nil {
		logger = slog.Default()
	}
	ticker := time.NewTicker(c.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			failed, err := c.sweep(ctx)
			if err != nil && ctx.Err() == nil {
				logger.Warn(
					"flushing cache access timestamps",
					"failedEntries",
					failed,
					"error",
					err,
				)
			}
		}
	}
}

// Open coordinates a successful cache open and keeps deletion blocked until
// the returned body is closed. Durable metadata is written at most once per
// persistence interval for the same version.
func (c *Coordinator) Open(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	open func() (io.ReadCloser, *cache.ObjectInfo, error),
	persist PersistFunc,
) (*OpenedCacheEntry, error) {
	shard, entry := c.acquire(versionID)
	entry.gate.RLock()

	body, info, err := open()
	if err != nil {
		entry.gate.RUnlock()
		c.release(shard, versionID, entry)
		return nil, err
	}

	persistErr := c.recordAccess(ctx, entry, versionID, durableAccess, persist, false)
	c.markActive(shard, entry)
	guardedBody := &guardedReadCloser{
		body: body,
		release: func() {
			entry.gate.RUnlock()
			c.release(shard, versionID, entry)
		},
	}

	return &OpenedCacheEntry{
		Body:         guardedBody,
		Info:         info,
		PersistError: persistErr,
	}, nil
}

// Commit serializes a local cache commit and its metadata update with deletion
// for the same version.
func (c *Coordinator) Commit(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	commit func() error,
	persist PersistFunc,
) (*CacheCommitResult, error) {
	shard, entry := c.acquire(versionID)
	defer c.release(shard, versionID, entry)

	entry.gate.RLock()
	defer entry.gate.RUnlock()

	if err := commit(); err != nil {
		return nil, err
	}
	persistErr := c.recordAccess(ctx, entry, versionID, durableAccess, persist, true)
	c.markActive(shard, entry)
	return &CacheCommitResult{PersistError: persistErr}, nil
}

// GuardDeletion serializes final checks and physical deletion with open bodies
// for the same version. Returning true retires the removed entry.
func (c *Coordinator) GuardDeletion(versionID string, remove func(time.Time) bool) {
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

func nextAccessTime(now, inMemory time.Time, durable *time.Time) time.Time {
	now = cacheeviction.NormalizeAccessTime(now)
	previous := cacheeviction.NormalizeAccessTime(inMemory)
	if durable != nil {
		durableTime := cacheeviction.NormalizeAccessTime(*durable)
		if durableTime.After(previous) {
			previous = durableTime
		}
	}
	if !previous.IsZero() && !now.After(previous) {
		return previous.Add(time.Microsecond)
	}
	return now
}

func (c *Coordinator) recordAccess(
	ctx context.Context,
	entry *accessEntry,
	versionID string,
	durableAccess *time.Time,
	persist PersistFunc,
	force bool,
) error {
	now := cacheeviction.NormalizeAccessTime(c.now())

	entry.stateMu.Lock()
	if durableAccess != nil {
		durableTime := cacheeviction.NormalizeAccessTime(*durableAccess)
		if durableTime.After(entry.state.lastPersisted) {
			entry.state.lastPersisted = durableTime
		}
	}
	accessedAt := nextAccessTime(now, entry.state.lastAccess, durableAccess)
	entry.state.lastAccess = accessedAt
	entry.state.lastTouched = now
	if persist != nil {
		entry.state.persist = persist
	}
	shouldPersist := persist != nil &&
		(force ||
			entry.state.lastAttempt.IsZero() ||
			c.persistenceInterval == 0 ||
			now.Sub(entry.state.lastAttempt) >= c.persistenceInterval)
	if shouldPersist {
		entry.state.lastAttempt = now
	}
	entry.stateMu.Unlock()

	if !shouldPersist {
		return nil
	}
	if err := persist(ctx, versionID, accessedAt); err != nil {
		return err
	}
	entry.stateMu.Lock()
	if accessedAt.After(entry.state.lastPersisted) {
		entry.state.lastPersisted = accessedAt
	}
	entry.stateMu.Unlock()
	return nil
}

func (c *Coordinator) sweep(ctx context.Context) (int, error) {
	now := cacheeviction.NormalizeAccessTime(c.now())
	candidates := c.acquireIdleEntries()
	var (
		failed   int
		firstErr error
	)
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			c.release(candidate.shard, candidate.versionID, candidate.entry)
			continue
		}
		if err := c.flushAndRetire(ctx, candidate, now); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
		c.release(candidate.shard, candidate.versionID, candidate.entry)
	}
	return failed, firstErr
}

func (c *Coordinator) acquireIdleEntries() []acquiredEntry {
	var candidates []acquiredEntry
	for index := range c.shards {
		shard := &c.shards[index]
		shard.mu.Lock()
		for versionID, entry := range shard.entries {
			if entry.refs != 0 {
				continue
			}
			entry.refs++
			candidates = append(candidates, acquiredEntry{
				shard:     shard,
				versionID: versionID,
				entry:     entry,
			})
		}
		shard.mu.Unlock()
	}
	return candidates
}

func (c *Coordinator) flushAndRetire(
	ctx context.Context,
	acquired acquiredEntry,
	now time.Time,
) error {
	entry := acquired.entry
	entry.gate.Lock()
	defer entry.gate.Unlock()

	entry.stateMu.Lock()
	state := entry.state
	dirty := state.lastAccess.After(state.lastPersisted)
	due := dirty && state.persist != nil &&
		(state.lastAttempt.IsZero() ||
			c.persistenceInterval == 0 ||
			now.Sub(state.lastAttempt) >= c.persistenceInterval)
	if due {
		entry.state.lastAttempt = now
	}
	entry.stateMu.Unlock()

	if due {
		if err := state.persist(ctx, acquired.versionID, state.lastAccess); err != nil {
			return err
		}
		entry.stateMu.Lock()
		if state.lastAccess.After(entry.state.lastPersisted) {
			entry.state.lastPersisted = state.lastAccess
		}
		state = entry.state
		entry.stateMu.Unlock()
	}

	clean := !state.lastAccess.After(state.lastPersisted)
	idle := !state.lastTouched.IsZero() && now.Sub(state.lastTouched) >= c.idleRetention
	if clean && idle {
		c.markRetired(acquired.shard, entry)
	}
	return nil
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
		for c.maxEntriesPerShard > 0 && len(shard.entries) >= c.maxEntriesPerShard {
			entryCount := len(shard.entries)
			evictOldestEntryLocked(shard)
			if len(shard.entries) == entryCount {
				break
			}
		}
		entry = &accessEntry{retired: true}
		shard.entries[versionID] = entry
	}
	entry.refs++
	return shard, entry
}

func evictOldestEntryLocked(shard *coordinatorShard) {
	var (
		cleanID      string
		cleanEntry   *accessEntry
		cleanTouched time.Time
	)
	for versionID, entry := range shard.entries {
		if entry.refs != 0 {
			continue
		}
		entry.stateMu.Lock()
		state := entry.state
		entry.stateMu.Unlock()
		if !state.lastAccess.After(state.lastPersisted) &&
			(cleanEntry == nil || state.lastTouched.Before(cleanTouched)) {
			cleanID = versionID
			cleanEntry = entry
			cleanTouched = state.lastTouched
		}
	}
	if cleanEntry != nil && shard.entries[cleanID] == cleanEntry {
		delete(shard.entries, cleanID)
	}
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

type guardedReadCloser struct {
	body     io.ReadCloser
	release  func()
	once     sync.Once
	closeErr error
}

func (r *guardedReadCloser) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *guardedReadCloser) Close() error {
	r.once.Do(func() {
		r.closeErr = r.body.Close()
		r.release()
	})
	return r.closeErr
}
