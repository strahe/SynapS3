package cacheaccess

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
)

const (
	// DefaultPersistenceInterval bounds foreground cache-access writes while
	// keeping the durable LRU order reasonably current across restarts.
	DefaultPersistenceInterval = time.Minute
	trackerShardCount          = 256
	defaultEntriesPerShard     = 64
)

// ErrLRUAccessUncertain means an access timestamp could not be retained or
// persisted. LRU eviction must remain paused for the rest of the process.
var ErrLRUAccessUncertain = errors.New("cache access is not reliable enough for LRU eviction")

// Store persists foreground cache access and cache commit metadata.
type Store interface {
	RecordVersionCacheAccess(context.Context, string, time.Time) error
	RecordVersionCacheCommit(context.Context, string, time.Time) error
}

type trackerEntry struct {
	lastAccess     time.Time
	lastAttempt    time.Time
	lastPersisted  time.Time
	lastTouched    time.Time
	commitRequired time.Time
}

type trackerShard struct {
	mu      sync.Mutex
	entries map[string]*trackerEntry
}

// Tracker coalesces durable access-time writes and retains the latest exact
// in-process access used by LRU finalization.
type Tracker struct {
	store               Store
	persistenceInterval time.Duration
	sweepInterval       time.Duration
	idleRetention       time.Duration
	maxEntriesPerShard  int
	now                 func() time.Time
	unsafeForLRU        atomic.Bool
	shards              [trackerShardCount]trackerShard
}

// NewTracker creates a bounded foreground cache-access tracker.
func NewTracker(persistenceInterval time.Duration, store Store) *Tracker {
	if store == nil {
		panic("cache access tracker requires a store")
	}
	if persistenceInterval < 0 {
		persistenceInterval = 0
	}
	sweepInterval := persistenceInterval
	if sweepInterval <= 0 {
		sweepInterval = DefaultPersistenceInterval
	}
	return &Tracker{
		store:               store,
		persistenceInterval: persistenceInterval,
		sweepInterval:       sweepInterval,
		idleRetention:       2 * sweepInterval,
		maxEntriesPerShard:  defaultEntriesPerShard,
		now:                 time.Now,
	}
}

// Run flushes coalesced access times and retires clean idle entries.
func (t *Tracker) Run(ctx context.Context, gate *Gate, logger *slog.Logger) {
	if t == nil {
		panic("nil cache access tracker")
	}
	if gate == nil {
		panic("cache access tracker requires a gate")
	}
	if logger == nil {
		logger = slog.Default()
	}
	ticker := time.NewTicker(t.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			failed, err := t.sweep(ctx, gate)
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

// RecordAccess records one successful foreground cache open.
func (t *Tracker) RecordAccess(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
) error {
	return t.record(ctx, versionID, durableAccess, false)
}

// RecordCommit records a successful local cache commit and forces the presence
// and access metadata write.
func (t *Tracker) RecordCommit(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
) error {
	return t.record(ctx, versionID, durableAccess, true)
}

// Latest returns the latest exact in-process access for one version.
func (t *Tracker) Latest(versionID string) time.Time {
	shard := t.shard(versionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if entry := shard.entries[versionID]; entry != nil {
		return entry.lastAccess
	}
	return time.Time{}
}

// FlushWhileGuarded persists one version's latest access. The caller must hold
// the version's cache gate.
func (t *Tracker) FlushWhileGuarded(ctx context.Context, versionID string) error {
	return t.flush(ctx, versionID, cacheeviction.NormalizeAccessTime(t.now()), true)
}

// Forget removes tracking state after the local cache entry is deleted.
func (t *Tracker) Forget(versionID string) {
	shard := t.shard(versionID)
	shard.mu.Lock()
	delete(shard.entries, versionID)
	shard.mu.Unlock()
}

// SafeForLRU reports whether all access timestamps that could affect LRU
// deletion are still represented in memory or durable storage.
func (t *Tracker) SafeForLRU() bool {
	return t != nil && !t.unsafeForLRU.Load()
}

func (t *Tracker) record(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	requireCommit bool,
) error {
	now := cacheeviction.NormalizeAccessTime(t.now())
	shard := t.shard(versionID)
	shard.mu.Lock()
	if shard.entries == nil {
		shard.entries = make(map[string]*trackerEntry)
	}
	entry := shard.entries[versionID]
	if entry == nil {
		t.evictOldestCleanLocked(shard)
		if t.maxEntriesPerShard > 0 && len(shard.entries) >= t.maxEntriesPerShard {
			shard.mu.Unlock()
			return t.persistOverflow(ctx, versionID, durableAccess, now, requireCommit)
		}
		entry = &trackerEntry{}
		shard.entries[versionID] = entry
	}

	if durableAccess != nil {
		durableTime := cacheeviction.NormalizeAccessTime(*durableAccess)
		if durableTime.After(entry.lastPersisted) {
			entry.lastPersisted = durableTime
		}
	}
	accessedAt := nextAccessTime(now, entry.lastAccess, durableAccess)
	entry.lastAccess = accessedAt
	entry.lastTouched = now
	if requireCommit && accessedAt.After(entry.commitRequired) {
		entry.commitRequired = accessedAt
	}
	shouldPersist := requireCommit ||
		entry.lastAttempt.IsZero() ||
		t.persistenceInterval == 0 ||
		now.Sub(entry.lastAttempt) >= t.persistenceInterval
	if shouldPersist {
		entry.lastAttempt = now
	}
	commit := !entry.commitRequired.IsZero()
	shard.mu.Unlock()

	if !shouldPersist {
		return nil
	}
	return t.persistAndAcknowledge(ctx, shard, versionID, entry, accessedAt, commit)
}

func (t *Tracker) persistOverflow(
	ctx context.Context,
	versionID string,
	durableAccess *time.Time,
	now time.Time,
	commit bool,
) error {
	accessedAt := nextAccessTime(now, time.Time{}, durableAccess)
	if err := t.persist(ctx, versionID, accessedAt, commit); err != nil {
		t.unsafeForLRU.Store(true)
		return fmt.Errorf("%w: persisting access for %s: %w", ErrLRUAccessUncertain, versionID, err)
	}
	return nil
}

func (t *Tracker) persistAndAcknowledge(
	ctx context.Context,
	shard *trackerShard,
	versionID string,
	entry *trackerEntry,
	accessedAt time.Time,
	commit bool,
) error {
	if err := t.persist(ctx, versionID, accessedAt, commit); err != nil {
		return err
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.entries[versionID] != entry {
		return nil
	}
	if accessedAt.After(entry.lastPersisted) {
		entry.lastPersisted = accessedAt
	}
	if commit && !entry.commitRequired.After(accessedAt) {
		entry.commitRequired = time.Time{}
	}
	return nil
}

func (t *Tracker) persist(
	ctx context.Context,
	versionID string,
	accessedAt time.Time,
	commit bool,
) error {
	if commit {
		return t.store.RecordVersionCacheCommit(ctx, versionID, accessedAt)
	}
	return t.store.RecordVersionCacheAccess(ctx, versionID, accessedAt)
}

func (t *Tracker) sweep(ctx context.Context, gate *Gate) (int, error) {
	now := cacheeviction.NormalizeAccessTime(t.now())
	versionIDs := t.snapshotVersionIDs()
	var (
		failed   int
		firstErr error
	)
	for _, versionID := range versionIDs {
		if ctx.Err() != nil {
			break
		}
		var err error
		gate.guardAccess(versionID, func() {
			err = t.flush(ctx, versionID, now, false)
		})
		if err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return failed, firstErr
}

func (t *Tracker) snapshotVersionIDs() []string {
	var versionIDs []string
	for index := range t.shards {
		shard := &t.shards[index]
		shard.mu.Lock()
		for versionID := range shard.entries {
			versionIDs = append(versionIDs, versionID)
		}
		shard.mu.Unlock()
	}
	return versionIDs
}

func (t *Tracker) flush(
	ctx context.Context,
	versionID string,
	now time.Time,
	force bool,
) error {
	shard := t.shard(versionID)
	shard.mu.Lock()
	entry := shard.entries[versionID]
	if entry == nil {
		shard.mu.Unlock()
		return nil
	}
	dirty := entry.lastAccess.After(entry.lastPersisted) || !entry.commitRequired.IsZero()
	due := dirty &&
		(force ||
			entry.lastAttempt.IsZero() ||
			t.persistenceInterval == 0 ||
			now.Sub(entry.lastAttempt) >= t.persistenceInterval)
	if due {
		entry.lastAttempt = now
	}
	accessedAt := entry.lastAccess
	commit := !entry.commitRequired.IsZero()
	shard.mu.Unlock()

	if due {
		if err := t.persistAndAcknowledge(
			ctx,
			shard,
			versionID,
			entry,
			accessedAt,
			commit,
		); err != nil {
			return err
		}
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.entries[versionID] != entry {
		return nil
	}
	clean := !entry.lastAccess.After(entry.lastPersisted) && entry.commitRequired.IsZero()
	idle := !entry.lastTouched.IsZero() && now.Sub(entry.lastTouched) >= t.idleRetention
	if clean && idle {
		delete(shard.entries, versionID)
	}
	return nil
}

func (t *Tracker) evictOldestCleanLocked(shard *trackerShard) {
	if t.maxEntriesPerShard <= 0 || len(shard.entries) < t.maxEntriesPerShard {
		return
	}
	var (
		oldestID      string
		oldestEntry   *trackerEntry
		oldestTouched time.Time
	)
	for versionID, entry := range shard.entries {
		clean := !entry.lastAccess.After(entry.lastPersisted) && entry.commitRequired.IsZero()
		if clean && (oldestEntry == nil || entry.lastTouched.Before(oldestTouched)) {
			oldestID = versionID
			oldestEntry = entry
			oldestTouched = entry.lastTouched
		}
	}
	if oldestEntry != nil && shard.entries[oldestID] == oldestEntry {
		delete(shard.entries, oldestID)
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

func (t *Tracker) shard(versionID string) *trackerShard {
	if t == nil {
		panic("nil cache access tracker")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(versionID))
	return &t.shards[h.Sum32()%trackerShardCount]
}
