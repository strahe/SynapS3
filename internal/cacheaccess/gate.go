package cacheaccess

import (
	"context"
	"hash/fnv"
	"io"
	"sync"

	"github.com/strahe/synaps3/internal/cache"
	"golang.org/x/sync/semaphore"
)

const (
	gateShardCount       = 256
	gateCapacity   int64 = 1<<63 - 1
)

// OpenedCacheEntry keeps a cache entry protected from deletion until Body is
// closed.
type OpenedCacheEntry struct {
	Body io.ReadCloser
	Info *cache.ObjectInfo
}

type gateEntry struct {
	permits *semaphore.Weighted
	refs    int
}

type gateShard struct {
	mu      sync.Mutex
	entries map[string]*gateEntry
}

// Gate serializes cache opens and commits with physical deletion for the same
// content cache key. Idle entries are removed as soon as their last user exits.
type Gate struct {
	shards [gateShardCount]gateShard
}

// NewGate creates a cache read/commit/delete gate.
func NewGate() *Gate {
	return &Gate{}
}

// Open protects a successful cache open until the returned body is closed.
func (g *Gate) Open(
	ctx context.Context,
	cacheKey string,
	open func() (io.ReadCloser, *cache.ObjectInfo, error),
) (*OpenedCacheEntry, error) {
	release, err := g.HoldRead(ctx, cacheKey)
	if err != nil {
		return nil, err
	}

	body, info, err := open()
	if err != nil {
		release()
		return nil, err
	}

	return &OpenedCacheEntry{
		Body: &guardedReadCloser{
			body:    body,
			release: release,
		},
		Info: info,
	}, nil
}

// HoldRead protects a cache entry from deletion across multiple opens. The
// returned release function is idempotent and must be called by the holder.
func (g *Gate) HoldRead(ctx context.Context, cacheKey string) (func(), error) {
	return g.hold(ctx, cacheKey, 1)
}

func (g *Gate) hold(ctx context.Context, cacheKey string, weight int64) (func(), error) {
	shard, entry := g.acquire(cacheKey)
	if err := entry.permits.Acquire(ctx, weight); err != nil {
		g.release(shard, cacheKey, entry)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.permits.Release(weight)
			g.release(shard, cacheKey, entry)
		})
	}, nil
}

// Commit serializes a local cache commit with deletion for the same cache key.
func (g *Gate) Commit(ctx context.Context, cacheKey string, commit func() error) error {
	return g.guardAccess(ctx, cacheKey, commit)
}

// GuardDeletion waits for open bodies and serializes final checks and physical
// deletion for one cache key.
func (g *Gate) GuardDeletion(ctx context.Context, cacheKey string, remove func()) error {
	release, err := g.hold(ctx, cacheKey, gateCapacity)
	if err != nil {
		return err
	}
	defer release()
	remove()
	return nil
}

func (g *Gate) guardAccess(ctx context.Context, cacheKey string, access func() error) error {
	release, err := g.HoldRead(ctx, cacheKey)
	if err != nil {
		return err
	}
	defer release()
	return access()
}

func (g *Gate) acquire(cacheKey string) (*gateShard, *gateEntry) {
	shard := g.shard(cacheKey)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.entries == nil {
		shard.entries = make(map[string]*gateEntry)
	}
	entry := shard.entries[cacheKey]
	if entry == nil {
		entry = &gateEntry{permits: semaphore.NewWeighted(gateCapacity)}
		shard.entries[cacheKey] = entry
	}
	entry.refs++
	return shard, entry
}

func (*Gate) release(shard *gateShard, cacheKey string, entry *gateEntry) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry.refs--
	if entry.refs == 0 && shard.entries[cacheKey] == entry {
		delete(shard.entries, cacheKey)
	}
}

func (g *Gate) shard(cacheKey string) *gateShard {
	if g == nil {
		panic("nil cache access gate")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(cacheKey))
	return &g.shards[h.Sum32()%gateShardCount]
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
