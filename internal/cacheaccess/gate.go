package cacheaccess

import (
	"hash/fnv"
	"io"
	"sync"

	"github.com/strahe/synaps3/internal/cache"
)

const gateShardCount = 256

// OpenedCacheEntry keeps a cache entry protected from deletion until Body is
// closed.
type OpenedCacheEntry struct {
	Body io.ReadCloser
	Info *cache.ObjectInfo
}

type gateEntry struct {
	mu   sync.RWMutex
	refs int
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
	cacheKey string,
	open func() (io.ReadCloser, *cache.ObjectInfo, error),
) (*OpenedCacheEntry, error) {
	release := g.HoldRead(cacheKey)

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
func (g *Gate) HoldRead(cacheKey string) func() {
	shard, entry := g.acquire(cacheKey)
	entry.mu.RLock()
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.RUnlock()
			g.release(shard, cacheKey, entry)
		})
	}
}

// Commit serializes a local cache commit with deletion for the same cache key.
func (g *Gate) Commit(cacheKey string, commit func() error) error {
	shard, entry := g.acquire(cacheKey)
	defer g.release(shard, cacheKey, entry)

	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return commit()
}

// GuardDeletion waits for open bodies and serializes final checks and physical
// deletion for one cache key.
func (g *Gate) GuardDeletion(cacheKey string, remove func()) {
	shard, entry := g.acquire(cacheKey)
	defer g.release(shard, cacheKey, entry)

	entry.mu.Lock()
	defer entry.mu.Unlock()
	remove()
}

func (g *Gate) guardAccess(cacheKey string, access func()) {
	release := g.HoldRead(cacheKey)
	defer release()
	access()
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
		entry = &gateEntry{}
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
