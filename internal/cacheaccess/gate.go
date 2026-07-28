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
// object version. Idle entries are removed as soon as their last user exits.
type Gate struct {
	shards [gateShardCount]gateShard
}

// NewGate creates a cache read/delete gate.
func NewGate() *Gate {
	return &Gate{}
}

// Open protects a successful cache open until the returned body is closed.
func (g *Gate) Open(
	versionID string,
	open func() (io.ReadCloser, *cache.ObjectInfo, error),
) (*OpenedCacheEntry, error) {
	shard, entry := g.acquire(versionID)
	entry.mu.RLock()

	body, info, err := open()
	if err != nil {
		entry.mu.RUnlock()
		g.release(shard, versionID, entry)
		return nil, err
	}

	return &OpenedCacheEntry{
		Body: &guardedReadCloser{
			body: body,
			release: func() {
				entry.mu.RUnlock()
				g.release(shard, versionID, entry)
			},
		},
		Info: info,
	}, nil
}

// Commit serializes a local cache commit with deletion for the same version.
func (g *Gate) Commit(versionID string, commit func() error) error {
	shard, entry := g.acquire(versionID)
	defer g.release(shard, versionID, entry)

	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return commit()
}

// GuardDeletion waits for open bodies and serializes final checks and physical
// deletion for one version.
func (g *Gate) GuardDeletion(versionID string, remove func()) {
	shard, entry := g.acquire(versionID)
	defer g.release(shard, versionID, entry)

	entry.mu.Lock()
	defer entry.mu.Unlock()
	remove()
}

func (g *Gate) guardAccess(versionID string, access func()) {
	shard, entry := g.acquire(versionID)
	defer g.release(shard, versionID, entry)

	entry.mu.RLock()
	defer entry.mu.RUnlock()
	access()
}

func (g *Gate) acquire(versionID string) (*gateShard, *gateEntry) {
	shard := g.shard(versionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.entries == nil {
		shard.entries = make(map[string]*gateEntry)
	}
	entry := shard.entries[versionID]
	if entry == nil {
		entry = &gateEntry{}
		shard.entries[versionID] = entry
	}
	entry.refs++
	return shard, entry
}

func (*Gate) release(shard *gateShard, versionID string, entry *gateEntry) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry.refs--
	if entry.refs == 0 && shard.entries[versionID] == entry {
		delete(shard.entries, versionID)
	}
}

func (g *Gate) shard(versionID string) *gateShard {
	if g == nil {
		panic("nil cache access gate")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(versionID))
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
