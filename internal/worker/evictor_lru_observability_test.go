package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

type unavailableCacheAccessStore struct{}

func (*unavailableCacheAccessStore) RecordVersionCacheAccess(
	context.Context,
	string,
	time.Time,
) error {
	return errors.New("database unavailable")
}

func (*unavailableCacheAccessStore) RecordVersionCacheCommit(
	context.Context,
	string,
	time.Time,
) error {
	return errors.New("database unavailable")
}

func unsafeCacheAccessTracker(t *testing.T) *cacheaccess.Tracker {
	t.Helper()
	tracker := cacheaccess.NewTracker(time.Minute, new(unavailableCacheAccessStore))
	for index := 0; index < 100_000 && tracker.SafeForLRU(); index++ {
		versionID := fmt.Sprintf("unsafe-version-%d", index)
		hash := fnv.New32a()
		_, _ = hash.Write([]byte(versionID))
		if hash.Sum32()%256 != 0 {
			continue
		}
		_ = tracker.RecordAccess(context.Background(), versionID, nil)
	}
	if tracker.SafeForLRU() {
		t.Fatal("cache access tracker remained safe after bounded dirty entries overflowed")
	}
	return tracker
}

func gatheredGaugeValue(name string) float64 {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return -1
	}
	for _, family := range families {
		if family.GetName() != name || len(family.GetMetric()) == 0 {
			continue
		}
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	return -1
}

func TestEvictor_LRUAccessTrackingPauseIsObservable(t *testing.T) {
	admin.CacheLRUEvictionPaused.Set(0)
	t.Cleanup(func() {
		admin.CacheLRUEvictionPaused.Set(0)
	})
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 {
			return 100
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		unsafeCacheAccessTracker(t),
		env.sm,
		0,
		10*time.Millisecond,
		logger,
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 100, 90, 80, 3),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(ctx)
		close(done)
	}()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for gatheredGaugeValue("synaps3_cache_lru_eviction_paused") != 1 {
		select {
		case <-deadline:
			cancel()
			waitForSignal(t, done, time.Second, "LRU pause observability test shutdown")
			t.Fatal("LRU pause metric did not become 1")
		case <-ticker.C:
		}
	}
	time.Sleep(40 * time.Millisecond)
	cancel()
	waitForSignal(t, done, time.Second, "LRU pause observability test shutdown")

	const pauseMessage = "LRU cache eviction paused until restart"
	if count := strings.Count(logs.String(), pauseMessage); count != 1 {
		t.Fatalf("LRU pause log count = %d, want 1; logs=%s", count, logs.String())
	}
}
