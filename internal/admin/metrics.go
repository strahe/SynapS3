package admin

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ObjectOperationsTotal counts S3 object operations by type and status.
	ObjectOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "backend",
		Name:      "object_operations_total",
		Help:      "Total S3 object operations by type and status",
	}, []string{"operation", "status"})

	// CacheUsedBytes tracks current cache usage in bytes.
	CacheUsedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "synaps3",
		Subsystem: "cache",
		Name:      "used_bytes",
		Help:      "Current cache usage in bytes",
	})

	// CacheHitsTotal counts cache hits (object found locally).
	CacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "cache",
		Name:      "hits_total",
		Help:      "Total cache hits (object found locally)",
	})

	// CacheMissesTotal counts cache misses (object not found locally).
	CacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "cache",
		Name:      "misses_total",
		Help:      "Total cache misses (object not found locally)",
	})

	// CacheLRUEvictionPaused reports whether LRU eviction is paused because
	// recent cache access could not be retained safely.
	CacheLRUEvictionPaused = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "synaps3",
		Subsystem: "cache",
		Name:      "lru_eviction_paused",
		Help:      "Whether LRU cache eviction is paused because recent cache access could not be retained safely",
	})

	// TaskQueueDepth tracks active task count by type and status.
	TaskQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "synaps3",
		Subsystem: "task",
		Name:      "queue_depth",
		Help:      "Number of tasks by type and status",
	}, []string{"type", "status"})

	// ObjectStateDistribution tracks object count by state.
	ObjectStateDistribution = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "synaps3",
		Subsystem: "object",
		Name:      "state_distribution",
		Help:      "Number of objects by state",
	}, []string{"state"})
)
