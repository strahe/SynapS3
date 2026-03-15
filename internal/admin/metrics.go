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

	// CacheMissesTotal counts cache misses (SP fallback required).
	CacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "cache",
		Name:      "misses_total",
		Help:      "Total cache misses (SP fallback required)",
	})

	// WorkerTasksProcessed counts tasks processed by worker type and result.
	WorkerTasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "worker",
		Name:      "tasks_processed_total",
		Help:      "Total tasks processed by worker type and result",
	}, []string{"worker", "result"})
)
