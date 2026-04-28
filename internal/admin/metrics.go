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

	// WorkerTasksProcessed counts tasks processed by worker type and result.
	WorkerTasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "worker",
		Name:      "tasks_processed_total",
		Help:      "Total tasks processed by worker type and result",
	}, []string{"worker", "result"})

	// DeadLetterTotal counts tasks that entered dead-letter status by worker type and task type.
	DeadLetterTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "synaps3",
		Subsystem: "worker",
		Name:      "dead_letter_total",
		Help:      "Total tasks that entered dead-letter status",
	}, []string{"worker", "task_type"})

	// TaskQueueDepth tracks pending task count by type and status.
	TaskQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "synaps3",
		Subsystem: "task",
		Name:      "queue_depth",
		Help:      "Number of tasks by type and status",
	}, []string{"type", "status"})

	// WorkerTaskDuration tracks per-task processing duration in seconds.
	WorkerTaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "synaps3",
		Subsystem: "worker",
		Name:      "task_duration_seconds",
		Help:      "Task processing duration in seconds by worker",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~200s
	}, []string{"worker"})

	// ObjectStateDistribution tracks object count by state.
	ObjectStateDistribution = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "synaps3",
		Subsystem: "object",
		Name:      "state_distribution",
		Help:      "Number of objects by state",
	}, []string{"state"})
)
