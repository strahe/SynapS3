package observability

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	monitoringInterval  = 15 * time.Second
	monitoringRetention = time.Hour
	monitoringCapacity  = int(monitoringRetention / monitoringInterval)
	monitoringMaxGap    = 2 * monitoringInterval
)

// MonitoringStatus describes the latest sample's metric availability.
type MonitoringStatus struct {
	CPU     string `json:"cpu"`
	RSS     string `json:"rss"`
	Traffic string `json:"traffic"`
}

// MonitoringPoint measures process resources and object payload transfer rates.
type MonitoringPoint struct {
	At                            time.Time `json:"at"`
	CPUCores                      *float64  `json:"cpu_cores"`
	RSSBytes                      *uint64   `json:"rss_bytes"`
	ClientUploadBytesPerSecond    *float64  `json:"client_upload_bytes_per_second"`
	ClientDownloadBytesPerSecond  *float64  `json:"client_download_bytes_per_second"`
	StorageUploadBytesPerSecond   *float64  `json:"storage_upload_bytes_per_second"`
	StorageDownloadBytesPerSecond *float64  `json:"storage_download_bytes_per_second"`
	GapBefore                     bool      `json:"gap_before"`
}

// MonitoringSnapshot is an isolated copy of a runtime's bounded sample history.
type MonitoringSnapshot struct {
	InstanceID       string            `json:"instance_id"`
	StartedAt        time.Time         `json:"started_at"`
	WindowStart      time.Time         `json:"window_start"`
	WindowEnd        time.Time         `json:"window_end"`
	SnapshotAt       time.Time         `json:"snapshot_at"`
	SampledAt        *time.Time        `json:"sampled_at"`
	IntervalSeconds  int               `json:"interval_seconds"`
	RetentionSeconds int               `json:"retention_seconds"`
	Status           MonitoringStatus  `json:"status"`
	Totals           TransferTotals    `json:"totals"`
	Points           []MonitoringPoint `json:"points"`
}

type monitoringSample struct {
	point MonitoringPoint
	at    time.Time
}

// RuntimeMonitor keeps read-only telemetry in a bounded process-local history.
type RuntimeMonitor struct {
	transfers *TransferCounters
	instance  string
	startedAt time.Time
	now       func() time.Time
	process   func() (*float64, *uint64)

	captureMu sync.Mutex
	mu        sync.RWMutex
	points    [monitoringCapacity]monitoringSample
	start     int
	count     int
	previous  time.Time
	cpu       *float64
	totals    TransferTotals
	status    MonitoringStatus
}

// NewRuntimeMonitor creates telemetry sources without starting background work.
func NewRuntimeMonitor(transfers *TransferCounters) *RuntimeMonitor {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return &RuntimeMonitor{
		transfers: transfers,
		instance:  uuid.NewString(),
		startedAt: time.Now(),
		now:       time.Now,
		process:   func() (*float64, *uint64) { return readProcessMetrics(registry) },
		status:    MonitoringStatus{CPU: "collecting", RSS: "collecting", Traffic: "collecting"},
	}
}

func readProcessMetrics(gatherer prometheus.Gatherer) (*float64, *uint64) {
	families, _ := gatherer.Gather()
	var cpu *float64
	var rss *uint64
	for _, family := range families {
		if len(family.Metric) != 1 {
			continue
		}
		metric := family.Metric[0]
		switch family.GetName() {
		case "process_cpu_seconds_total":
			if metric.Counter != nil && metric.Counter.Value != nil {
				value := metric.Counter.GetValue()
				if value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value) {
					cpu = &value
				}
			}
		case "process_resident_memory_bytes":
			if metric.Gauge != nil && metric.Gauge.Value != nil {
				value := metric.Gauge.GetValue()
				if value >= 0 && value < float64(math.MaxUint64) && !math.IsNaN(value) {
					bytes := uint64(value)
					rss = &bytes
				}
			}
		}
	}
	return cpu, rss
}

// Capture records one sample; the first capture establishes rate baselines.
func (m *RuntimeMonitor) Capture() {
	m.captureMu.Lock()
	defer m.captureMu.Unlock()
	cpu, rss := m.process()
	now := m.now()
	totals := m.transfers.Totals()
	point := MonitoringPoint{At: now.UTC(), RSSBytes: rss}
	status := MonitoringStatus{CPU: "collecting", RSS: "unavailable", Traffic: "collecting"}
	if rss != nil {
		status.RSS = "ready"
	}
	if cpu == nil {
		status.CPU = "unavailable"
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.previous.IsZero() {
		m.startedAt = now
	}
	elapsed := now.Sub(m.previous)
	point.GapBefore = !m.previous.IsZero() && (elapsed <= 0 || elapsed > monitoringMaxGap)
	if !m.previous.IsZero() && !point.GapBefore {
		seconds := elapsed.Seconds()
		if cpu != nil && m.cpu != nil && *cpu >= *m.cpu {
			cores := (*cpu - *m.cpu) / seconds
			point.CPUCores = &cores
			status.CPU = "ready"
		}
		if totals.Client.UploadBytes >= m.totals.Client.UploadBytes &&
			totals.Client.DownloadBytes >= m.totals.Client.DownloadBytes &&
			totals.Storage.UploadBytes >= m.totals.Storage.UploadBytes &&
			totals.Storage.DownloadBytes >= m.totals.Storage.DownloadBytes {
			clientUpload := float64(totals.Client.UploadBytes-m.totals.Client.UploadBytes) / seconds
			clientDownload := float64(totals.Client.DownloadBytes-m.totals.Client.DownloadBytes) / seconds
			storageUpload := float64(totals.Storage.UploadBytes-m.totals.Storage.UploadBytes) / seconds
			storageDownload := float64(totals.Storage.DownloadBytes-m.totals.Storage.DownloadBytes) / seconds
			point.ClientUploadBytesPerSecond = &clientUpload
			point.ClientDownloadBytesPerSecond = &clientDownload
			point.StorageUploadBytesPerSecond = &storageUpload
			point.StorageDownloadBytesPerSecond = &storageDownload
			status.Traffic = "ready"
		}
	}
	m.previous, m.cpu, m.totals, m.status = now, cpu, totals, status
	for m.count > 0 && now.Sub(m.points[m.start].at) > monitoringRetention {
		m.points[m.start] = monitoringSample{}
		m.start = (m.start + 1) % len(m.points)
		m.count--
	}
	if m.count == len(m.points) {
		m.start = (m.start + 1) % len(m.points)
		m.count--
	}
	m.points[(m.start+m.count)%len(m.points)] = monitoringSample{point: point, at: now}
	m.count++
}

// Run samples until cancelled; its owner captures the initial baseline first.
func (m *RuntimeMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(monitoringInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Capture()
		}
	}
}

// Snapshot copies the last sample's totals and the retained history atomically.
func (m *RuntimeMonitor) Snapshot() MonitoringSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := m.now()
	snapshot := MonitoringSnapshot{
		InstanceID: m.instance, StartedAt: m.startedAt.UTC(),
		WindowStart: now.Add(-monitoringRetention).UTC(), WindowEnd: now.UTC(), SnapshotAt: now.UTC(),
		IntervalSeconds: int(monitoringInterval / time.Second), RetentionSeconds: int(monitoringRetention / time.Second),
		Status: m.status, Totals: m.totals, Points: make([]MonitoringPoint, 0, m.count),
	}
	if !m.previous.IsZero() {
		at := m.previous.UTC()
		snapshot.SampledAt = &at
	}
	for i := range m.count {
		sample := m.points[(m.start+i)%len(m.points)]
		if now.Sub(sample.at) > monitoringRetention {
			continue
		}
		point := sample.point
		point.CPUCores = cloneMonitoringValue(point.CPUCores)
		point.RSSBytes = cloneMonitoringValue(point.RSSBytes)
		point.ClientUploadBytesPerSecond = cloneMonitoringValue(point.ClientUploadBytesPerSecond)
		point.ClientDownloadBytesPerSecond = cloneMonitoringValue(point.ClientDownloadBytesPerSecond)
		point.StorageUploadBytesPerSecond = cloneMonitoringValue(point.StorageUploadBytesPerSecond)
		point.StorageDownloadBytesPerSecond = cloneMonitoringValue(point.StorageDownloadBytesPerSecond)
		snapshot.Points = append(snapshot.Points, point)
	}
	return snapshot
}

func cloneMonitoringValue[T float64 | uint64](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
