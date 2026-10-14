package observability

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestRuntimeMonitorBaselinesRatesAndIsolatedSnapshot(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cpu := 10.0
	monitor, counters := controlledRuntimeMonitor(&now, &cpu)
	counters.bytes[ClientDownload].Store(1<<53 + 1)
	empty := monitor.Snapshot()
	if empty.SampledAt != nil || len(empty.Points) != 0 || empty.Status.CPU != "collecting" {
		t.Fatalf("unstarted snapshot = %#v", empty)
	}
	monitor.Capture()
	first := monitor.Snapshot()
	if first.Points[0].CPUCores != nil || first.Points[0].ClientUploadBytesPerSecond != nil || *first.Points[0].RSSBytes != 4096 {
		t.Fatalf("baseline = %#v", first.Points[0])
	}
	now = now.Add(15 * time.Second)
	cpu += 30
	if _, err := counters.WrapWriter(ClientUpload, io.Discard).Write(make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	if _, err := counters.WrapWriter(ClientDownload, io.Discard).Write(make([]byte, 30)); err != nil {
		t.Fatal(err)
	}
	monitor.Capture()
	if _, err := counters.WrapWriter(ClientUpload, io.Discard).Write(make([]byte, 60)); err != nil {
		t.Fatal(err)
	}
	got := monitor.Snapshot()
	point := got.Points[1]
	if *point.CPUCores != 2 || *point.ClientUploadBytesPerSecond != 20 || *point.ClientDownloadBytesPerSecond != 2 || *point.StorageDownloadBytesPerSecond != 0 {
		t.Fatalf("rates = %#v", point)
	}
	if got.Totals.Client.UploadBytes != 300 || got.Status != (MonitoringStatus{"ready", "ready", "ready"}) {
		t.Fatalf("snapshot totals/status = %#v / %#v", got.Totals, got.Status)
	}
	*got.Points[1].CPUCores = 99
	*got.Points[1].RSSBytes = 99
	got.Points[1].GapBefore = true
	if fresh := monitor.Snapshot().Points[1]; *fresh.CPUCores != 2 || *fresh.RSSBytes != 4096 || fresh.GapBefore {
		t.Fatal("snapshot mutation changed monitor history")
	}
	encoded, err := json.Marshal(got)
	if err != nil || !strings.Contains(string(encoded), `"upload_bytes":"300"`) || !strings.Contains(string(encoded), `"cpu_cores":null`) {
		t.Fatalf("snapshot JSON = %s, %v", encoded, err)
	}
}

func TestRuntimeMonitorGapAndResourceFailuresRecoverIndependently(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cpu := 10.0
	monitor, _ := controlledRuntimeMonitor(&now, &cpu)
	monitor.Capture()
	now = now.Add(31 * time.Second)
	cpu += 5
	monitor.Capture()
	gap := monitor.Snapshot().Points[1]
	if !gap.GapBefore || gap.CPUCores != nil || gap.ClientUploadBytesPerSecond != nil || gap.RSSBytes == nil {
		t.Fatalf("delayed point = %#v", gap)
	}
	monitor.process = func() (*float64, *uint64) { return nil, nil }
	now = now.Add(15 * time.Second)
	monitor.Capture()
	failed := monitor.Snapshot()
	if failed.Status != (MonitoringStatus{"unavailable", "unavailable", "ready"}) || failed.Points[2].ClientUploadBytesPerSecond == nil {
		t.Fatalf("resource failure = %#v", failed)
	}
	monitor.process = func() (*float64, *uint64) {
		cpuValue, rss := cpu, uint64(4096)
		return &cpuValue, &rss
	}
	now = now.Add(15 * time.Second)
	monitor.Capture()
	if recovered := monitor.Snapshot(); recovered.Status.CPU != "collecting" || recovered.Points[3].CPUCores != nil || recovered.Status.RSS != "ready" {
		t.Fatalf("CPU recovery baseline = %#v", recovered)
	}
	now = now.Add(15 * time.Second)
	cpu += 15
	monitor.Capture()
	if recovered := monitor.Snapshot(); recovered.Status.CPU != "ready" || *recovered.Points[4].CPUCores != 1 {
		t.Fatalf("CPU recovery rate = %#v", recovered)
	}
}

func TestRuntimeMonitorBoundsHistoryAndPrunesByElapsedTime(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cpu := 0.0
	monitor, _ := controlledRuntimeMonitor(&now, &cpu)
	for range 241 {
		monitor.Capture()
		now = now.Add(15 * time.Second)
	}
	if got := monitor.Snapshot(); len(got.Points) != 240 || !got.Points[0].At.Equal(monitor.startedAt.Add(15*time.Second)) {
		t.Fatalf("retained history = %d points, first = %v", len(got.Points), got.Points[0].At)
	}
	now = now.Add(time.Hour)
	if got := monitor.Snapshot(); len(got.Points) != 0 {
		t.Fatalf("expired history = %d points", len(got.Points))
	}
	monitor.Capture()
	if got := monitor.Snapshot(); len(got.Points) != 1 || !got.Points[0].GapBefore {
		t.Fatalf("history after long pause = %#v", got.Points)
	}
}

func TestRuntimeMonitorConcurrentCaptureSnapshotAndCancellation(t *testing.T) {
	t.Parallel()
	monitor := NewRuntimeMonitor(NewTransferCounters())
	monitor.process = func() (*float64, *uint64) {
		cpu, rss := 1.0, uint64(4096)
		return &cpu, &rss
	}
	var work sync.WaitGroup
	for range 4 {
		work.Go(func() {
			for range 30 {
				monitor.Capture()
				_ = monitor.Snapshot()
			}
		})
	}
	work.Wait()
	count := len(monitor.Snapshot().Points)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	go func() {
		monitor.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled monitor did not exit")
	}
	if len(monitor.Snapshot().Points) != count {
		t.Fatal("Run captured an extra baseline")
	}
	if other := NewRuntimeMonitor(NewTransferCounters()); other.instance == monitor.instance || len(other.Snapshot().Points) != 0 {
		t.Fatal("monitor instances share identity or history")
	}
}

func TestReadProcessMetricsPreservesIndependentMissingValues(t *testing.T) {
	t.Parallel()
	zero, rss := 0.0, 4096.0
	gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return []*dto.MetricFamily{
			{Name: new("process_cpu_seconds_total"), Metric: []*dto.Metric{{Counter: &dto.Counter{Value: &zero}}}},
			{Name: new("process_resident_memory_bytes"), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: &rss}}}},
		}, nil
	})
	cpu, memory := readProcessMetrics(gatherer)
	if cpu == nil || *cpu != 0 || memory == nil || *memory != 4096 {
		t.Fatalf("process metrics = %v, %v", cpu, memory)
	}
	missing := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return []*dto.MetricFamily{{Name: new("process_cpu_seconds_total"), Metric: []*dto.Metric{{Counter: &dto.Counter{}}}}}, nil
	})
	if cpu, rss := readProcessMetrics(missing); cpu != nil || rss != nil {
		t.Fatalf("missing metrics = %v, %v, want nil", cpu, rss)
	}
	partial := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return []*dto.MetricFamily{{Name: new("process_resident_memory_bytes"), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: &rss}}}}}, errors.New("CPU unavailable")
	})
	if cpu, rss := readProcessMetrics(partial); cpu != nil || rss == nil || *rss != 4096 {
		t.Fatalf("partial metrics = %v, %v, want missing CPU and valid RSS", cpu, rss)
	}
}

func controlledRuntimeMonitor(now *time.Time, cpu *float64) (*RuntimeMonitor, *TransferCounters) {
	counters := NewTransferCounters()
	monitor := NewRuntimeMonitor(counters)
	monitor.startedAt = *now
	monitor.now = func() time.Time { return *now }
	monitor.process = func() (*float64, *uint64) {
		cpuValue, rss := *cpu, uint64(4096)
		return &cpuValue, &rss
	}
	return monitor, counters
}
