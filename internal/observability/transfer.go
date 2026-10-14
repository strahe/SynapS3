package observability

import (
	"io"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Flow identifies an object payload transfer boundary and direction.
type Flow uint8

const (
	ClientUpload Flow = iota
	ClientDownload
	StorageUpload
	StorageDownload
)

// TransferByteTotals holds cumulative payload bytes at one boundary.
type TransferByteTotals struct {
	UploadBytes   uint64 `json:"upload_bytes,string"`
	DownloadBytes uint64 `json:"download_bytes,string"`
}

// TransferTotals contains process-local cumulative payload bytes.
type TransferTotals struct {
	Client  TransferByteTotals `json:"client"`
	Storage TransferByteTotals `json:"storage"`
}

// TransferCounters measures bytes actually read or written by payload streams.
type TransferCounters struct {
	bytes    [4]atomic.Uint64
	registry *prometheus.Registry
}

// NewTransferCounters creates isolated counters and their Prometheus gatherer.
func NewTransferCounters() *TransferCounters {
	c := &TransferCounters{registry: prometheus.NewRegistry()}
	for flow, labels := range []struct{ scope, direction string }{
		{"client", "upload"},
		{"client", "download"},
		{"storage", "upload"},
		{"storage", "download"},
	} {
		counter := &c.bytes[flow]
		c.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "synaps3",
			Subsystem: "transfer",
			Name:      "bytes_total",
			Help:      "Total object payload bytes transferred",
			ConstLabels: prometheus.Labels{
				"scope": labels.scope, "direction": labels.direction,
			},
		}, func() float64 { return float64(counter.Load()) }))
	}
	return c
}

func (c *TransferCounters) counter(flow Flow) *atomic.Uint64 {
	if c == nil || int(flow) >= len(c.bytes) {
		return nil
	}
	return &c.bytes[flow]
}

// WrapReader counts bytes returned by Read, including partial failed reads.
func (c *TransferCounters) WrapReader(flow Flow, r io.Reader) io.Reader {
	if counter := c.counter(flow); counter != nil && r != nil {
		return &transferReader{reader: r, counter: counter}
	}
	return r
}

// WrapReadCloser counts reads and preserves the stream's Close behavior.
func (c *TransferCounters) WrapReadCloser(flow Flow, r io.ReadCloser) io.ReadCloser {
	if counter := c.counter(flow); counter != nil && r != nil {
		return &transferReadCloser{reader: r, counter: counter}
	}
	return r
}

// WrapWriter counts bytes accepted by Write, including partial failed writes.
func (c *TransferCounters) WrapWriter(flow Flow, w io.Writer) io.Writer {
	if counter := c.counter(flow); counter != nil && w != nil {
		return &transferWriter{writer: w, counter: counter}
	}
	return w
}

// Totals reads the cumulative counters without collecting Prometheus metrics.
func (c *TransferCounters) Totals() TransferTotals {
	if c == nil {
		return TransferTotals{}
	}
	return TransferTotals{
		Client:  TransferByteTotals{UploadBytes: c.bytes[ClientUpload].Load(), DownloadBytes: c.bytes[ClientDownload].Load()},
		Storage: TransferByteTotals{UploadBytes: c.bytes[StorageUpload].Load(), DownloadBytes: c.bytes[StorageDownload].Load()},
	}
}

// Gatherer exposes only this instance's payload transfer counters.
func (c *TransferCounters) Gatherer() prometheus.Gatherer { return c.registry }

type transferReader struct {
	reader  io.Reader
	counter *atomic.Uint64
}

func (r *transferReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.counter.Add(uint64(n))
	}
	return n, err
}

type transferReadCloser struct {
	reader  io.ReadCloser
	counter *atomic.Uint64
}

func (r *transferReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.counter.Add(uint64(n))
	}
	return n, err
}

func (r *transferReadCloser) Close() error { return r.reader.Close() }

type transferWriter struct {
	writer  io.Writer
	counter *atomic.Uint64
}

func (w *transferWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		w.counter.Add(uint64(n))
	}
	return n, err
}
