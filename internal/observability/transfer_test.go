package observability

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

func TestTransferCountersMeasurePartialPayloadAndPreserveStreams(t *testing.T) {
	t.Parallel()
	counters := NewTransferCounters()
	errTransfer := errors.New("transfer interrupted")
	reader := &partialTransfer{err: errTransfer}
	wrapped := counters.WrapReadCloser(ClientUpload, reader)
	n, err := wrapped.Read(make([]byte, 8))
	if n != 3 || !errors.Is(err, errTransfer) {
		t.Fatalf("Read = (%d, %v), want (3, interrupted)", n, err)
	}
	if err := wrapped.Close(); !errors.Is(err, errTransfer) || !reader.closed {
		t.Fatalf("Close = %v, closed = %v", err, reader.closed)
	}
	writer := counters.WrapWriter(ClientDownload, &partialTransfer{err: errTransfer})
	if n, err := writer.Write([]byte("payload")); n != 3 || !errors.Is(err, errTransfer) {
		t.Fatalf("Write = (%d, %v), want (3, interrupted)", n, err)
	}
	if _, err := io.Copy(io.Discard, counters.WrapReader(StorageDownload, strings.NewReader("stored"))); err != nil {
		t.Fatal(err)
	}
	if _, err := counters.WrapWriter(StorageUpload, io.Discard).Write([]byte("upload")); err != nil {
		t.Fatal(err)
	}
	if got := counters.Totals(); got != (TransferTotals{Client: TransferByteTotals{3, 3}, Storage: TransferByteTotals{6, 6}}) {
		t.Fatalf("Totals = %#v", got)
	}
	var absent *TransferCounters
	if absent.WrapReader(ClientUpload, reader) != reader || absent.WrapReadCloser(ClientUpload, reader) != reader || absent.WrapWriter(ClientDownload, reader) != reader {
		t.Fatal("nil counters must preserve original streams")
	}
}

func TestTransferCountersConcurrentTotalsAndPrometheusIsolation(t *testing.T) {
	t.Parallel()
	counters := NewTransferCounters()
	var writers sync.WaitGroup
	for range 8 {
		writers.Go(func() {
			writer := counters.WrapWriter(StorageUpload, io.Discard)
			for range 100 {
				if _, err := writer.Write([]byte("payload")); err != nil {
					t.Error(err)
				}
				_ = counters.Totals()
			}
		})
	}
	writers.Wait()
	if got := counters.Totals().Storage.UploadBytes; got != 5600 {
		t.Fatalf("concurrent total = %d, want 5600", got)
	}
	families, err := counters.Gatherer().Gather()
	if err != nil || len(families) != 1 || len(families[0].Metric) != 4 {
		t.Fatalf("Gather = %v, %v", families, err)
	}
	for _, metric := range families[0].Metric {
		labels := map[string]string{}
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		want := float64(0)
		if labels["scope"] == "storage" && labels["direction"] == "upload" {
			want = 5600
		}
		if got := metric.Counter.GetValue(); got != want {
			t.Fatalf("counter %v = %v, want %v", labels, got, want)
		}
	}
	other := NewTransferCounters()
	if other.Totals() != (TransferTotals{}) {
		t.Fatal("new counter instance shares previous totals")
	}
}

func TestTransferTotalsEncodeLargeCountersAsDecimalStrings(t *testing.T) {
	t.Parallel()
	counters := NewTransferCounters()
	counters.bytes[ClientUpload].Store(1<<53 + 1)
	data, err := json.Marshal(counters.Totals())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"upload_bytes":"9007199254740993"`) {
		t.Fatalf("totals JSON = %s", data)
	}
}

type partialTransfer struct {
	err    error
	closed bool
}

func (p *partialTransfer) Read(b []byte) (int, error) { return copy(b, "abc"), p.err }
func (p *partialTransfer) Write([]byte) (int, error)  { return 3, p.err }
func (p *partialTransfer) Close() error {
	p.closed = true
	return p.err
}
