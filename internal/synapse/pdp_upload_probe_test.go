package synapse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/providerbenchmark"
)

func TestPDPBatchUploadProbeUploadsSampleWithoutFinalizing(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/proxy/pdp/piece/uploads":
			w.Header().Set("Location", "/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000")
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPut && r.URL.Path == "/proxy/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000":
			if r.ContentLength != providerbenchmark.SampleBytes {
				t.Errorf("content length = %d", r.ContentLength)
			}
			if r.Header.Get("Content-Type") != "application/octet-stream" {
				t.Errorf("content type = %q", r.Header.Get("Content-Type"))
			}
			count, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			if int64(count) != providerbenchmark.SampleBytes {
				t.Errorf("body bytes = %d", count)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	probe := NewPDPBatchUploadProbe(true)
	duration, err := probe.Probe(t.Context(), server.URL+"/proxy")
	if err != nil || duration <= 0 {
		t.Fatalf("Probe = %v, %v", duration, err)
	}
	if got := strings.Join(requests, ","); got != "POST /proxy/pdp/piece/uploads,PUT /proxy/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("requests = %s", got)
	}
}

func TestPDPBatchUploadProbeRejectsForeignSessionLocation(t *testing.T) {
	var putCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalled = true
		}
		w.Header().Set("Location", "http://other.example/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	probe := NewPDPBatchUploadProbe(true)
	if _, err := probe.Probe(t.Context(), server.URL); err == nil {
		t.Fatal("foreign session location accepted")
	}
	if putCalled {
		t.Fatal("PUT was sent after invalid Location")
	}
}

func TestPDPBatchUploadProbeRejectsFailedPutWithoutFollowingRedirect(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000")
			w.WriteHeader(http.StatusCreated)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Location", "/redirect-target")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	probe := NewPDPBatchUploadProbe(true)
	if _, err := probe.Probe(t.Context(), server.URL); err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("redirected PUT = %v", err)
	}
	if len(requests) != 2 || requests[0] != "POST /pdp/piece/uploads" || requests[1] != "PUT /pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("requests = %v", requests)
	}
}

func TestPDPBatchUploadProbeTimesOutDuringPut(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000")
			w.WriteHeader(http.StatusCreated)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	probe := NewPDPBatchUploadProbe(true)
	probe.timeout = 20 * time.Millisecond
	if _, err := probe.Probe(t.Context(), server.URL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed out PUT = %v", err)
	}
}

func TestPDPBatchUploadProbeRespectsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	probe := NewPDPBatchUploadProbe(true)
	start := time.Now()
	if _, err := probe.Probe(ctx, "http://127.0.0.1:1"); err == nil {
		t.Fatal("cancelled context accepted")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancelled probe took too long")
	}
}
