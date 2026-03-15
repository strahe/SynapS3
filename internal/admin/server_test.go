package admin

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/testutil"
)

// stubCache implements the subset of cache.Cache needed for health checks.
type stubCache struct {
	cache.Cache
	rootDir  string
	usedByte int64
}

func (s *stubCache) RootDir() string  { return s.rootDir }
func (s *stubCache) UsedBytes() int64 { return s.usedByte }

func TestHealthz_Healthy(t *testing.T) {
	db := testutil.NewTestDB(t)
	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir, usedByte: 42}

	srv := New(":0", db, sc, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var hr healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		t.Fatal(err)
	}
	if hr.Status != "ok" {
		t.Fatalf("expected status ok, got %q", hr.Status)
	}
	if len(hr.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", hr.Errors)
	}
}

func TestHealthz_DBDown(t *testing.T) {
	db := testutil.NewTestDB(t)
	_ = db.Close() // force ping to fail

	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir}

	srv := New(":0", db, sc, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var hr healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		t.Fatal(err)
	}
	if hr.Status != "unhealthy" {
		t.Fatalf("expected status unhealthy, got %q", hr.Status)
	}
	if len(hr.Errors) == 0 {
		t.Fatal("expected errors, got none")
	}
}

func TestMetrics_Endpoint(t *testing.T) {
	db := testutil.NewTestDB(t)
	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir}

	srv := New(":0", db, sc, testLogger())

	// Increment metrics so we can verify their presence.
	ObjectOperationsTotal.WithLabelValues("put", "success").Inc()
	WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	text := string(body)
	for _, prefix := range []string{
		"synaps3_backend_object_operations_total",
		"synaps3_cache_used_bytes",
		"synaps3_cache_hits_total",
		"synaps3_cache_misses_total",
		"synaps3_worker_tasks_processed_total",
	} {
		if !strings.Contains(text, prefix) {
			t.Errorf("metrics output missing %q", prefix)
		}
	}

	// Verify Content-Type is Prometheus text format.
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") && !strings.Contains(ct, "text/openmetrics") {
		t.Errorf("unexpected content-type: %s", ct)
	}
}

func TestHealthz_CacheDirMissing(t *testing.T) {
	db := testutil.NewTestDB(t)
	sc := &stubCache{rootDir: "/nonexistent/path/that/does/not/exist"}

	srv := New(":0", db, sc, testLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealthz)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}

	var hr healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		t.Fatal(err)
	}
	if hr.Status != "unhealthy" {
		t.Fatalf("expected unhealthy, got %q", hr.Status)
	}
	found := false
	for _, e := range hr.Errors {
		if strings.Contains(e, "cache") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cache error in errors, got %v", hr.Errors)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
