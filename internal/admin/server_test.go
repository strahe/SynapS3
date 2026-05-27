package admin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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

// stubWorkerHealth implements WorkerHealthChecker for tests.
type stubWorkerHealth struct {
	health map[string]bool
}

func (s *stubWorkerHealth) WorkerHealth() map[string]bool { return s.health }

func TestHealthz_Healthy(t *testing.T) {
	db := testutil.NewTestDB(t)
	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir, usedByte: 42}

	srv := New(":0", db, sc, 107374182400, repository.NewRepositories(db), nil, nil, config.DefaultFilecoinCopies, testLogger())
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

	srv := New(":0", db, sc, 107374182400, repository.NewRepositories(db), nil, nil, config.DefaultFilecoinCopies, testLogger())
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

	srv := New(":0", db, sc, 107374182400, repository.NewRepositories(db), nil, nil, config.DefaultFilecoinCopies, testLogger())

	// Increment metrics so we can verify their presence.
	ObjectOperationsTotal.WithLabelValues("put", "success").Inc()
	WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
	WorkerTaskDuration.WithLabelValues("uploader").Observe(0.5)
	WorkerTaskDuration.WithLabelValues("uploader").Observe(0.5)

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

	srv := New(":0", db, sc, 107374182400, repository.NewRepositories(db), nil, nil, config.DefaultFilecoinCopies, testLogger())
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

func TestHealthz_WorkerUnhealthy(t *testing.T) {
	db := testutil.NewTestDB(t)
	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir}
	wh := &stubWorkerHealth{health: map[string]bool{
		"uploader": true,
		"onchain":  false,
	}}

	srv := New(":0", db, sc, 107374182400, repository.NewRepositories(db), wh, nil, config.DefaultFilecoinCopies, testLogger())
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
		if strings.Contains(e, "worker/onchain") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected worker/onchain error in errors, got %v", hr.Errors)
	}
}

func TestHealthz_WorkerHealthy(t *testing.T) {
	db := testutil.NewTestDB(t)
	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir}
	wh := &stubWorkerHealth{health: map[string]bool{
		"uploader": true,
		"onchain":  true,
		"evictor":  true,
		"proofset": true,
	}}

	srv := New(":0", db, sc, 107374182400, repository.NewRepositories(db), wh, nil, config.DefaultFilecoinCopies, testLogger())
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
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRefreshMetrics(t *testing.T) {
	db := testutil.NewTestDB(t)
	cacheDir := t.TempDir()
	sc := &stubCache{rootDir: cacheDir}

	repos := repository.NewRepositories(db)
	srv := New(":0", db, sc, 107374182400, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())

	ctx := context.Background()

	// Seed a queued task.
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J000000000000000TASK001",
		IdempotencyKey: "test-refresh-task",
		Status:         model.TaskStatusQueued,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("seeding task: %v", err)
	}

	// Seed an object.
	bucket := &model.Bucket{Name: "metrics-bucket", Status: model.BucketStatusActive}
	if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	versionID := model.NewVersionID()
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         "metrics.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
		State:       model.ObjectStateCached,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("seeding object: %v", err)
	}

	// Call refreshMetrics and verify gauges via /metrics endpoint.
	srv.refreshMetrics(ctx)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	text := string(body)
	for _, metric := range []string{
		"synaps3_task_queue_depth",
		"synaps3_object_state_distribution",
	} {
		if !strings.Contains(text, metric) {
			t.Errorf("metrics output missing %q", metric)
		}
	}
}

func TestWithSecurityHeaders(t *testing.T) {
	handler := withSecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	res := rec.Result()
	defer func() { _ = res.Body.Close() }()

	if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := res.Header.Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options = %q, want DENY", got)
	}
	if got := res.Header.Get("Referrer-Policy"); got != "strict-origin-when-cross-origin" {
		t.Errorf("Referrer-Policy = %q, want strict-origin-when-cross-origin", got)
	}
}
