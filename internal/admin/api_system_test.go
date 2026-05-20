package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/strahe/synaps3/internal/buildinfo"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/testutil"
)

func newSystemAPITestServer(t *testing.T) *Server {
	t.Helper()

	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	return New(":0", db, &stubCache{rootDir: t.TempDir(), usedByte: 42}, 100, repos, &stubWorkerHealth{health: map[string]bool{"uploader": true}}, nil, config.DefaultFilecoinCopies, testLogger())
}

func serveSystemAPI(server *Server, path string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/system/info", server.handleAPISystemInfo)
	mux.HandleFunc("GET /api/v1/workers", server.handleAPIWorkers)
	mux.HandleFunc("GET /api/v1/cache/stats", server.handleAPICacheStats)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	mux.ServeHTTP(rec, req)
	return rec
}

func TestHandleAPISystemInfo(t *testing.T) {
	server := newSystemAPITestServer(t)
	rec := serveSystemAPI(server, "/api/v1/system/info")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var res systemInfoResponse
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode system info: %v", err)
	}
	if res.Version != buildinfo.Version {
		t.Fatalf("version = %q, want %q", res.Version, buildinfo.Version)
	}
	if res.Commit != buildinfo.Commit {
		t.Fatalf("commit = %q, want %q", res.Commit, buildinfo.Commit)
	}
	if res.BuildDate != buildinfo.Date {
		t.Fatalf("build_date = %q, want %q", res.BuildDate, buildinfo.Date)
	}
	if res.UptimeSeconds < 0 {
		t.Fatalf("uptime_seconds = %d, want >= 0", res.UptimeSeconds)
	}
}

func TestHandleAPIWorkers(t *testing.T) {
	server := newSystemAPITestServer(t)
	rec := serveSystemAPI(server, "/api/v1/workers")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var res workerStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode workers: %v", err)
	}
	if res.Workers == nil {
		t.Fatal("workers = nil, want map")
	}
	if !res.Workers["uploader"] {
		t.Fatalf("workers[uploader] = false, want true")
	}
}

func TestHandleAPICacheStats(t *testing.T) {
	server := newSystemAPITestServer(t)
	rec := serveSystemAPI(server, "/api/v1/cache/stats")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var res cacheStatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode cache stats: %v", err)
	}
	if res.UsedBytes != 42 {
		t.Fatalf("used_bytes = %d, want 42", res.UsedBytes)
	}
	if res.MaxBytes != 100 {
		t.Fatalf("max_bytes = %d, want 100", res.MaxBytes)
	}
}
