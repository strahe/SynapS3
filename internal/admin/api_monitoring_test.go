package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/observability"
)

func TestAPIMonitoringAuthenticatedSnapshot(t *testing.T) {
	counters := observability.NewTransferCounters()
	read := func(body string) {
		t.Helper()
		if _, err := io.Copy(io.Discard, counters.WrapReader(observability.ClientUpload, strings.NewReader(body))); err != nil {
			t.Fatal(err)
		}
	}
	read("payload")
	monitor := observability.NewRuntimeMonitor(counters)
	monitor.Capture()
	read("unsampled")

	// No database, cache, or provider dependency is supplied to this endpoint.
	srv := newTestAuthServer(t, "admin-password").WithMonitoring(monitor, counters)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/monitoring", srv.handleAPIMonitoring)
	handler := srv.withAdminAuth(mux)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/monitoring", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", rr.Code)
	}

	var instanceID string
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/monitoring", nil)
		req.SetBasicAuth("admin", "admin-password")
		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK || rr.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("status = %d, cache control = %q", rr.Code, rr.Header().Get("Cache-Control"))
		}
		var snapshot observability.MonitoringSnapshot
		if err := json.Unmarshal(rr.Body.Bytes(), &snapshot); err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Points) != 1 || snapshot.Totals.Client.UploadBytes != 7 {
			t.Fatalf("request changed sampled history or totals: %+v", snapshot)
		}
		if snapshot.SampledAt == nil || !snapshot.SampledAt.Equal(snapshot.Points[0].At) {
			t.Fatal("sample timestamp does not match history")
		}
		if snapshot.IntervalSeconds != 15 || snapshot.RetentionSeconds != 3600 || snapshot.WindowEnd.Sub(snapshot.WindowStart) != time.Hour {
			t.Fatalf("unexpected monitoring window: %+v", snapshot)
		}
		if snapshot.InstanceID == "" || (instanceID != "" && snapshot.InstanceID != instanceID) {
			t.Fatal("instance identity changed between reads")
		}
		instanceID = snapshot.InstanceID
		if !strings.Contains(rr.Body.String(), `"upload_bytes":"7"`) {
			t.Fatal("cumulative bytes must be encoded as strings")
		}
	}
}

func TestAPIMonitoringUnavailable(t *testing.T) {
	rr := httptest.NewRecorder()
	(&Server{}).handleAPIMonitoring(rr, httptest.NewRequest(http.MethodGet, "/api/v1/monitoring", nil))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status = %d, cache control = %q", rr.Code, rr.Header().Get("Cache-Control"))
	}
}
