package provider

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/types"
)

func TestCheckHealth_HTTPStatus(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		want       string
	}{
		{name: "ok", statusCode: http.StatusOK, want: "reachable"},
		{name: "4xx response", statusCode: http.StatusMethodNotAllowed, want: "unreachable"},
		{name: "service unavailable", statusCode: http.StatusServiceUnavailable, want: "unreachable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("method = %q, want GET", r.Method)
				}
				if r.URL.Path != "/pdp/ping" {
					t.Errorf("path = %q, want /pdp/ping", r.URL.Path)
				}
				w.WriteHeader(tt.statusCode)
			}))
			defer srv.Close()

			status := CheckHealth(context.Background(), srv.URL, 2*time.Second)
			if status != tt.want {
				t.Errorf("status = %q, want %q", status, tt.want)
			}
		})
	}
}

func TestCheckHealth_Unreachable(t *testing.T) {
	status := CheckHealth(context.Background(), "http://127.0.0.1:1", 1*time.Second)
	if status != "unreachable" {
		t.Errorf("expected unreachable, got %q", status)
	}
}

func TestCheckHealthLogsClientConstructionError(t *testing.T) {
	var logs bytes.Buffer
	checker := NewHealthChecker(nil)
	checker.logger = slog.New(slog.NewTextHandler(&logs, nil))

	status := checker.Check(context.Background(), "ftp://provider.example", time.Second)

	if status != "unreachable" {
		t.Fatalf("status = %q, want unreachable", status)
	}
	if !strings.Contains(logs.String(), "failed to create PDP health client") {
		t.Fatalf("logs = %q, want PDP client construction warning", logs.String())
	}
}

func TestCheckHealth_EmptyURL(t *testing.T) {
	status := CheckHealth(context.Background(), "", 1*time.Second)
	if status != "n/a" {
		t.Errorf("expected n/a, got %q", status)
	}
}

func TestCheckHealth_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	status := CheckHealth(context.Background(), srv.URL, 100*time.Millisecond)
	if status != "unreachable" {
		t.Errorf("expected unreachable (timeout), got %q", status)
	}
}

func TestHealthCheckerUsesProvidedClient(t *testing.T) {
	transport := &recordingRoundTripper{}
	checker := NewHealthChecker(&http.Client{Transport: transport})

	if status := checker.Check(context.Background(), "https://provider.example", time.Second); status != "reachable" {
		t.Fatalf("status = %q, want reachable", status)
	}
	if transport.calls != 1 {
		t.Fatalf("round trip calls = %d, want 1", transport.calls)
	}
	if transport.method != http.MethodGet {
		t.Fatalf("method = %q, want GET", transport.method)
	}
	if transport.path != "/pdp/ping" {
		t.Fatalf("path = %q, want /pdp/ping", transport.path)
	}
}

func TestCheckHealthBatch(t *testing.T) {
	reachableSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer reachableSrv.Close()

	providers := []ProviderDetail{
		{ID: types.NewOnChainID(1), ServiceURL: reachableSrv.URL, HealthStatus: "skipped"},
		{ID: types.NewOnChainID(2), ServiceURL: "http://127.0.0.1:1", HealthStatus: "skipped"},
		{ID: types.NewOnChainID(3), ServiceURL: "", HealthStatus: "skipped"},
	}

	CheckHealthBatch(context.Background(), providers, 2*time.Second)

	if providers[0].HealthStatus != "reachable" {
		t.Errorf("provider 1: expected reachable, got %q", providers[0].HealthStatus)
	}
	if providers[1].HealthStatus != "unreachable" {
		t.Errorf("provider 2: expected unreachable, got %q", providers[1].HealthStatus)
	}
	if providers[2].HealthStatus != "n/a" {
		t.Errorf("provider 3: expected n/a, got %q", providers[2].HealthStatus)
	}
}

func TestCheckHealthBatch_Empty(t *testing.T) {
	// Should not panic on empty slice.
	CheckHealthBatch(context.Background(), nil, time.Second)
	CheckHealthBatch(context.Background(), []ProviderDetail{}, time.Second)
}

type recordingRoundTripper struct {
	calls  int
	method string
	path   string
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	r.method = req.Method
	r.path = req.URL.Path
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
		Request:    req,
	}, nil
}
