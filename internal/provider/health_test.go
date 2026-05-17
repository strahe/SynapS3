package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/types"
)

func TestCheckHealth_Reachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	status := CheckHealth(context.Background(), srv.URL, 2*time.Second)
	if status != "reachable" {
		t.Errorf("expected reachable, got %q", status)
	}
}

func TestCheckHealth_Unreachable(t *testing.T) {
	status := CheckHealth(context.Background(), "http://127.0.0.1:1", 1*time.Second)
	if status != "unreachable" {
		t.Errorf("expected unreachable, got %q", status)
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
	if transport.method != http.MethodHead {
		t.Fatalf("method = %q, want HEAD", transport.method)
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
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	r.method = req.Method
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
		Request:    req,
	}, nil
}
