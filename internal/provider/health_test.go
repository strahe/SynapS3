package provider

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/types"
)

func TestProbeHTTPResponses(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		location   string
		body       string
		wantStatus string
		wantDetail string
	}{
		{name: "ok", statusCode: http.StatusOK, body: "curio-pdp", wantStatus: "reachable"},
		{name: "wrong service identity", statusCode: http.StatusOK, body: "not-pdp", wantStatus: "unreachable", wantDetail: "Service URL did not answer as a PDP provider"},
		{name: "4xx response", statusCode: http.StatusMethodNotAllowed, wantStatus: "unreachable", wantDetail: "Health check returned HTTP 405"},
		{name: "service unavailable", statusCode: http.StatusServiceUnavailable, wantStatus: "unreachable", wantDetail: "Health check returned HTTP 503"},
		{name: "redirect", statusCode: http.StatusFound, location: "/elsewhere", wantStatus: "unreachable", wantDetail: "Health check was redirected (HTTP 302)"},
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
				if tt.location != "" {
					w.Header().Set("Location", tt.location)
				}
				w.WriteHeader(tt.statusCode)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			got := NewHealthChecker(nil).Probe(context.Background(), srv.URL, 2*time.Second)
			if got.Status != tt.wantStatus || got.Detail != tt.wantDetail {
				t.Errorf("probe = %+v, want status %q detail %q", got, tt.wantStatus, tt.wantDetail)
			}
		})
	}
}

func TestProbeTLSVerificationFailures(t *testing.T) {
	cert := &x509.Certificate{DNSNames: []string{"other.example"}}
	tests := []struct {
		name string
		err  error
	}{
		{name: "unknown authority", err: x509.UnknownAuthorityError{Cert: cert}},
		{name: "hostname mismatch", err: x509.HostnameError{Certificate: cert, Host: "provider.example"}},
		{name: "expired", err: x509.CertificateInvalidError{Cert: cert, Reason: x509.Expired, Detail: "certificate has expired"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &recordingRoundTripper{err: &tls.CertificateVerificationError{
				UnverifiedCertificates: []*x509.Certificate{cert}, Err: tt.err,
			}}
			checker := NewHealthChecker(&http.Client{Transport: transport})
			got := checker.Probe(t.Context(), "https://provider.example", time.Second)
			if got.Status != "unreachable" || got.Detail != "Provider TLS certificate could not be verified" {
				t.Errorf("probe = %+v, want unreachable with TLS verification failure detail", got)
			}
		})
	}
}

func TestProbeConnectionRefused(t *testing.T) {
	got := NewHealthChecker(nil).Probe(context.Background(), "http://127.0.0.1:1", time.Second)
	if got.Status != "unreachable" || got.Detail != "Provider refused the connection" {
		t.Errorf("probe = %+v, want unreachable with refused connection detail", got)
	}
}

func TestProbeConnectionClosedBeforeReply(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer srv.Close()

	got := NewHealthChecker(nil).Probe(context.Background(), srv.URL, 2*time.Second)
	if got.Status != "unreachable" || got.Detail != "Provider closed the connection before replying" {
		t.Errorf("probe = %+v, want unreachable with closed connection detail", got)
	}
}

func TestCheckHealthLogsClientConstructionError(t *testing.T) {
	var logs bytes.Buffer
	checker := NewHealthChecker(nil)
	checker.logger = slog.New(slog.NewTextHandler(&logs, nil))

	got := checker.Probe(context.Background(), "ftp://provider.example", time.Second)

	if got.Status != "unreachable" || got.Detail != "Service URL is invalid" {
		t.Fatalf("probe = %+v, want unreachable with invalid URL detail", got)
	}
	if !strings.Contains(logs.String(), "failed to create PDP health client") {
		t.Fatalf("logs = %q, want PDP client construction warning", logs.String())
	}
}

func TestCheckHealth_EmptyURL(t *testing.T) {
	if status := CheckHealth(context.Background(), "", 1*time.Second); status != "n/a" {
		t.Errorf("expected n/a, got %q", status)
	}
	got := NewHealthChecker(nil).Probe(context.Background(), "", time.Second)
	if got.Status != "n/a" || got.Detail != "Provider has no service URL" {
		t.Errorf("probe = %+v, want n/a with missing URL detail", got)
	}
}

func TestCheckHealth_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	got := NewHealthChecker(nil).Probe(context.Background(), srv.URL, 100*time.Millisecond)
	if got.Status != "unreachable" || got.Detail != "Health check timed out" {
		t.Errorf("probe = %+v, want unreachable with timeout detail", got)
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
		_, _ = io.WriteString(w, "curio-pdp")
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
	err    error
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	r.method = req.Method
	r.path = req.URL.Path
	if r.err != nil {
		return nil, r.err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("curio-pdp")),
		Request:    req,
	}, nil
}
