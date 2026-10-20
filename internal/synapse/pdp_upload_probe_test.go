package synapse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synapse-go/pdp"
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
	measurement, err := probe.Probe(t.Context(), server.URL+"/proxy")
	if err != nil || measurement.Duration <= 0 || measurement.Bytes != providerbenchmark.SampleBytes {
		t.Fatalf("Probe = %v, %v", measurement, err)
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
		_, _ = io.WriteString(w, "provider upload disabled; eid=request-42")
	}))
	defer server.Close()
	probe := NewPDPBatchUploadProbe(true)
	if _, err := probe.Probe(t.Context(), server.URL); err == nil || !strings.Contains(err.Error(), "HTTP 307") || !strings.Contains(err.Error(), "eid=request-42") {
		t.Fatalf("redirected PUT = %v", err)
	}
	if len(requests) != 2 || requests[0] != "POST /pdp/piece/uploads" || requests[1] != "PUT /pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("requests = %v", requests)
	}
}

type uploadProbeTransport func(*http.Request) (*http.Response, error)

func TestProviderHTTPErrorRetainsStatusAndIncompleteBody(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  io.Reader
		want  string
		cause error
	}{
		{name: "response body", body: strings.NewReader("provider disabled; eid=request-42"), want: "eid=request-42"},
		{name: "read failure", body: io.MultiReader(strings.NewReader("eid=request-43\n"), iotest.ErrReader(io.ErrUnexpectedEOF)), want: "could not read complete provider response", cause: io.ErrUnexpectedEOF},
		{name: "body limit", body: strings.NewReader(strings.Repeat("a", providerErrorResponseLimit+1)), want: "remaining body not read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "https://provider.test/pdp/piece/uploads", nil)
			err := readProviderHTTPError(req, &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(tc.body)})
			httpErr, ok := errors.AsType[*pdp.HTTPError](err)
			got := errtext.Format(err)
			if !ok || httpErr.StatusCode != http.StatusServiceUnavailable || !strings.Contains(got, tc.want) || !strings.Contains(got, "POST https://provider.test/pdp/piece/uploads") || len(got) > errtext.MaxBytes {
				t.Fatalf("provider diagnostic = %s", got)
			}
			if tc.cause != nil && (!errors.Is(err, tc.cause) || !strings.Contains(got, "eid=request-43")) {
				t.Fatalf("partial response cause lost: %v", err)
			}
		})
	}
}

func (f uploadProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPDPBatchUploadProbeMeasurementWindow(t *testing.T) {
	for _, tc := range []struct {
		name              string
		bytes             int64
		createTimeout     bool
		cancelParent      bool
		cancelAfterWindow bool
		transportFailure  bool
		wantError         error
	}{
		{name: "partial sample", bytes: providerbenchmark.MinimumSampleBytes},
		{name: "below minimum", bytes: providerbenchmark.MinimumSampleBytes - 1, wantError: context.DeadlineExceeded},
		{name: "session timeout", createTimeout: true, wantError: context.DeadlineExceeded},
		{name: "parent cancellation", bytes: providerbenchmark.MinimumSampleBytes, cancelParent: true, wantError: context.Canceled},
		{name: "parent canceled at window", bytes: providerbenchmark.MinimumSampleBytes, cancelAfterWindow: true, wantError: context.DeadlineExceeded},
		{name: "transport failure", bytes: providerbenchmark.MinimumSampleBytes, transportFailure: true, wantError: io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			probe := NewPDPBatchUploadProbe(true)
			probe.measurementWindow = 20 * time.Millisecond
			probe.sessionCreateTimeout = 20 * time.Millisecond
			probe.client.Transport = uploadProbeTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost {
					if tc.createTimeout {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return &http.Response{
						StatusCode: http.StatusCreated,
						Header:     http.Header{"Location": []string{"/pdp/piece/uploads/123e4567-e89b-12d3-a456-426614174000"}},
						Body:       io.NopCloser(strings.NewReader("")),
					}, nil
				}
				defer func() { _ = r.Body.Close() }()
				if r.GetBody != nil {
					t.Error("upload sample body is replayable")
				}
				if _, err := io.CopyN(io.Discard, r.Body, tc.bytes); err != nil {
					return nil, err
				}
				if tc.transportFailure {
					return nil, io.ErrUnexpectedEOF
				}
				if tc.cancelParent {
					cancel()
				}
				<-r.Context().Done()
				if tc.cancelAfterWindow {
					cancel()
				}
				return nil, r.Context().Err()
			})
			measurement, err := probe.Probe(ctx, "https://provider.example")
			if tc.wantError != nil {
				if !errors.Is(err, tc.wantError) || measurement.Bytes != 0 {
					t.Fatalf("Probe = %#v, %v; want %v", measurement, err, tc.wantError)
				}
				return
			}
			if err != nil || measurement.Bytes != tc.bytes || measurement.Duration < probe.measurementWindow {
				t.Fatalf("Probe = %#v, %v", measurement, err)
			}
		})
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
