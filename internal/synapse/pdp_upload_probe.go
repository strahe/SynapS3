package synapse

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/providerbenchmark"
)

var uploadSessionPath = regexp.MustCompile(`(?:^|/)pdp/piece/uploads/([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})$`)

var errUploadMeasurementWindow = errors.New("upload measurement window ended")

type PDPBatchUploadProbe struct {
	client               *http.Client
	measurementWindow    time.Duration
	sessionCreateTimeout time.Duration
	minimumSampleBytes   int64
}

func NewPDPBatchUploadProbe(allowPrivate bool) *PDPBatchUploadProbe {
	return &PDPBatchUploadProbe{
		client:               NewProviderHTTPClient(0, allowPrivate),
		measurementWindow:    providerbenchmark.MeasurementWindow,
		sessionCreateTimeout: providerbenchmark.SessionCreateTimeout,
		minimumSampleBytes:   providerbenchmark.MinimumSampleBytes,
	}
}

// Probe measures only the PUT phase; the upload session is intentionally not finalized.
func (p *PDPBatchUploadProbe) Probe(ctx context.Context, serviceURL string) (providerbenchmark.Measurement, error) {
	if err := ctx.Err(); err != nil {
		return providerbenchmark.Measurement{}, err
	}
	base, err := parsePDPHTTPURL(serviceURL, "provider service URL")
	if err != nil || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return providerbenchmark.Measurement{}, errors.New("invalid provider service URL")
	}
	base = cloneUploadBaseURL(base)
	createURL := base.ResolveReference(&url.URL{Path: "pdp/piece/uploads"})
	payload := make([]byte, providerbenchmark.SampleBytes)
	if _, err := rand.Read(payload); err != nil {
		return providerbenchmark.Measurement{}, fmt.Errorf("preparing upload sample: %w", err)
	}
	createCtx, cancelCreate := context.WithTimeout(ctx, p.sessionCreateTimeout)
	defer cancelCreate()
	createReq, err := http.NewRequestWithContext(createCtx, http.MethodPost, createURL.String(), nil)
	if err != nil {
		return providerbenchmark.Measurement{}, err
	}
	createResp, err := p.client.Do(createReq)
	if err != nil {
		return providerbenchmark.Measurement{}, err
	}
	if createResp.StatusCode != http.StatusCreated {
		responseErr := readProviderHTTPError(createReq, createResp)
		_ = createResp.Body.Close()
		return providerbenchmark.Measurement{}, fmt.Errorf("could not create upload session: %w", responseErr)
	}
	_, _ = io.CopyN(io.Discard, createResp.Body, 1024)
	_ = createResp.Body.Close()
	cancelCreate()
	uuid, err := checkedUploadSessionID(createResp.Header.Get("Location"), createURL)
	if err != nil {
		return providerbenchmark.Measurement{}, err
	}
	putURL := base.ResolveReference(&url.URL{Path: "pdp/piece/uploads/" + uuid})
	putCtx, cancelPut := context.WithTimeoutCause(ctx, p.measurementWindow, errUploadMeasurementWindow)
	defer cancelPut()
	body := &uploadCountingReader{reader: bytes.NewReader(payload)}
	// An opaque ReadCloser prevents net/http from constructing a replayable body.
	putReq, err := http.NewRequestWithContext(putCtx, http.MethodPut, putURL.String(), io.NopCloser(body))
	if err != nil {
		return providerbenchmark.Measurement{}, err
	}
	putReq.ContentLength = providerbenchmark.SampleBytes
	putReq.Header.Set("Content-Type", "application/octet-stream")
	started := time.Now()
	putResp, err := p.client.Do(putReq)
	duration := time.Since(started)
	if err != nil {
		if context.Cause(putCtx) == errUploadMeasurementWindow && ctx.Err() == nil {
			if count := body.bytes.Load(); count >= p.minimumSampleBytes {
				return providerbenchmark.Measurement{Bytes: count, Duration: duration}, nil
			}
		}
		return providerbenchmark.Measurement{}, err
	}
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusNoContent {
		return providerbenchmark.Measurement{}, fmt.Errorf("could not upload sample: %w", readProviderHTTPError(putReq, putResp))
	}
	_, _ = io.CopyN(io.Discard, putResp.Body, 1024)
	return providerbenchmark.Measurement{Bytes: providerbenchmark.SampleBytes, Duration: duration}, nil
}

type uploadCountingReader struct {
	reader io.Reader
	bytes  atomic.Int64
}

func (r *uploadCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

func cloneUploadBaseURL(base *url.URL) *url.URL {
	copy := *base
	if !strings.HasSuffix(copy.Path, "/") {
		copy.Path += "/"
	}
	return &copy
}

func checkedUploadSessionID(location string, createURL *url.URL) (string, error) {
	parsed, err := url.Parse(location)
	if err != nil || parsed == nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return "", errors.New("invalid upload session location")
	}
	if parsed.IsAbs() && (parsed.Scheme != createURL.Scheme || !strings.EqualFold(parsed.Host, createURL.Host)) {
		return "", errors.New("upload session location changed origin")
	}
	if parsed.Host != "" && !parsed.IsAbs() {
		return "", errors.New("invalid upload session location")
	}
	match := uploadSessionPath.FindStringSubmatch(parsed.Path)
	if len(match) != 2 {
		return "", errors.New("invalid upload session path")
	}
	return match[1], nil
}
