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
	"time"

	"github.com/strahe/synaps3/internal/providerbenchmark"
)

const uploadProbeTimeout = 180 * time.Second

var uploadSessionPath = regexp.MustCompile(`(?:^|/)pdp/piece/uploads/([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})$`)

type PDPBatchUploadProbe struct {
	client  *http.Client
	timeout time.Duration
}

func NewPDPBatchUploadProbe(allowPrivate bool) *PDPBatchUploadProbe {
	return &PDPBatchUploadProbe{client: newPDPStatusHTTPClient(0, allowPrivate), timeout: uploadProbeTimeout}
}

// Probe measures only the PUT phase; the upload session is intentionally not finalized.
func (p *PDPBatchUploadProbe) Probe(ctx context.Context, serviceURL string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	base, err := parsePDPHTTPURL(serviceURL, "provider service URL")
	if err != nil || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return 0, errors.New("invalid provider service URL")
	}
	base = cloneUploadBaseURL(base)
	createURL := base.ResolveReference(&url.URL{Path: "pdp/piece/uploads"})
	payload := make([]byte, providerbenchmark.SampleBytes)
	if _, err := rand.Read(payload); err != nil {
		return 0, fmt.Errorf("preparing upload sample: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	createReq, err := http.NewRequestWithContext(ctx, http.MethodPost, createURL.String(), nil)
	if err != nil {
		return 0, err
	}
	createResp, err := p.client.Do(createReq)
	if err != nil {
		return 0, err
	}
	defer func() { _ = createResp.Body.Close() }()
	_, _ = io.CopyN(io.Discard, createResp.Body, 1024)
	if createResp.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("upload session creation returned HTTP %d", createResp.StatusCode)
	}
	uuid, err := checkedUploadSessionID(createResp.Header.Get("Location"), createURL)
	if err != nil {
		return 0, err
	}
	putURL := base.ResolveReference(&url.URL{Path: "pdp/piece/uploads/" + uuid})
	// An opaque ReadCloser prevents net/http from constructing a replayable body.
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL.String(), io.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		return 0, err
	}
	putReq.ContentLength = providerbenchmark.SampleBytes
	putReq.Header.Set("Content-Type", "application/octet-stream")
	started := time.Now()
	putResp, err := p.client.Do(putReq)
	if err != nil {
		return 0, err
	}
	duration := time.Since(started)
	defer func() { _ = putResp.Body.Close() }()
	_, _ = io.CopyN(io.Discard, putResp.Body, 1024)
	if putResp.StatusCode != http.StatusNoContent {
		return 0, fmt.Errorf("upload sample returned HTTP %d", putResp.StatusCode)
	}
	return duration, nil
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
