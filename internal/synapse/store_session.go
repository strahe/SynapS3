package synapse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
)

const storeControlTimeout = 30 * time.Second

var (
	ErrStoreSessionMissing    = errors.New("upload session is unavailable")
	ErrStoreSessionClaimed    = errors.New("upload session is already claimed")
	ErrStoreSessionIncomplete = errors.New("upload session is incomplete")
)

func (c *dataSetTargetAdapter) CreateStoreSession(ctx context.Context) (string, error) {
	endpoint, err := storeSessionURL(c.ServiceURL(), "")
	if err != nil {
		return "", err
	}
	controlCtx, cancel := context.WithTimeout(ctx, storeControlTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(controlCtx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.storeSessionRequest(req, http.StatusCreated, nil)
	if err != nil {
		return "", NormalizeProviderOperationError(ctx, err)
	}
	return checkedUploadSessionID(resp.Header.Get("Location"), endpoint)
}

func (c *dataSetTargetAdapter) UploadStoreSession(ctx context.Context, serviceURL, sessionID string, reader io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
	if sessionID == "" || reader == nil || opts == nil || !opts.PieceCID.Defined() {
		return nil, errors.New("upload submission requires bytes and a piece identity")
	}
	info, err := piece.ParseV2(opts.PieceCID)
	if err != nil {
		return nil, err
	}
	if info.RawSize > chain.MaxUploadSize {
		return nil, errors.New("upload submission exceeds the provider upload limit")
	}
	endpoint, err := storeSessionURL(serviceURL, sessionID)
	if err != nil {
		return nil, err
	}
	counted := &storeUploadReader{reader: reader, maximum: int64(info.RawSize), progress: opts.OnProgress}
	// An opaque body prevents net/http from replaying the upload.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), counted)
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(info.RawSize)
	req.Header.Set("Content-Type", "application/octet-stream")
	_, err = c.storeSessionRequest(req, http.StatusNoContent, ErrStoreSessionClaimed)
	uploaded, panicValue, panicked := counted.finish()
	if panicked {
		panic(panicValue)
	}
	if err != nil {
		return nil, NormalizeProviderOperationError(ctx, err)
	}
	if uploaded != int64(info.RawSize) {
		return nil, errors.New("upload submission returned before all bytes were sent")
	}
	return &storage.StoreResult{PieceCID: opts.PieceCID, Size: uploaded}, nil
}

func (c *dataSetTargetAdapter) FinalizeStoreSession(ctx context.Context, serviceURL, sessionID string, pieceCID cid.Cid) error {
	if sessionID == "" {
		return errors.New("upload session identity is required")
	}
	if _, err := piece.ParseV2(pieceCID); err != nil {
		return err
	}
	endpoint, err := storeSessionURL(serviceURL, sessionID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		PieceCID string `json:"pieceCid"`
	}{PieceCID: pieceCID.String()})
	if err != nil {
		return err
	}
	controlCtx, cancel := context.WithTimeout(ctx, storeControlTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(controlCtx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	_, err = c.storeSessionRequest(req, http.StatusOK, ErrStoreSessionIncomplete)
	return NormalizeProviderOperationError(ctx, err)
}

func storeSessionURL(serviceURL, sessionID string) (*url.URL, error) {
	base, err := parsePDPHTTPURL(serviceURL, "provider service URL")
	if err != nil || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("invalid provider service URL")
	}
	base = cloneUploadBaseURL(base)
	createURL := base.ResolveReference(&url.URL{Path: "pdp/piece/uploads"})
	if sessionID == "" {
		return createURL, nil
	}
	checked, err := checkedUploadSessionID("/pdp/piece/uploads/"+sessionID, createURL)
	if err != nil || checked != sessionID {
		return nil, errors.New("invalid upload session identity")
	}
	return base.ResolveReference(&url.URL{Path: "pdp/piece/uploads/" + checked}), nil
}

func (c *dataSetTargetAdapter) storeSessionRequest(req *http.Request, success int, conflict error) (*http.Response, error) {
	client := c.providerHTTP
	if client == nil {
		client = NewProviderHTTPClient(0, false)
	}
	bounded := *client
	bounded.Timeout = 0
	req.Header.Set("User-Agent", pdp.DefaultUserAgent)
	resp, err := bounded.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, pdp.MaxControlResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > pdp.MaxControlResponseBytes {
		return nil, errors.New("upload session response exceeds the provider response limit")
	}
	if resp.StatusCode == success {
		return resp, nil
	}
	httpErr := &pdp.HTTPError{
		Method: req.Method, URL: req.URL.String(), StatusCode: resp.StatusCode,
		Body: strings.TrimSpace(string(body)),
	}
	if resp.StatusCode == http.StatusNotFound && conflict != nil {
		return nil, errors.Join(ErrStoreSessionMissing, httpErr)
	}
	if resp.StatusCode == http.StatusConflict && conflict != nil {
		return nil, errors.Join(conflict, httpErr)
	}
	return nil, httpErr
}

type storeUploadReader struct {
	reader   io.Reader
	maximum  int64
	progress func(int64)

	readMu      sync.Mutex
	activeReads sync.WaitGroup
	mu          sync.Mutex
	uploaded    int64
	finished    bool
	closed      bool
	closeOnce   sync.Once
	closeErr    error
	panicValue  any
	panicked    bool
}

func (r *storeUploadReader) Read(data []byte) (n int, err error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	r.activeReads.Add(1)
	r.mu.Unlock()
	defer r.activeReads.Done()
	n, err = r.reader.Read(data)
	if n <= 0 {
		return n, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.panicked {
		return 0, errors.New("upload progress callback failed")
	}
	r.uploaded += int64(n)
	if r.uploaded > r.maximum {
		return 0, errors.New("upload bytes exceed the expected piece size")
	}
	if !r.finished && r.progress != nil {
		defer func() {
			if value := recover(); value != nil {
				r.panicValue, r.panicked = value, true
				n, err = 0, errors.New("upload progress callback failed")
			}
		}()
		r.progress(r.uploaded)
	}
	return n, err
}

func (r *storeUploadReader) finish() (int64, any, bool) {
	_ = r.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finished = true
	return r.uploaded, r.panicValue, r.panicked
}

func (r *storeUploadReader) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		if closer, ok := r.reader.(io.Closer); ok {
			r.closeErr = closer.Close()
		}
		r.activeReads.Wait()
	})
	return r.closeErr
}
