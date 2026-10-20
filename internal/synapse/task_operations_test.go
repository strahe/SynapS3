package synapse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func storeSessionTestTarget(t *testing.T, serviceURL string, client *http.Client) *dataSetTargetAdapter {
	t.Helper()
	ref, err := storage.NewDataSetRef(sdktypes.NewBigInt(101), sdktypes.NewBigInt(202), sdktypes.NewBigInt(303))
	if err != nil {
		t.Fatal(err)
	}
	dataSet, err := storage.NewDataSetContext(storage.Provider{ID: ref.ProviderID(), ServiceURL: serviceURL}, &inertPDPProviderClient{}, nil, ref)
	if err != nil {
		t.Fatal(err)
	}
	target := newDataSetTargetAdapter(dataSet, nil)
	target.providerHTTP = client
	return target
}

func TestProviderCandidateWaitKeepsProbeFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cause error
		wait  bool
	}{
		{name: "empty selection", wait: true},
		{name: "healthy absence", cause: fmt.Errorf("selection: %w", storage.ErrNoHealthyProviders), wait: true},
		{name: "partial selection", cause: &storage.InsufficientUploadContextsError{Requested: 2, Available: 1}, wait: true},
		{name: "probe failed", cause: errors.Join(storage.ErrNoHealthyProviders, errors.New("provider request failed"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsProviderCandidateWait(&NoProviderCandidatesError{Cause: tc.cause}); got != tc.wait {
				t.Fatalf("candidate wait = %t, want %t", got, tc.wait)
			}
		})
	}
}

func TestTaskStoreSessionStages(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = "12345678-1234-4234-8234-123456789abc"
	const uploadPath = "/provider/api/pdp/piece/uploads"
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == uploadPath:
			w.Header().Set("Location", "/pdp/piece/uploads/"+sessionID)
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPut && r.URL.Path == uploadPath+"/"+sessionID:
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil || !bytes.Equal(body, payload) || r.ContentLength != int64(len(payload)) {
				t.Errorf("upload body mismatch: size=%d, error=%v", r.ContentLength, readErr)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == uploadPath+"/"+sessionID:
			var body struct {
				PieceCID string `json:"pieceCid"`
			}
			if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil || body.PieceCID != info.CIDv2.String() {
				t.Errorf("finalize identity mismatch: %s, %v", body.PieceCID, decodeErr)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	serviceURL := server.URL + "/provider/api"
	target := storeSessionTestTarget(t, serviceURL, server.Client())
	counters := observability.NewTransferCounters()
	target.transferCounters = counters
	id, err := target.CreateStoreSession(t.Context())
	if err != nil || id != sessionID {
		t.Fatalf("CreateStoreSession = %q, %v", id, err)
	}
	var progress int64
	result, err := target.UploadStoreSession(t.Context(), serviceURL, id, bytes.NewReader(payload), &storage.StoreOptions{
		PieceCID: info.CIDv2, OnProgress: func(n int64) { progress = n },
	})
	if err != nil || result == nil || !result.PieceCID.Equals(info.CIDv2) || result.Size != int64(len(payload)) || progress != int64(len(payload)) {
		t.Fatalf("UploadStoreSession = %#v, %v; progress=%d", result, err, progress)
	}
	if err := target.FinalizeStoreSession(t.Context(), serviceURL, id, info.CIDv2); err != nil {
		t.Fatal(err)
	}
	if got := counters.Totals().Storage.UploadBytes; got != uint64(len(payload)) {
		t.Errorf("storage upload bytes = %d, want %d", got, len(payload))
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"POST " + uploadPath, "PUT " + uploadPath + "/" + id, "POST " + uploadPath + "/" + id}; !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

func TestTaskStoreSessionErrorsPreserveHTTPStatus(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	const id = "12345678-1234-4234-8234-123456789abc"
	for _, tc := range []struct {
		name     string
		finalize bool
		status   int
		want     error
	}{
		{name: "upload missing", status: http.StatusNotFound, want: ErrStoreSessionMissing},
		{name: "upload claimed", status: http.StatusConflict, want: ErrStoreSessionClaimed},
		{name: "finalize missing", finalize: true, status: http.StatusNotFound, want: ErrStoreSessionMissing},
		{name: "finalize incomplete", finalize: true, status: http.StatusConflict, want: ErrStoreSessionIncomplete},
		{name: "provider unavailable", status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests++
				mu.Unlock()
				_, _ = io.Copy(io.Discard, r.Body)
				http.Error(w, "provider response", tc.status)
			}))
			defer server.Close()
			target := storeSessionTestTarget(t, server.URL, server.Client())
			var got error
			if tc.finalize {
				got = target.FinalizeStoreSession(t.Context(), server.URL, id, info.CIDv2)
			} else {
				_, got = target.UploadStoreSession(t.Context(), server.URL, id, bytes.NewReader(payload), &storage.StoreOptions{PieceCID: info.CIDv2})
			}
			if tc.want != nil && !errors.Is(got, tc.want) {
				t.Fatalf("session error = %v, want %v", got, tc.want)
			}
			if errors.Is(got, ErrStoreUploadTruncated) {
				t.Fatalf("fully read request was reported as truncated: %v", got)
			}
			httpErr, ok := errors.AsType[*pdp.HTTPError](got)
			if !ok || httpErr.StatusCode != tc.status || !strings.Contains(httpErr.Body, "provider response") {
				t.Fatalf("HTTP classification = %#v, %v", httpErr, got)
			}
			mu.Lock()
			defer mu.Unlock()
			if requests != 1 {
				t.Fatalf("provider request count = %d, want 1", requests)
			}
		})
	}
}

func TestTaskStoreSessionTruncatedUploadPreservesReadError(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	target := storeSessionTestTarget(t, server.URL, server.Client())
	body := io.MultiReader(bytes.NewReader(payload[:len(payload)/2]), iotest.ErrReader(io.ErrUnexpectedEOF))
	result, err := target.UploadStoreSession(t.Context(), server.URL, "12345678-1234-4234-8234-123456789abc", body, &storage.StoreOptions{PieceCID: info.CIDv2})
	if result != nil || !errors.Is(err, ErrStoreUploadTruncated) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated upload = %#v, %v", result, err)
	}
}

func TestTaskStoreSessionCancellation(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	const id = "12345678-1234-4234-8234-123456789abc"
	for _, stage := range []string{"create", "upload", "finalize"} {
		t.Run(stage, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(entered)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			target := storeSessionTestTarget(t, server.URL, server.Client())
			counters := observability.NewTransferCounters()
			target.transferCounters = counters
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var callErr error
				switch stage {
				case "create":
					_, callErr = target.CreateStoreSession(ctx)
				case "upload":
					_, callErr = target.UploadStoreSession(ctx, server.URL, id, bytes.NewReader(payload), &storage.StoreOptions{PieceCID: info.CIDv2})
				case "finalize":
					callErr = target.FinalizeStoreSession(ctx, server.URL, id, info.CIDv2)
				}
				done <- callErr
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("session request did not reach the provider")
			}
			cancel()
			select {
			case callErr := <-done:
				if !errors.Is(callErr, context.Canceled) {
					t.Fatalf("cancelled request = %v", callErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("session request did not stop after cancellation")
			}
			var wantBytes uint64
			if stage == "upload" {
				wantBytes = uint64(len(payload))
			}
			if got := counters.Totals().Storage.UploadBytes; got != wantBytes {
				t.Errorf("cancelled %s upload bytes = %d, want %d", stage, got, wantBytes)
			}
		})
	}
}

func TestTaskStoreSessionKeepsOriginalProviderURL(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	const id = "12345678-1234-4234-8234-123456789abc"
	var mu sync.Mutex
	var originalCalls int
	original := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		originalCalls++
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer original.Close()
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("original session reached the current provider: %s", r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer current.Close()
	target := storeSessionTestTarget(t, current.URL, original.Client())
	if _, err := target.UploadStoreSession(t.Context(), original.URL, id, bytes.NewReader(payload), &storage.StoreOptions{PieceCID: info.CIDv2}); err != nil {
		t.Fatal(err)
	}
	if err := target.FinalizeStoreSession(t.Context(), original.URL, id, info.CIDv2); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if originalCalls != 2 {
		t.Fatalf("original provider requests = %d, want 2", originalCalls)
	}
}

func TestTaskStoreSessionRejectsUnsafeLocation(t *testing.T) {
	t.Parallel()
	for _, location := range []string{
		"https://other.example/pdp/piece/uploads/12345678-1234-4234-8234-123456789abc",
		"//other.example/pdp/piece/uploads/12345678-1234-4234-8234-123456789abc",
		"/pdp/piece/uploads/12345678-1234-4234-8234-123456789abc?token=secret",
		"/pdp/piece/uploads/12345678-1234-4234-8234-123456789abc#fragment",
		"/pdp/piece/uploads/not-a-uuid",
	} {
		t.Run(location, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusCreated)
			}))
			defer server.Close()
			target := storeSessionTestTarget(t, server.URL, server.Client())
			if id, err := target.CreateStoreSession(t.Context()); err == nil || id != "" {
				t.Fatalf("unsafe session location = %q, %v", id, err)
			}
		})
	}
}

func TestTaskStoreSessionPreservesProviderURLPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		allowPrivate bool
		redirect     bool
	}{
		{name: "private address"},
		{name: "redirect", allowPrivate: true, redirect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Error("upload session followed a redirect")
				w.WriteHeader(http.StatusCreated)
			}))
			defer redirected.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !tc.redirect {
					t.Error("upload session reached a forbidden private address")
				}
				w.Header().Set("Location", redirected.URL+"/pdp/piece/uploads/12345678-1234-4234-8234-123456789abc")
				w.WriteHeader(http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			target := storeSessionTestTarget(t, server.URL, NewProviderHTTPClient(0, tc.allowPrivate))
			if id, err := target.CreateStoreSession(t.Context()); err == nil || id != "" {
				t.Fatalf("unsafe session request = %q, %v", id, err)
			}
		})
	}
}

func TestTaskStoreSessionProgressPanicReturnsToCaller(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	target := storeSessionTestTarget(t, server.URL, server.Client())
	counters := observability.NewTransferCounters()
	target.transferCounters = counters
	defer func() {
		if got := recover(); got != "progress callback failed" {
			t.Errorf("progress panic = %v", got)
		}
		if got := counters.Totals().Storage.UploadBytes; got != 0 {
			t.Errorf("storage upload bytes = %d, want 0 for withheld body", got)
		}
	}()
	_, _ = target.UploadStoreSession(t.Context(), server.URL, "12345678-1234-4234-8234-123456789abc", bytes.NewReader(payload), &storage.StoreOptions{
		PieceCID: info.CIDv2, OnProgress: func(int64) { panic("progress callback failed") },
	})
}

type blockedStoreUploadBody struct {
	entered   chan struct{}
	closed    chan struct{}
	exited    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func (r *blockedStoreUploadBody) Read([]byte) (int, error) {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.closed
	close(r.exited)
	return 0, io.ErrClosedPipe
}

func (r *blockedStoreUploadBody) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func TestTaskStoreSessionEarlyResponseStopsBodyRead(t *testing.T) {
	t.Parallel()
	body := &blockedStoreUploadBody{entered: make(chan struct{}), closed: make(chan struct{}), exited: make(chan struct{})}
	defer func() { _ = body.Close() }()
	info, err := piece.CalculateFromBytes(bytes.Repeat([]byte{0xab}, 512))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
			t.Error(err)
			return
		}
		select {
		case <-body.entered:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = http.NewResponseController(w).Flush()
	}))
	defer server.Close()
	target := storeSessionTestTarget(t, server.URL, server.Client())
	done := make(chan error, 1)
	go func() {
		_, err := target.UploadStoreSession(t.Context(), server.URL, "12345678-1234-4234-8234-123456789abc", body, &storage.StoreOptions{PieceCID: info.CIDv2})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("early provider response completed the upload")
		}
		select {
		case <-body.exited:
		default:
			t.Fatal("upload returned while its source was still being read")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("early provider response did not stop the upload source")
	}
}

func TestTaskDataSetObservationUsesOneStatusRequest(t *testing.T) {
	t.Parallel()
	hash := common.HexToHash("0x1234")
	confirmedHash := common.HexToHash("0x5678")
	for _, state := range []string{"pending", "confirmed", "confirmed rejected", "failed", "reorged", "unavailable"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodGet || r.URL.Path != "/pdp/data-sets/create/"+hash.Hex() {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				if state == "unavailable" {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				body := map[string]any{"createMessageHash": hash.Hex(), "txStatus": state, "dataSetCreated": state == "confirmed"}
				if state == "confirmed" {
					body["dataSetId"], body["ok"], body["confirmedTxHash"] = 202, true, confirmedHash.Hex()
				}
				if state == "confirmed rejected" {
					body["txStatus"], body["ok"], body["confirmedTxHash"] = "confirmed", false, confirmedHash.Hex()
				}
				if err := json.NewEncoder(w).Encode(body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			provider, err := storage.NewProviderContext(storage.Provider{ID: sdktypes.NewBigInt(101), ServiceURL: server.URL}, &inertPDPProviderClient{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			target := newProviderTargetAdapter(provider)
			target.providerHTTP = server.Client()
			result, err := target.WaitForDataSetCreated(t.Context(), server.URL+"/pdp/data-sets/create/"+hash.Hex(), sdktypes.NewBigInt(303))
			if calls != 1 {
				t.Fatalf("status requests = %d, want 1", calls)
			}
			if state == "unavailable" {
				if err == nil {
					t.Fatal("provider error was lost")
				}
				return
			}
			if state == "failed" || state == "reorged" || state == "confirmed rejected" {
				txStatus := state
				if state == "confirmed rejected" {
					txStatus = "confirmed"
				}
				if !errors.Is(err, ErrProviderTransactionRejected) || !strings.Contains(err.Error(), "tx_status="+fmt.Sprintf("%q", txStatus)) || result == nil || result.TransactionID != hash.Hex() {
					t.Fatalf("rejected observation result=%#v err=%v", result, err)
				}
				if state == "confirmed rejected" && result.ConfirmedTransactionID != confirmedHash.Hex() {
					t.Fatalf("rejected confirmation hash=%s", result.ConfirmedTransactionID)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if state == "pending" && result != nil {
				t.Fatalf("pending result = %#v", result)
			}
			if state == "confirmed" && (result == nil || result.DataSet.DataSetID().String() != "202" || result.DataSet.ClientDataSetID().String() != "303") {
				t.Fatalf("confirmed result = %#v", result)
			}
		})
	}
}
