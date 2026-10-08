package synapse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

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

func TestTaskStoreSubmitsWithoutParkingPoll(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/pdp/piece/uploads":
			w.Header().Set("Location", "/pdp/piece/uploads/test")
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPut && r.URL.Path == "/pdp/piece/uploads/test":
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil || !bytes.Equal(body, payload) {
				t.Errorf("upload body mismatch: %v", readErr)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/pdp/piece/uploads/test":
			var body struct {
				PieceCID string `json:"pieceCid"`
			}
			if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil || body.PieceCID != info.CIDv2.String() {
				t.Errorf("finalize identity mismatch: %s, %v", body.PieceCID, decodeErr)
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	ref, err := storage.NewDataSetRef(sdktypes.NewBigInt(101), sdktypes.NewBigInt(202), sdktypes.NewBigInt(303))
	if err != nil {
		t.Fatal(err)
	}
	dataSet, err := storage.NewDataSetContext(storage.Provider{ID: ref.ProviderID(), ServiceURL: server.URL}, &inertPDPProviderClient{}, nil, ref)
	if err != nil {
		t.Fatal(err)
	}
	target := newDataSetTargetAdapter(dataSet, nil)
	target.providerHTTP = server.Client()
	result, err := target.Store(t.Context(), bytes.NewReader(payload), &storage.StoreOptions{PieceCID: info.CIDv2})
	if err != nil || result == nil || !result.PieceCID.Equals(info.CIDv2) {
		t.Fatalf("Store = %#v, %v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"POST /pdp/piece/uploads", "PUT /pdp/piece/uploads/test", "POST /pdp/piece/uploads/test"}; !slices.Equal(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

func TestTaskDataSetObservationUsesOneStatusRequest(t *testing.T) {
	t.Parallel()
	hash := common.HexToHash("0x1234")
	for _, state := range []string{"pending", "confirmed", "unavailable"} {
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
					body["dataSetId"], body["ok"] = 202, true
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
