package synapse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/strahe/synapse-go/pdp"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestProviderPiecesKnownSeparatesAbsenceFromObservationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/prefix/pdp/data-sets/11/pieces/") {
			t.Errorf("unexpected provider request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch strings.TrimPrefix(r.URL.Path, "/prefix/pdp/data-sets/11/pieces/") {
		case "7":
			_, _ = fmt.Fprint(w, `{"pieceId":7,"pieceCid":"ignored"}`)
		case "8":
			http.Error(w, "Piece not found", http.StatusNotFound)
		case "9":
			http.Error(w, "database unavailable", http.StatusInternalServerError)
		default:
			_, _ = fmt.Fprint(w, `{"pieceId":99}`)
		}
	}))
	defer server.Close()
	adapter := &StorageServiceAdapter{providerHTTP: server.Client()}
	result, err := adapter.ProviderPiecesKnown(t.Context(), server.URL+"/prefix", sdktypes.NewBigInt(11), []sdktypes.BigInt{
		sdktypes.NewBigInt(7), sdktypes.NewBigInt(8), sdktypes.NewBigInt(9), sdktypes.NewBigInt(10),
	})
	if err != nil || len(result.Known) != 1 || result.Known[0].String() != "7" || len(result.Unknown) != 1 || result.Unknown[0].String() != "8" || len(result.Errors) != 2 {
		t.Fatalf("provider states = %#v, err=%v", result, err)
	}
	var responseErr *pdp.HTTPError
	if !errors.As(result.Errors["9"], &responseErr) || responseErr.StatusCode != http.StatusInternalServerError || !strings.Contains(responseErr.Body, "database unavailable") {
		t.Fatalf("provider diagnostic = %v", result.Errors["9"])
	}
	if !strings.Contains(result.Errors["10"].Error(), "identity mismatch") {
		t.Fatalf("mismatched piece identity = %v", result.Errors["10"])
	}
}

func TestProviderPiecesKnownAllNotFoundRetainsFallbackEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer server.Close()
	adapter := &StorageServiceAdapter{providerHTTP: server.Client()}
	result, err := adapter.ProviderPiecesKnown(t.Context(), server.URL, sdktypes.NewBigInt(11), []sdktypes.BigInt{sdktypes.NewBigInt(7), sdktypes.NewBigInt(8)})
	if err != nil || len(result.Known) != 0 || len(result.Unknown) != 2 || len(result.Errors) != 0 {
		t.Fatalf("all unknown = %#v, err=%v", result, err)
	}
	for _, badURL := range []string{"https://user:password@example.test", "https://example.test?secret=x", "https://example.test/#fragment"} {
		if _, err := adapter.ProviderPiecesKnown(t.Context(), badURL, sdktypes.NewBigInt(11), nil); err == nil {
			t.Fatalf("unsafe provider URL was accepted: %s", badURL)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err = adapter.ProviderPiecesKnown(ctx, server.URL, sdktypes.NewBigInt(11), []sdktypes.BigInt{sdktypes.NewBigInt(7)})
	if !errors.Is(err, context.Canceled) || len(result.Unknown) != 0 {
		t.Fatalf("interrupted observation = %#v, err=%v", result, err)
	}
}

func TestClassifyRemovalRejection(t *testing.T) {
	for _, tt := range []struct {
		status int
		want   RemovalRejection
	}{
		{http.StatusNotFound, RemovalRejectedPieceNotFound},
		{http.StatusUnauthorized, RemovalRejectedByProvider},
		{http.StatusTooManyRequests, RemovalRejectedByProvider},
		{http.StatusInternalServerError, RemovalNotRejected},
	} {
		err := fmt.Errorf("delete pieces: %w", &pdp.HTTPError{StatusCode: tt.status})
		if got := ClassifyRemovalRejection(err); got != tt.want {
			t.Errorf("status %d = %v, want %v", tt.status, got, tt.want)
		}
	}
	if ClassifyRemovalRejection(context.DeadlineExceeded) != RemovalNotRejected {
		t.Fatal("an uncertain transport failure was treated as a refusal")
	}
}
