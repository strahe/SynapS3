package synapse

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/strahe/synapse-go/pdp"
)

func TestCreateDataSetErrorsRetainProviderEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"bad request", http.MethodPost, "/pdp/data-sets", 400},
		{"unauthorized", http.MethodPost, "/pdp/data-sets", 401},
		{"forbidden", http.MethodPost, "/pdp/data-sets", 403},
		{"status forbidden", http.MethodGet, "/pdp/data-sets/created/0xabc", 403},
		{"other post", http.MethodPost, "/pdp/piece", 400},
		{"rate limited", http.MethodPost, "/pdp/data-sets", 429},
		{"server failure", http.MethodPost, "/pdp/data-sets", 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := &pdp.HTTPError{Method: tc.method, URL: "https://provider.example" + tc.path, StatusCode: tc.status}
			got := normalizeCreateDataSetError(t.Context(), fmt.Errorf("create: %w", cause))
			response, ok := errors.AsType[*pdp.HTTPError](got)
			if !ok || response != cause || !errors.Is(got, cause) {
				t.Fatalf("provider evidence = %T %v", got, got)
			}
		})
	}
	for _, cause := range []error{errors.New("provider returned HTTP 403"), pdp.ErrLocationHeader} {
		if got := normalizeCreateDataSetError(t.Context(), cause); !errors.Is(got, cause) {
			t.Fatalf("error cause lost: %v", got)
		}
	}
}
