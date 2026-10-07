package synapse

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/strahe/synapse-go/pdp"
)

func TestCreateDataSetRefusalClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		method  string
		path    string
		status  int
		refused bool
	}{
		{"bad request", http.MethodPost, "/pdp/data-sets", 400, true},
		{"unauthorized", http.MethodPost, "/pdp/data-sets", 401, true},
		{"forbidden", http.MethodPost, "/pdp/data-sets", 403, true},
		{"status forbidden", http.MethodGet, "/pdp/data-sets/created/0xabc", 403, false},
		{"other post", http.MethodPost, "/pdp/piece", 400, false},
		{"rate limited", http.MethodPost, "/pdp/data-sets", 429, false},
		{"server failure", http.MethodPost, "/pdp/data-sets", 500, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := &pdp.HTTPError{Method: tc.method, URL: "https://provider.example" + tc.path, StatusCode: tc.status}
			got := normalizeCreateDataSetError(t.Context(), fmt.Errorf("create: %w", cause))
			rejection, refused := errors.AsType[*DataSetProviderRejectionError](got)
			if refused != tc.refused || (refused && rejection.StatusCode != tc.status) || !errors.Is(got, cause) {
				t.Fatalf("classification = %T %v, refused=%v", got, got, refused)
			}
		})
	}
	for _, cause := range []error{errors.New("provider returned HTTP 403"), pdp.ErrLocationHeader} {
		if _, refused := errors.AsType[*DataSetProviderRejectionError](normalizeCreateDataSetError(t.Context(), cause)); refused {
			t.Fatalf("non-response %v classified as refusal", cause)
		}
	}
}
