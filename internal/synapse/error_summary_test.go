package synapse

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/strahe/synapse-go/pdp"
)

func TestErrorSummary(t *testing.T) {
	providerErr := fmt.Errorf("storage.DataSetContext.SubmitCommit: add pieces: %w", &pdp.HTTPError{
		Method: "POST", URL: "https://provider.example/pdp/data-sets/7/pieces", StatusCode: 500,
		Body: "failed to add pieces:\n  piece not found",
	})
	rpcErr := fmt.Errorf("reading receipt: %w", &url.Error{
		Op: "Post", URL: "https://rpc.example/v1/secret-key?token=secret-token", Err: errors.New("connection refused"),
	})
	cases := []struct {
		name    string
		err     error
		want    string
		without string
	}{
		{name: "nil", err: nil, want: ""},
		{
			name: "provider reply is kept without its URL", err: providerErr,
			want: "provider returned HTTP 500: failed to add pieces: piece not found", without: "provider.example",
		},
		{
			name: "request URL is reduced to its origin", err: rpcErr,
			want: `reading receipt: Post "https://rpc.example": connection refused`, without: "secret",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ErrorSummary(tc.err)
			if got != tc.want {
				t.Fatalf("ErrorSummary() = %q, want %q", got, tc.want)
			}
			if tc.without != "" && strings.Contains(got, tc.without) {
				t.Fatalf("ErrorSummary() = %q, must not contain %q", got, tc.without)
			}
		})
	}
}

func TestErrorSummaryBoundsLongReplies(t *testing.T) {
	got := ErrorSummary(&pdp.HTTPError{StatusCode: 502, Body: strings.Repeat("é", 2*errorSummaryLimit)})
	if utf8.RuneCountInString(got) != errorSummaryLimit+1 || !strings.HasSuffix(got, "…") {
		t.Fatalf("ErrorSummary() kept %d runes, want %d plus an ellipsis", utf8.RuneCountInString(got), errorSummaryLimit)
	}
}
