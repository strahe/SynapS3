package errtext

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synapse-go/pdp"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestRedact(t *testing.T) {
	r := newRedactor("https://rpc.example/v1/rpc-path-key?token=rpc-query-token", "0xabcdef0123456789", "session-secret-value")
	cases := []struct {
		name    string
		text    string
		want    string
		without []string
	}{
		{
			name: "invalid database text is visible",
			text: "provider\x00reply\xff",
			want: `provider\u0000reply\xff`,
		},
		{
			name: "provider reply and URL path are kept",
			text: `pdp: POST https://sp.example/pdp/piece/pull: HTTP 409: dataset is terminated`,
			want: `pdp: POST https://sp.example/pdp/piece/pull: HTTP 409: dataset is terminated`,
		},
		{
			name:    "URL credentials, query, and fragment are removed",
			text:    `Get "https://user:pass@sp.example/piece/bafk?auth=abc#frag": EOF`,
			want:    `Get "https://sp.example/piece/bafk": EOF`,
			without: []string{"user", "pass", "auth=abc", "frag"},
		},
		{
			name:    "RPC URL keeps only its origin",
			text:    `reading receipt: Post "https://rpc.example/v1/rpc-path-key?token=rpc-query-token": connection refused`,
			want:    `reading receipt: Post "https://rpc.example": connection refused`,
			without: []string{"rpc-path-key", "rpc-query-token"},
		},
		{
			name:    "configured secrets are replaced with or without hex prefix",
			text:    "signing with abcdef0123456789 failed; secret session-secret-value rejected",
			want:    "signing with [redacted] failed; secret [redacted] rejected",
			without: []string{"abcdef0123456789", "session-secret-value"},
		},
		{
			name: "trailing punctuation stays outside the URL",
			text: "provider unreachable (https://sp.example/pdp?x=1).",
			want: "provider unreachable (https://sp.example/pdp).",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := r.redact(tc.text)
			if got != tc.want {
				t.Fatalf("redact() = %q, want %q", got, tc.want)
			}
			for _, secret := range tc.without {
				if strings.Contains(got, secret) {
					t.Fatalf("redact() = %q, still contains %q", got, secret)
				}
			}
		})
	}
}

func TestRedactTruncatesOnRuneBoundary(t *testing.T) {
	r := &Redactor{}
	text := "begin\n" + strings.Repeat("é", MaxBytes) + "\nprovider request ID: final-id"
	got := r.redact(text)
	if !utf8.ValidString(got) || len(got) > MaxBytes || !strings.HasPrefix(got, "begin\n") || !strings.HasSuffix(got, "provider request ID: final-id") || !strings.Contains(got, fmt.Sprintf("redacted text was %d bytes", len(text))) {
		t.Fatalf("redact() returned %d bytes, valid UTF-8 %v", len(got), utf8.ValidString(got))
	}
	if again := r.redact(got); again != got {
		t.Fatal("redaction is not idempotent")
	}
	secret := "secret-at-cut-boundary"
	raw := strings.Repeat("a", MaxBytes/2) + secret + strings.Repeat("b", MaxBytes)
	if got := New("", secret).Redact(raw); strings.Contains(got, "secret-at-") || strings.Contains(got, "boundary") {
		t.Fatal("truncation exposed a partial secret")
	}
}

type rpcDiagnosticError struct {
	code int
	data any
}

func (e rpcDiagnosticError) Error() string  { return "execution reverted: provider failed" }
func (e rpcDiagnosticError) ErrorCode() int { return e.code }
func (e rpcDiagnosticError) ErrorData() any { return e.data }

func TestFormatPreservesRPCDiagnosticsAndProtectsSignedTransactions(t *testing.T) {
	cause := rpcDiagnosticError{code: 3, data: "0xdeadbeef"}
	got := Format(fmt.Errorf("could not submit: %w", cause))
	if !strings.Contains(got, "could not submit: execution reverted") || !strings.Contains(got, "RPC code: 3") || !strings.Contains(got, "Revert data: 0xdeadbeef") {
		t.Fatalf("missing RPC details: %s", got)
	}
	for _, data := range []any{"private-key", map[string]any{"secret": "value"}} {
		got := Format(rpcDiagnosticError{code: 3, data: data})
		if strings.Contains(got, "private-key") || strings.Contains(got, "value") {
			t.Fatalf("uncontrolled RPC data: %s", got)
		}
	}
	if got := Format(rpcDiagnosticError{code: -32000, data: "0xdeadbeef"}); strings.Contains(got, "Revert data") {
		t.Fatal("non-revert RPC data was exposed")
	}
	tx := types.NewTx(&types.LegacyTx{Nonce: 1, Gas: 21000, GasPrice: big.NewInt(2)})
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	encoded := hex.EncodeToString(raw)
	broadcast := &sdktypes.BroadcastError{Transaction: tx, Cause: rpcDiagnosticError{code: 3, data: "0x" + encoded}}
	protected := RedactError(broadcast)
	if strings.Contains(protected.Error(), encoded) || !errors.Is(protected, broadcast) {
		t.Fatalf("signed transaction protection failed: %s", protected)
	}
	joined := Format(errors.Join(protected, rpcDiagnosticError{code: -32001}))
	if strings.Contains(joined, encoded) || !strings.Contains(joined, "RPC code: -32001") {
		t.Fatalf("joined RPC diagnostic = %s", joined)
	}
}

func TestScopedRedactorCombinesRuntimeAndDraftSecrets(t *testing.T) {
	previous := active.Load()
	t.Cleanup(func() { active.Store(previous) })
	Configure("https://runtime-rpc.test/runtime-path", "runtime-secret")
	draft := New("https://draft-rpc.test/draft-path", "draft-secret")
	got := draft.Redact("runtime-secret draft-secret https://runtime-rpc.test/runtime-path https://draft-rpc.test/draft-path")
	if strings.Contains(got, "secret") || strings.Contains(got, "-path") {
		t.Fatalf("scoped secret leakage: %s", got)
	}
	if got := Redact("draft-secret"); got != "draft-secret" {
		t.Fatalf("draft changed global configuration: %s", got)
	}
}

func TestFormatPreservesProviderContextAndIdentity(t *testing.T) {
	cause := &pdp.HTTPError{Method: "POST", URL: "https://provider.example/pdp/data-sets/7/pieces?token=secret", StatusCode: 500, Body: "failed to add pieces:\n  piece not found"}
	err := fmt.Errorf("could not register pieces: %w", cause)
	got := RedactError(err)
	for _, want := range []string{"could not register pieces", "https://provider.example/pdp/data-sets/7/pieces", "HTTP 500", "failed to add pieces:\n  piece not found"} {
		if !strings.Contains(got.Error(), want) {
			t.Fatalf("diagnostic missing %q: %s", want, got)
		}
	}
	if strings.Contains(got.Error(), "secret") || !errors.Is(got, cause) || Format(got) != got.Error() {
		t.Fatal("diagnostic redaction or identity is unstable")
	}
}
