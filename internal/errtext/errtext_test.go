package errtext

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
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
			name:    "RPC URL is replaced entirely",
			text:    `reading receipt: Post "https://rpc.example/v1/rpc-path-key?token=rpc-query-token": connection refused`,
			want:    `reading receipt: Post "[redacted]": connection refused`,
			without: []string{"rpc.example", "rpc-path-key", "rpc-query-token"},
		},
		{
			name: "bare RPC hosts are replaced case insensitively",
			text: "lookup RPC.EXAMPLE: rpc.example has no such host",
			want: "lookup [redacted]: [redacted] has no such host",
		},
		{
			name: "other provider hostnames are kept",
			text: "https://other-rpc.example/pdp and https://rpc.example.provider/pdp",
			want: "https://other-rpc.example/pdp and https://rpc.example.provider/pdp",
		},
		{
			name: "sentence punctuation follows a bare RPC host",
			text: "DNS lookup failed for rpc.example.",
			want: "DNS lookup failed for [redacted].",
		},
		{
			name: "provider paths are not scanned as bare hosts",
			text: "https://sp.example/pdp/rpc.example/piece",
			want: "https://sp.example/pdp/rpc.example/piece",
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

func TestFormatRedactsRPCNetworkErrors(t *testing.T) {
	for _, tc := range []struct {
		name, rpcURL, hostname string
	}{
		{"hostname", "https://user:password@rpc-host-token.example:8545/v1/path-token?key=query-token#fragment-token", "rpc-host-token.example"},
		{"IPv6", "https://user:password@[2001:db8::1]:8545/v1/path-token?key=query-token#fragment-token", "2001:db8::1"},
		{"DNS root dot", "https://user:password@rpc-host-token.example.", "rpc-host-token.example."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRedactor(tc.rpcURL)
			err := fmt.Errorf("reading receipt: %w", &url.Error{
				Op:  "Post",
				URL: tc.rpcURL,
				Err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{
					Name: tc.hostname, Err: "no such host",
				}},
			})
			got := r.Format(err)
			for _, secret := range []string{"user", "password", tc.hostname, "path-token", "query-token", "fragment-token"} {
				if strings.Contains(got, secret) {
					t.Fatalf("network error contains %q: %s", secret, got)
				}
			}
			for _, want := range []string{"reading receipt", "Post", "dial tcp", "lookup", "no such host", placeholder} {
				if !strings.Contains(got, want) {
					t.Fatalf("network error lost %q: %s", want, got)
				}
			}
			if r.Redact(got) != got {
				t.Fatal("network error redaction is not idempotent")
			}
		})
	}
}

func TestRPCHostRedactionBoundariesAndPlaceholders(t *testing.T) {
	for _, tc := range []struct {
		rpc, text, want string
	}{
		{"https://[2001:db8::1]:8545/rpc", "https://[2001:db8::1:2]/pdp", "https://[2001:db8::1:2]/pdp"},
		{"https://[2001:db8::1]:8545/rpc", "dial tcp [2001:db8::1]:8545: refused", "dial tcp [redacted]: refused"},
		{"https://[2001:db8::1]:8545/rpc", "address 2001:db8::1:2 refused", "address 2001:db8::1:2 refused"},
		{"https://tenant-secret.example.", `Post "https://tenant-secret.example.": refused`, `Post "[redacted]": refused`},
		{"https://tenant-secret.example.", "endpoint failed (https://tenant-secret.example.).", "endpoint failed ([redacted])."},
		{"http://redacted:8545", "http://redacted:8545/rpc", "[redacted]"},
	} {
		r := newRedactor(tc.rpc)
		if got := r.Redact(tc.text); got != tc.want || r.Redact(got) != got {
			t.Errorf("Redact(%q) = %q, want stable %q", tc.text, got, tc.want)
		}
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
	previous := active.Load()
	t.Cleanup(func() { active.Store(previous) })
	for _, tc := range []struct {
		name, rpcURL, providerURL, safeURL, secretHost string
	}{
		{"separate provider", "https://rpc.example:8545/rpc", "https://url-user:url-password@provider.example/pdp/data-sets/7/pieces?token=query-secret#fragment-secret", "https://provider.example/pdp/data-sets/7/pieces", "rpc.example"},
		{"shared hostname", "https://shared.example:8545/rpc", "https://url-user:url-password@shared.example:8080/pdp/data-sets/7/pieces?token=query-secret#fragment-secret", ":8080/pdp/data-sets/7/pieces", "shared.example"},
		{"shared IPv4", "http://127.0.0.1:1234/rpc/v1", "http://url-user:url-password@127.0.0.1:8080/pdp/data-sets/7/pieces?token=query-secret#fragment-secret", ":8080/pdp/data-sets/7/pieces", "127.0.0.1"},
		{"shared IPv6", "https://[2001:db8::1]:8545/rpc", "https://url-user:url-password@[2001:db8::1]:8080/pdp/data-sets/7/pieces?token=query-secret#fragment-secret", ":8080/pdp/data-sets/7/pieces", "2001:db8::1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			Configure(tc.rpcURL)
			cause := &pdp.HTTPError{Method: "POST", URL: tc.providerURL, StatusCode: 500, Body: "failed to add pieces:\n  piece not found"}
			err := fmt.Errorf("could not register pieces: %w", cause)
			got := RedactError(err)
			// The SDK escapes control characters in the provider's response.
			for _, want := range []string{"could not register pieces", tc.safeURL, "HTTP 500", `failed to add pieces:\n  piece not found`} {
				if !strings.Contains(got.Error(), want) {
					t.Fatalf("diagnostic missing %q: %s", want, got)
				}
			}
			for _, secret := range []string{tc.secretHost, "url-user", "url-password", "query-secret", "fragment-secret"} {
				if strings.Contains(got.Error(), secret) {
					t.Fatalf("diagnostic contains %q: %s", secret, got)
				}
			}
			if !errors.Is(got, cause) || Format(got) != got.Error() {
				t.Fatalf("diagnostic redaction or identity is unstable: identity=%t, first=%q, second=%q", errors.Is(got, cause), got.Error(), Format(got))
			}
		})
	}
}

// A provider body reaches the diagnostic whole and escaped: redaction covers
// all of it, in either form of a secret, before the shared limit truncates the
// middle.
func TestFormatKeepsTheWholeProviderResponse(t *testing.T) {
	previous := active.Load()
	t.Cleanup(func() { active.Store(previous) })
	secret := `provider-"secret\`
	Configure("", secret)
	for _, tc := range []struct {
		name      string
		body      string
		truncated bool
	}{
		{name: "within the SDK rendering", body: "rejected\n " + secret + " tail-marker"},
		{name: "beyond the SDK rendering", body: strings.Repeat("a", 5000) + "\n " + secret + " tail-marker"},
		{name: "beyond the diagnostic limit", body: "head-marker " + strings.Repeat("b", 2*MaxBytes) + "\n " + secret + " tail-marker", truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := &pdp.HTTPError{Method: "POST", URL: "https://provider.example/pdp/data-sets/7/pieces", StatusCode: 500, Body: tc.body}
			got := Format(fmt.Errorf("could not register pieces: %w", cause))
			for _, want := range []string{"could not register pieces: pdp: POST https://provider.example/pdp/data-sets/7/pieces: HTTP 500: ", `\n ` + placeholder + " tail-marker"} {
				if !strings.Contains(got, want) {
					t.Fatalf("diagnostic missing %q: %.200s ... %s", want, got, got[max(0, len(got)-200):])
				}
			}
			if strings.Contains(got, "secret") || strings.Contains(got, "bytes omitted") || len(got) > MaxBytes {
				t.Fatalf("diagnostic leaked, kept the SDK prefix, or exceeded the limit: %d bytes", len(got))
			}
			if strings.Contains(got, "[truncated: redacted text was") != tc.truncated || (tc.truncated && !strings.Contains(got, "head-marker")) {
				t.Fatalf("unexpected truncation: %.200s", got)
			}
		})
	}
}
