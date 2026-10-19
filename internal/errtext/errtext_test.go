package errtext

import (
	"strings"
	"testing"
	"unicode/utf8"
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
	got := (&redactor{}).redact(strings.Repeat("a", MaxBytes-1) + "é" + strings.Repeat("b", 100))
	if !utf8.ValidString(got) || len(got) > MaxBytes+len("…") || !strings.HasSuffix(got, "…") {
		t.Fatalf("redact() returned %d bytes, valid UTF-8 %v", len(got), utf8.ValidString(got))
	}
}
