// Package errtext prepares error text for persisted error fields, which the
// Admin API shows to operators unchanged.
package errtext

import (
	"cmp"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"unicode/utf8"
)

// MaxBytes bounds persisted error text. A provider may reply with a body of
// several megabytes, and error fields are rewritten on every attempt.
const MaxBytes = 4096

const placeholder = "[redacted]"

var urlPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>]+`)

type redactor struct {
	// secrets are replaced longest first so a secret containing another is
	// never left partly visible.
	secrets []string
	// rpcHost URLs keep only their origin: RPC endpoints commonly carry an
	// API key in the path.
	rpcHost string
}

var active atomic.Pointer[redactor]

// Configure registers the chain RPC URL and other configured secret values.
// Call it once at startup, before any error text is persisted; until then only
// URL credentials, queries, and fragments are removed.
func Configure(rpcURL string, secrets ...string) {
	active.Store(newRedactor(rpcURL, secrets...))
}

// Redact returns text safe to persist in an error field: configured secrets
// become a placeholder, URLs lose credentials, query, and fragment, and the
// result fits MaxBytes.
func Redact(text string) string {
	r := active.Load()
	if r == nil {
		r = &redactor{}
	}
	return r.redact(text)
}

// RedactPtr applies Redact to an optional error field; nil stays nil.
func RedactPtr(text *string) *string {
	if text == nil {
		return nil
	}
	redacted := Redact(*text)
	return &redacted
}

func newRedactor(rpcURL string, secrets ...string) *redactor {
	r := &redactor{}
	if parsed, err := url.Parse(strings.TrimSpace(rpcURL)); err == nil {
		r.rpcHost = strings.ToLower(parsed.Host)
	}
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		r.secrets = append(r.secrets, secret)
		if hex, ok := strings.CutPrefix(strings.ToLower(secret), "0x"); ok && hex != "" {
			r.secrets = append(r.secrets, secret[2:])
		}
	}
	slices.SortFunc(r.secrets, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	return r
}

func (r *redactor) redact(text string) string {
	for _, secret := range r.secrets {
		text = strings.ReplaceAll(text, secret, placeholder)
	}
	text = urlPattern.ReplaceAllStringFunc(text, r.redactURL)
	return truncate(text)
}

func (r *redactor) redactURL(raw string) string {
	trimmed := strings.TrimRight(raw, ".,;:)]}")
	suffix := raw[len(trimmed):]
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return placeholder + suffix
	}
	parsed.User = nil
	parsed.RawQuery, parsed.ForceQuery = "", false
	parsed.Fragment, parsed.RawFragment = "", ""
	if r.rpcHost != "" && strings.EqualFold(parsed.Host, r.rpcHost) {
		parsed.Path, parsed.RawPath = "", ""
	}
	return parsed.String() + suffix
}

func truncate(text string) string {
	if len(text) <= MaxBytes {
		return text
	}
	cut := MaxBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…"
}
