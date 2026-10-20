// Package errtext prepares error text for persisted error fields, which the
// Admin API shows to operators unchanged.
package errtext

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/ethereum/go-ethereum/ethclient"
	sdktypes "github.com/strahe/synapse-go/types"
)

// MaxBytes bounds persisted error text. A provider may reply with a body of
// several megabytes, and error fields are rewritten on every attempt.
const MaxBytes = 64 * 1024

const placeholder = "[redacted]"

var urlPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>]+`)

// Redactor removes configured secrets from diagnostic text.
type Redactor struct {
	// secrets are replaced longest first so a secret containing another is
	// never left partly visible.
	secrets []string
	// rpcHosts URLs keep only their origin: RPC endpoints commonly carry an
	// API key in the path.
	rpcHosts []string
}

var active atomic.Pointer[Redactor]

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
		r = &Redactor{}
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

// New creates a request-scoped redactor without changing startup configuration.
func New(rpcURL string, secrets ...string) *Redactor {
	r := newRedactor(rpcURL, secrets...)
	if configured := active.Load(); configured != nil {
		r.secrets = append(r.secrets, configured.secrets...)
		r.rpcHosts = append(r.rpcHosts, configured.rpcHosts...)
		slices.SortFunc(r.secrets, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	}
	return r
}

func newRedactor(rpcURL string, secrets ...string) *Redactor {
	r := &Redactor{}
	if parsed, err := url.Parse(strings.TrimSpace(rpcURL)); err == nil {
		if parsed.Host != "" {
			r.rpcHosts = []string{strings.ToLower(parsed.Host)}
		}
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

// Redact prepares diagnostic text for operator-facing output.
func (r *Redactor) Redact(text string) string {
	return r.redact(text)
}

func (r *Redactor) redact(text string) string {
	for _, secret := range r.secrets {
		encoded := strings.TrimPrefix(strings.TrimPrefix(secret, "0x"), "0X")
		if _, err := hex.DecodeString(encoded); err == nil && len(encoded) >= 32 {
			pattern := regexp.MustCompile("(?i)(?:0x)?" + regexp.QuoteMeta(encoded))
			text = pattern.ReplaceAllString(text, placeholder)
		} else {
			text = strings.ReplaceAll(text, secret, placeholder)
		}
	}
	text = safeText(text)
	text = urlPattern.ReplaceAllStringFunc(text, r.redactURL)
	return truncate(text)
}

// safeText preserves invalid database text as visible escapes.
func safeText(text string) string {
	if !strings.ContainsRune(text, 0) && utf8.ValidString(text) {
		return text
	}
	var result strings.Builder
	for len(text) > 0 {
		r, size := utf8.DecodeRuneInString(text)
		switch {
		case r == 0:
			result.WriteString(`\u0000`)
		case r == utf8.RuneError && size == 1:
			quoted := strconv.Quote(text[:1])
			result.WriteString(quoted[1 : len(quoted)-1])
		default:
			result.WriteRune(r)
		}
		text = text[size:]
	}
	return result.String()
}

func (r *Redactor) redactURL(raw string) string {
	trimmed := strings.TrimRight(raw, ".,;:)]}")
	suffix := raw[len(trimmed):]
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return placeholder + suffix
	}
	parsed.User = nil
	parsed.RawQuery, parsed.ForceQuery = "", false
	parsed.Fragment, parsed.RawFragment = "", ""
	if slices.Contains(r.rpcHosts, strings.ToLower(parsed.Host)) {
		parsed.Path, parsed.RawPath = "", ""
	}
	return parsed.String() + suffix
}

func truncate(text string) string {
	if len(text) <= MaxBytes {
		return text
	}
	marker := fmt.Sprintf("\n[truncated: redacted text was %d bytes]\n", len(text))
	available := MaxBytes - len(marker)
	head, tail := (available+1)/2, len(text)-available/2
	for head > 0 && !utf8.RuneStart(text[head]) {
		head--
	}
	for tail < len(text) && !utf8.RuneStart(text[tail]) {
		tail++
	}
	return text[:head] + marker + text[tail:]
}

// Format keeps error context and supported RPC diagnostics, then redacts and bounds it.
func Format(err error) string {
	r := active.Load()
	if r == nil {
		r = &Redactor{}
	}
	return r.Format(err)
}

// Format uses request-scoped secrets for diagnostic error output.
func (r *Redactor) Format(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	redactor := *r
	redactor.secrets = slices.Clone(r.secrets)
	var collect func(error)
	diagnostics := make(map[string]bool)
	collect = func(current error) {
		if current == nil {
			return
		}
		if sensitive, ok := current.(interface{ SensitiveErrorValues() []string }); ok {
			redactor.secrets = append(redactor.secrets, sensitive.SensitiveErrorValues()...)
		}
		if broadcast, ok := current.(*sdktypes.BroadcastError); ok && broadcast.Transaction != nil {
			if raw, marshalErr := broadcast.Transaction.MarshalBinary(); marshalErr == nil {
				redactor.secrets = append(redactor.secrets, hex.EncodeToString(raw))
			}
		}
		if _, rendered := current.(*redactedError); rendered {
			return
		}
		if coded, ok := current.(interface{ ErrorCode() int }); ok {
			detail := fmt.Sprintf("\nRPC code: %d", coded.ErrorCode())
			if data, ok := ethclient.RevertErrorData(current); ok {
				detail += "\nRevert data: 0x" + hex.EncodeToString(data)
			}
			if !diagnostics[detail] {
				text += detail
				diagnostics[detail] = true
			}
		}
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			for _, cause := range wrapped.Unwrap() {
				collect(cause)
			}
		case interface{ Unwrap() error }:
			collect(wrapped.Unwrap())
		}
	}
	collect(err)
	slices.SortFunc(redactor.secrets, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	return redactor.redact(text)
}

type redactedError struct {
	cause   error
	message string
}

func (e *redactedError) Error() string { return e.message }
func (e *redactedError) Unwrap() error { return e.cause }

// RedactError preserves error identity while making its message safe for output.
func RedactError(err error) error {
	if err == nil {
		return nil
	}
	return &redactedError{cause: err, message: Format(err)}
}

// RedactError preserves error identity with a request-scoped safe message.
func (r *Redactor) RedactError(err error) error {
	if err == nil {
		return nil
	}
	return &redactedError{cause: err, message: r.Format(err)}
}
