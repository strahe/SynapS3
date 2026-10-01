package synapse

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/strahe/synapse-go/pdp"
)

// errorSummaryLimit bounds the failure text kept on tasks and storage records;
// a provider may reply with a large error body.
const errorSummaryLimit = 500

var requestURLPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s"'<>]+`)

// ErrorSummary describes a failed provider or chain request for operators. A
// provider's error reply is kept because it is the reason the request failed.
// URLs are reduced to their origin: a chain RPC endpoint can carry an API key
// in its path or query.
func ErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok {
		text = fmt.Sprintf("provider returned HTTP %d", httpErr.StatusCode)
		if httpErr.Body != "" {
			text += ": " + httpErr.Body
		}
	}
	text = requestURLPattern.ReplaceAllStringFunc(text, urlOrigin)
	text = strings.Join(strings.Fields(text), " ")
	if utf8.RuneCountInString(text) > errorSummaryLimit {
		text = string([]rune(text)[:errorSummaryLimit]) + "…"
	}
	return text
}

// SummarizedError returns ErrorSummary as an error, or nil for a nil error.
func SummarizedError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(ErrorSummary(err))
}

func urlOrigin(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "[url]"
	}
	return parsed.Scheme + "://" + parsed.Host
}
