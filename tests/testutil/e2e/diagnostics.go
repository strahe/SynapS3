package e2e

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

const maxDiagnosticBytes = 8192

var redactionPatterns = []*regexp.Regexp{
	regexp.MustCompile(`0x[0-9a-fA-F]{64}`),
	regexp.MustCompile(`(?i)("?(?:secret_key|csrf_token|password|private_key)"?\s*[:=]\s*")([^"]*)(")`),
	regexp.MustCompile(`(?i)((?:secret_key|csrf_token|password|private_key)\s*=\s*)\S+`),
	regexp.MustCompile(`(?i)(SYNAPS3_[A-Z0-9_]*PRIVATE_KEY=)\S+`),
}

func Redact(value string) string {
	out := value
	for _, pattern := range redactionPatterns {
		switch pattern.NumSubexp() {
		case 0:
			out = pattern.ReplaceAllString(out, "[REDACTED]")
		case 3:
			out = pattern.ReplaceAllString(out, `${1}[REDACTED]${3}`)
		case 1:
			out = pattern.ReplaceAllString(out, `${1}[REDACTED]`)
		}
	}
	if len(out) > maxDiagnosticBytes {
		out = out[:maxDiagnosticBytes] + "...[truncated]"
	}
	return strings.TrimSpace(out)
}

func DiagnosticValue(value any) string {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return Redact(fmt.Sprintf("%#v", value))
	}
	return Redact(string(raw))
}

type BoundedLog struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func NewBoundedLog(limit int) *BoundedLog {
	if limit <= 0 {
		limit = 64 * 1024
	}
	return &BoundedLog{limit: limit}
}

func (b *BoundedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	if len(b.data) > b.limit {
		b.data = append([]byte(nil), b.data[len(b.data)-b.limit:]...)
	}
	return len(p), nil
}

func (b *BoundedLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Redact(string(b.data))
}

type WaitSnapshot struct {
	Description string `json:"description"`
	Last        any    `json:"last,omitempty"`
	LastError   string `json:"last_error,omitempty"`
	Elapsed     string `json:"elapsed"`
}

func NewWaitSnapshot(description string, started time.Time, last any, lastErr error) WaitSnapshot {
	snapshot := WaitSnapshot{
		Description: description,
		Last:        last,
		Elapsed:     time.Since(started).Round(time.Millisecond).String(),
	}
	if lastErr != nil {
		snapshot.LastError = Redact(lastErr.Error())
	}
	return snapshot
}
