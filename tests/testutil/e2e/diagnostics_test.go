package e2e

import (
	"strings"
	"testing"
	"time"
)

func TestDiagnosticValueFormatsReadableRedactedJSON(t *testing.T) {
	value := DiagnosticValue(map[string]any{
		"secret_key": "secret",
		"items":      []string{"one", "two"},
	})

	if !strings.Contains(value, "\n") {
		t.Fatalf("DiagnosticValue should format multi-line JSON: %q", value)
	}
	if strings.Contains(value, `: "secret"`) || !strings.Contains(value, "[REDACTED]") {
		t.Fatalf("DiagnosticValue should redact secrets: %s", value)
	}
}

func TestWaitSnapshotKeepsLastValueStructured(t *testing.T) {
	value := DiagnosticValue(NewWaitSnapshot("upload", time.Now(), map[string]string{"status": "waiting"}, nil))

	if !strings.Contains(value, `"last": {`) {
		t.Fatalf("WaitSnapshot should keep last value structured: %s", value)
	}
	if strings.Contains(value, `\"status\"`) {
		t.Fatalf("WaitSnapshot should not double encode last value: %s", value)
	}
}
