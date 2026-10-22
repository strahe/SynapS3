//go:build systemtest

package systemtest

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/testpg"
)

func TestHarnessStopsAllComponentsWhenS3ServerFails(t *testing.T) {
	t.Parallel()
	testpg.DSN(t)
	// Database setup has a separate budget from the runtime's five-second shutdown guard.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	invalidSocket := filepath.Join(t.TempDir(), "missing", "s3.sock")
	_, err := newHarness(ctx, logger, invalidSocket)
	if err == nil {
		t.Fatal("newHarness succeeded with an unbindable S3 socket")
	}
	if !strings.Contains(err.Error(), "S3 server") {
		t.Fatalf("newHarness error = %v, want S3 server context", err)
	}
	if strings.Contains(err.Error(), "runtime shutdown exceeded") ||
		strings.Contains(err.Error(), "systemtest runtime did not stop after startup failure") {
		t.Fatalf("runtime did not stop after S3 server failure: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("harness initialization and cleanup exceeded test deadline: %v", ctx.Err())
	}
}
