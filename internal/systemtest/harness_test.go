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
)

func TestHarnessStopsAllComponentsWhenS3ServerFails(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
	if ctx.Err() != nil {
		t.Fatalf("runtime shutdown exceeded test deadline: %v", ctx.Err())
	}
}
