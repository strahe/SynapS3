//go:build systemtest

package system_test

import (
	"context"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

// newSystemHarness starts a harness on the database selected by build tags.
func newSystemHarness(t *testing.T, logger *slog.Logger, options systemtest.HarnessOptions) (*systemtest.Harness, error) {
	t.Helper()
	options.Database = systemDatabase(t)
	return systemtest.NewHarness(t.Context(), logger, options)
}

func waitForBucketReady(t *testing.T, admin *e2e.AdminClient, bucket string) {
	t.Helper()
	e2e.Eventually(t, t.Context(), 60*time.Second, "bucket ready for uploads", func(ctx context.Context) (string, bool, error) {
		var detail struct {
			Status string `json:"status"`
		}
		_, err := admin.GetJSON(ctx, "/api/v1/buckets/"+url.PathEscape(bucket), &detail)
		return detail.Status, detail.Status == "ready", err
	})
}
