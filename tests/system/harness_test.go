//go:build systemtest

package system_test

import (
	"log/slog"
	"testing"

	"github.com/strahe/synaps3/internal/systemtest"
)

// newSystemHarness starts a harness on the database selected by build tags.
func newSystemHarness(t *testing.T, logger *slog.Logger, options systemtest.HarnessOptions) (*systemtest.Harness, error) {
	t.Helper()
	options.Database = systemDatabase(t)
	return systemtest.NewHarness(t.Context(), logger, options)
}
