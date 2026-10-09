//go:build postgres

package worker

import (
	"testing"

	"github.com/strahe/synaps3/internal/testutil"
)

func TestPostgresBootstrapIsolatesInvalidLegacyPendingWithoutStoppingEngine(t *testing.T) {
	assertBootstrapIsolatesInvalidLegacyPending(t, testutil.NewTestPostgresDB)
}
