//go:build postgres

package worker

import (
	"testing"

	"github.com/strahe/synaps3/internal/testutil"
)

func testPostgresMessageSettlement(t *testing.T) {
	t.Helper()
	t.Run("postgres", func(t *testing.T) {
		assertMessageSettlementRollback(t, testutil.NewTestPostgresDB(t), "error")
	})
}

func testPostgresSchedulerWake(t *testing.T) {
	t.Helper()
	t.Run("postgres", func(t *testing.T) {
		assertSchedulerWakeBoundaries(t, testutil.NewTestPostgresDB(t))
	})
}
