//go:build postgres

package task

import (
	"testing"

	"github.com/strahe/synaps3/internal/testutil"
)

func testPostgresCheckpointedEffect(t *testing.T) {
	t.Helper()
	t.Run("postgres", func(t *testing.T) {
		assertCheckpointedEffectCommitsCheckpointBeforeEffect(t, testutil.NewTestPostgresDB(t))
	})
}
