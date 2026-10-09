//go:build postgres

package worker

import (
	"testing"

	"github.com/strahe/synaps3/internal/testutil"
)

func TestPostgresConcurrentManualRetriesReturnOneSuccessor(t *testing.T) {
	assertConcurrentManualRetries(t, testutil.NewTestPostgresDB(t))
}

func TestPostgresPeriodicRetryCompetition(t *testing.T) {
	assertPeriodicRetryCompetition(t, periodicHarnessWithDB(t, testutil.NewTestPostgresDB(t)))
}
