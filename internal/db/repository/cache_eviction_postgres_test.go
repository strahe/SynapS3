//go:build postgres

package repository_test

import "testing"

func TestPostgresLRUCandidateCursorUsesAccessTimeFallbackAndContentTieBreak(t *testing.T) {
	assertLRUCandidateCursorUsesAccessTimeFallbackAndContentTieBreak(t, migratedPostgresDB(t))
}
