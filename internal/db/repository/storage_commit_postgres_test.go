package repository_test

import (
	"context"
	"os"
	"testing"
)

func TestPostgresConcurrentStorageCommitReservationsRespectCapacity(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	assertConcurrentStorageCommitReservationsRespectCapacity(
		t,
		newPostgresReplacementDB(t, context.Background(), dsn),
	)
}
