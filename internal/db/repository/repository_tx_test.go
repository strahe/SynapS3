package repository_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/strahe/synaps3/internal/db/repository"
)

type sqliteBusyError struct{}

func (*sqliteBusyError) Error() string { return "database is locked" }
func (*sqliteBusyError) Code() int     { return 5 }

func TestRepositoryTransactionContentionPreservesCause(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cause    error
		identity bool
	}{
		{"serialization", &pgconn.PgError{Code: "40001", Message: "serialization failure"}, false},
		{"deadlock", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}, false},
		{"sqlite_busy", &sqliteBusyError{}, false},
		{"task_identity", repository.ErrTaskIdentityContended, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repos := repository.NewRepositories(testDB(t))
			calls := 0
			err := repos.WithTx(t.Context(), func(*repository.Repositories) error {
				calls++
				return fmt.Errorf("writing a task: %w", tc.cause)
			})
			if calls != 20 || !errors.Is(err, repository.ErrRepositoryContended) || !errors.Is(err, tc.cause) || errors.Is(err, repository.ErrTaskIdentityContended) != tc.identity {
				t.Fatalf("contention classification calls=%d err=%v", calls, err)
			}
		})
	}
}
