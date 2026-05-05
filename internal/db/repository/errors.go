package repository

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrAlreadyExists is returned when an insert violates a unique constraint.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned when a CAS update matches zero rows (entity missing or wrong state).
var ErrNotFound = errors.New("not found")

// ErrInvalidInput is returned when repository input fails validation.
var ErrInvalidInput = errors.New("invalid input")

var errConcurrentObjectCreate = errors.New("concurrent object create")

// isUniqueViolation detects unique constraint violations for both PostgreSQL and SQLite.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	return strings.Contains(err.Error(), "UNIQUE constraint")
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}

func shouldRetryObjectWrite(err error, canRestartTx bool) bool {
	if isSQLiteBusy(err) {
		return true
	}
	return canRestartTx && errors.Is(err, errConcurrentObjectCreate)
}

func shouldRetryRepositoryTx(err error) bool {
	return errors.Is(err, errConcurrentObjectCreate)
}
