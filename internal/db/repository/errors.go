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

// isUniqueViolation detects unique constraint violations for both PostgreSQL and SQLite.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	return strings.Contains(err.Error(), "UNIQUE constraint")
}
