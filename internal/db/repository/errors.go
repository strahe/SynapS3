package repository

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrAlreadyExists is returned when an insert violates a unique constraint.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned when a CAS update matches zero rows (entity missing or wrong state).
var ErrNotFound = errors.New("not found")

// ErrInvalidInput is returned when repository input fails validation.
var ErrInvalidInput = errors.New("invalid input")

// ErrConflict is returned when a compare-and-restore operation sees stale state.
var ErrConflict = errors.New("conflict")

// ErrPermanentDeleteStorageBusy reports that a permanent delete cannot yet
// cancel storage work without risking a remote write. It remains compatible
// with ErrConflict for existing callers.
var ErrPermanentDeleteStorageBusy = fmt.Errorf("permanent delete blocked by storage work: %w", ErrConflict)

// ErrReplicaRepairItemCancelled reports that the exact repair copy no longer
// has work the claimed coordinator may execute.
var ErrReplicaRepairItemCancelled = errors.New("replica repair item cancelled")

// ErrUploadTaskCancelled reports that an ordinary upload task no longer has
// live storage work it may execute.
var ErrUploadTaskCancelled = errors.New("upload task cancelled")

// ErrTaskClaimLost reports that a worker no longer owns the running task claim.
var ErrTaskClaimLost = errors.New("task claim lost")

// ErrItemClaimLost reports that a provider replacement worker no longer owns
// the item lease identified by its fencing token.
var ErrItemClaimLost = errors.New("replacement item claim lost")

// ErrReplacementRetryUnsupported means the task belongs to an operator-approved
// provider replacement, which resumes only through its own retry action so the
// replacement record and the task never disagree. It wraps ErrConflict.
var ErrReplacementRetryUnsupported = fmt.Errorf("provider replacement work cannot be retried from the task queue: %w", ErrConflict)

// ErrAlreadyCurrent is returned when a restore would not change the current object representation.
var ErrAlreadyCurrent = errors.New("already current")

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
