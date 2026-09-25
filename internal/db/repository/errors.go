package repository

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrAlreadyExists is returned when an insert violates a unique constraint.
var ErrAlreadyExists = errors.New("already exists")

var ErrS3AccountNameExists = errors.New("S3 account name already exists")

// ErrNotFound is returned when a CAS update matches zero rows (entity missing or wrong state).
var ErrNotFound = errors.New("not found")

// ErrInvalidInput is returned when repository input fails validation.
var ErrInvalidInput = errors.New("invalid input")

// ErrConflict is returned when a compare-and-restore operation sees stale state.
var ErrConflict = errors.New("conflict")

// ErrReplicaTargetLowered reports an attempt to reduce a bucket's replica
// target. It stays an ErrInvalidInput so existing handling still applies, but it
// is distinguishable because the reason a caller needs to hear is specific.
var ErrReplicaTargetLowered = fmt.Errorf("lowering the replica target is not supported: %w", ErrInvalidInput)

// ErrPermanentDeleteStorageBusy reports that a permanent delete cannot yet
// cancel storage work without risking a remote write. It remains compatible
// with ErrConflict for existing callers.
var ErrPermanentDeleteStorageBusy = fmt.Errorf("permanent delete blocked by storage work: %w", ErrConflict)

// ErrContentCleanupInProgress reports that a write names content whose cleanup
// has started or already deleted it. The bytes have to be written again once
// the cleanup finishes, which then creates new content.
var ErrContentCleanupInProgress = errors.New("storage content is being cleaned up")

// ErrContentCleanupNotReady reports that a finished cleanup cannot delete its
// content yet because other work still needs the rows.
var ErrContentCleanupNotReady = errors.New("storage content cleanup is waiting for other work")

// ErrTaskLeaseLost reports that a claim generation is stale or its lease can
// no longer be proven valid.
var ErrTaskLeaseLost = errors.New("task lease lost")

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

func isS3AccountNameUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" && pgErr.ConstraintName == "uq_s3_accounts_name"
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed: s3_accounts.name")
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
