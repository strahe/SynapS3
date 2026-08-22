package repository

import (
	"strings"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func storageHealthEmptyJSONArraySQL(db bun.IDB) string {
	if db.Dialect().Name() == dialect.PG {
		return "'[]'::jsonb"
	}
	return "'[]'"
}

func storageHealthReadyDataSetStatusListSQL() string {
	return storageHealthSQLLiteralList(
		string(model.StorageDataSetStatusReady),
		string(model.StorageDataSetStatusDraining),
	)
}

func storageHealthAbnormalObservationStatusListSQL() string {
	return storageHealthSQLLiteralList(
		string(observability.StatusDegraded),
		string(observability.StatusUnavailable),
		string(observability.StatusUnknown),
	)
}

func storageHealthCommittedCopyStatusSQL() string {
	return storageHealthSQLLiteral(string(model.StorageUploadCopyStatusCommitted))
}

func storageHealthAvailableObservationStatusSQL() string {
	return storageHealthSQLLiteral(string(observability.StatusAvailable))
}

func storageHealthUnavailableObservationStatusSQL() string {
	return storageHealthSQLLiteral(string(observability.StatusUnavailable))
}

func storageHealthDegradedObservationStatusSQL() string {
	return storageHealthSQLLiteral(string(observability.StatusDegraded))
}

func storageHealthUnknownObservationStatusSQL() string {
	return storageHealthSQLLiteral(string(observability.StatusUnknown))
}

func storageHealthSQLLiteralList(values ...string) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, storageHealthSQLLiteral(value))
	}
	return strings.Join(out, ", ")
}

// Enum values share spellings across domains, so the allowlist is a set rather
// than a switch.
var storageHealthSQLLiterals = func() map[string]struct{} {
	values := []string{
		string(model.StorageDataSetStatusPending),
		string(model.StorageDataSetStatusCreating),
		string(model.StorageDataSetStatusReady),
		string(model.StorageDataSetStatusFailed),
		string(model.StorageDataSetStatusUnavailable),
		string(model.StorageDataSetStatusDraining),
		string(model.StorageDataSetStatusRetired),
		string(model.StorageUploadCopyStatusPending),
		string(model.StorageUploadCopyStatusPieceReady),
		string(model.StorageUploadCopyStatusCommitting),
		string(model.StorageUploadCopyStatusCommitted),
		string(model.StorageUploadCopyStatusFailed),
		string(observability.StatusAvailable),
		string(observability.StatusDegraded),
		string(observability.StatusUnavailable),
		string(observability.StatusUnknown),
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}()

// Only enum values that already exist as domain constants may be inlined into
// raw SQL. The panic guards against interpolating caller-supplied text.
func storageHealthSQLLiteral(value string) string {
	if _, ok := storageHealthSQLLiterals[value]; !ok {
		panic("unsupported storage health SQL literal")
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
