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

func storageHealthSQLLiteral(value string) string {
	switch value {
	case string(model.StorageDataSetStatusReady),
		string(model.StorageDataSetStatusDraining),
		string(model.StorageUploadCopyStatusCommitted),
		string(observability.StatusAvailable),
		string(observability.StatusDegraded),
		string(observability.StatusUnavailable),
		string(observability.StatusUnknown):
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	default:
		panic("unsupported storage health SQL literal")
	}
}
