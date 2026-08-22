package repository

import (
	"testing"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
)

// Every status these queries can encounter must be inlinable. A value missing
// from the allowlist panics when the query is built, which would take down the
// process at request time rather than failing a query.
func TestStorageHealthSQLLiteralAcceptsEveryDomainStatus(t *testing.T) {
	dataSetStatuses := []model.StorageDataSetStatus{
		model.StorageDataSetStatusPending,
		model.StorageDataSetStatusCreating,
		model.StorageDataSetStatusReady,
		model.StorageDataSetStatusFailed,
		model.StorageDataSetStatusUnavailable,
		model.StorageDataSetStatusDraining,
		model.StorageDataSetStatusRetired,
	}
	copyStatuses := []model.StorageUploadCopyStatus{
		model.StorageUploadCopyStatusPending,
		model.StorageUploadCopyStatusPieceReady,
		model.StorageUploadCopyStatusCommitting,
		model.StorageUploadCopyStatusCommitted,
		model.StorageUploadCopyStatusFailed,
	}
	observationStatuses := []observability.Status{
		observability.StatusAvailable,
		observability.StatusDegraded,
		observability.StatusUnavailable,
		observability.StatusUnknown,
	}

	values := make([]string, 0, len(dataSetStatuses)+len(copyStatuses)+len(observationStatuses))
	for _, status := range dataSetStatuses {
		values = append(values, string(status))
	}
	for _, status := range copyStatuses {
		values = append(values, string(status))
	}
	for _, status := range observationStatuses {
		values = append(values, string(status))
	}

	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			if got := storageHealthSQLLiteral(value); got != "'"+value+"'" {
				t.Fatalf("storageHealthSQLLiteral(%q) = %q, want %q", value, got, "'"+value+"'")
			}
		})
	}
}

func TestStorageHealthSQLLiteralRejectsUnknownValue(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("storageHealthSQLLiteral accepted an unknown value, want panic")
		}
	}()
	storageHealthSQLLiteral("'; DROP TABLE storage_data_sets; --")
}
