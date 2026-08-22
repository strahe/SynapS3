package repository

import "fmt"

// A readable committed copy is one this node recorded as committed with a
// complete retrieval identity, on a data set that still serves reads. It
// reflects local bookkeeping and is not a live data-safety guarantee.
func readableCommittedCopyPredicateSQL(copyAlias, dataSetAlias string) string {
	return readableCommittedCopyPredicateWithDataSetStatusSQL(
		copyAlias,
		dataSetAlias,
		fmt.Sprintf("%s.status IN (%s)", dataSetAlias, storageHealthReadyDataSetStatusListSQL()),
	)
}

// dataSetStatusCondition widens the data set status test for callers that must
// also treat a data set as its own readable source while it is being finalized.
func readableCommittedCopyPredicateWithDataSetStatusSQL(copyAlias, dataSetAlias, dataSetStatusCondition string) string {
	return fmt.Sprintf(`%[1]s.status = %[3]s
		  AND %[1]s.storage_data_set_id IS NOT NULL
		  AND %[1]s.provider_id IS NOT NULL AND %[1]s.provider_id <> ''
		  AND %[2]s.data_set_id IS NOT NULL AND %[2]s.data_set_id <> ''
		  AND %[4]s
		  AND %[1]s.piece_id IS NOT NULL AND %[1]s.piece_id <> ''
		  AND %[1]s.retrieval_url IS NOT NULL AND %[1]s.retrieval_url <> ''`,
		copyAlias,
		dataSetAlias,
		storageHealthCommittedCopyStatusSQL(),
		dataSetStatusCondition,
	)
}

// A replica slot can hold several data set generations during a provider
// replacement, so a lookup that knows only the upload and the slot must resolve
// to the generation that currently owns the slot. A copy with no data set yet
// belongs to the slot until one is assigned.
func currentGenerationCopySQL(copyAlias string) string {
	return fmt.Sprintf(`(
		%[1]s.storage_data_set_id IS NULL
		OR EXISTS (
			SELECT 1 FROM storage_data_sets AS current_slot_data_set
			WHERE current_slot_data_set.id = %[1]s.storage_data_set_id
			  AND current_slot_data_set.is_current
		)
	)`, copyAlias)
}

// Durability is measured in logical replica slots. One slot can hold several
// physical data set generations while a provider replacement is in flight, and
// those generations must never count as separate replicas.
func distinctReadableSlotCountSQL(copyAlias, dataSetAlias, uploadIDExpr string) string {
	return fmt.Sprintf(`(
		SELECT COUNT(DISTINCT %[2]s.copy_index)
		FROM storage_upload_copies AS %[1]s
		JOIN storage_data_sets AS %[2]s ON %[2]s.id = %[1]s.storage_data_set_id
		WHERE %[1]s.upload_id = %[3]s
		  AND %[4]s
	)`,
		copyAlias,
		dataSetAlias,
		uploadIDExpr,
		readableCommittedCopyPredicateSQL(copyAlias, dataSetAlias),
	)
}
