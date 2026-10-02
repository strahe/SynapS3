package storagecommit

import "time"

// AttentionRecord describes one submitted request flagged for operator
// attention.
type AttentionRecord struct {
	RequestID     string
	TaskID        *int64
	DataSetRowID  int64
	ProviderID    string
	DataSetID     string
	PieceCIDs     []string
	TransactionID string
	SubmitError   string
	Code          AttentionCode
	SubmittedAt   time.Time
	AttentionAt   time.Time
}
