package storagecommit

import "time"

type AttentionRecord struct {
	CopyID        int64
	ContentID     int64
	CopyIndex     int
	DataSetRowID  int64
	ProviderID    string
	DataSetID     string
	PieceCID      string
	AttemptID     string
	TransactionID string
	Code          AttentionCode
	AttemptedAt   time.Time
	AttentionAt   time.Time
}

type ManualReleaseInput struct {
	CopyID                       int64
	ExpectedAttemptID            string
	AcknowledgePossibleDuplicate bool
	Now                          time.Time
}
