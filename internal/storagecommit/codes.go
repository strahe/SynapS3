package storagecommit

import "fmt"

// AttentionCode identifies a durable commit that needs operator review.
type AttentionCode string

const (
	AttentionAttemptOnlyAmbiguous AttentionCode = "attempt_only_ambiguous"
	AttentionUnattributedPiece    AttentionCode = "unattributed_piece"
	AttentionInvalidSubmission    AttentionCode = "invalid_submission"
	AttentionSubmissionMismatch   AttentionCode = "submission_mismatch"
	AttentionDataSetUnavailable   AttentionCode = "data_set_unavailable"
	AttentionConfirmationTimeout  AttentionCode = "confirmation_timeout"
)

func (c AttentionCode) Valid() bool {
	switch c {
	case AttentionAttemptOnlyAmbiguous,
		AttentionUnattributedPiece,
		AttentionInvalidSubmission,
		AttentionSubmissionMismatch,
		AttentionDataSetUnavailable,
		AttentionConfirmationTimeout:
		return true
	default:
		return false
	}
}

func ParseAttentionCode(value string) (AttentionCode, error) {
	code := AttentionCode(value)
	if !code.Valid() {
		return "", fmt.Errorf("unknown storage commit attention code %q", value)
	}
	return code, nil
}
