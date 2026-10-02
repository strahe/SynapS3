package storagecommit

import "fmt"

// AttentionCode identifies a submitted request that needs operator review.
type AttentionCode string

const (
	// AttentionSubmissionMismatch means the request's nonce was consumed by
	// pieces other than the request's own. Nothing more is sent.
	AttentionSubmissionMismatch AttentionCode = "submission_mismatch"
	// AttentionDataSetUnavailable means the request's data set could not be
	// reached while a send of it may still land. Recovery keeps checking.
	AttentionDataSetUnavailable AttentionCode = "data_set_unavailable"
	// AttentionConfirmationTimeout means the request is still unregistered past
	// the attention threshold. Recovery keeps sending it.
	AttentionConfirmationTimeout AttentionCode = "confirmation_timeout"
)

func (c AttentionCode) Valid() bool {
	//exhaustive:enforce
	switch c {
	case AttentionSubmissionMismatch,
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
