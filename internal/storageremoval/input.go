package storageremoval

import (
	"errors"

	"github.com/strahe/synaps3/internal/storagepipeline"
)

type Input struct {
	storagepipeline.ExecutionIdentity
	RequestID string `json:"request_id"`
}

func ValidateInput(input Input) error {
	if input.RequestID == "" {
		return errors.New("request_id is required")
	}
	if !input.Complete() {
		return storagepipeline.ErrExecutionIdentityMissing
	}
	return nil
}
