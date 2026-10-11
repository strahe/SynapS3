// Package walletoperation owns wallet task inputs and recovery checkpoints.
package walletoperation

import (
	"errors"
)

type Input struct {
	OperationID int64 `json:"operation_id"`
}

type Checkpoint struct {
	BroadcastAttempted bool   `json:"broadcast_attempted"`
	TransactionHash    string `json:"transaction_hash,omitempty"`
}

func ValidateInput(input Input) error {
	if input.OperationID < 1 {
		return errors.New("operation_id must be positive")
	}
	return nil
}
