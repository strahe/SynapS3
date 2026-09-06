package storagereplacement

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
)

const (
	CoordinateTaskKeyPrefix = "provider-replacement:"
	RetireTaskKeyPrefix     = "storage-dataset-retire:"
)

type CoordinateInput struct {
	ReplacementID int64 `json:"replacement_id"`
	Generation    int64 `json:"generation"`
}

type RetireInput struct {
	ReplacementID int64 `json:"replacement_id"`
	DataSetID     int64 `json:"data_set_id"`
	Generation    int64 `json:"generation"`
}

func CoordinateTaskKey(replacementID, generation int64) string {
	return fmt.Sprintf("%s%d:%d", CoordinateTaskKeyPrefix, replacementID, generation)
}

func RetireTaskKey(dataSetID, generation int64) string {
	return fmt.Sprintf("%s%d:%d", RetireTaskKeyPrefix, dataSetID, generation)
}

func ParseCoordinateInput(task *model.Task) (CoordinateInput, error) {
	if task == nil {
		return CoordinateInput{}, errors.New("nil provider replacement task")
	}
	var input CoordinateInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return CoordinateInput{}, fmt.Errorf("decoding provider replacement input: %w", err)
	}
	if input.ReplacementID < 1 || input.Generation < 1 {
		return CoordinateInput{}, errors.New("provider replacement input is incomplete")
	}
	return input, nil
}

func ParseRetireInput(task *model.Task) (RetireInput, error) {
	if task == nil {
		return RetireInput{}, errors.New("nil data-set retirement task")
	}
	var input RetireInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return RetireInput{}, fmt.Errorf("decoding data-set retirement input: %w", err)
	}
	if input.ReplacementID < 1 || input.DataSetID < 1 || input.Generation < 1 {
		return RetireInput{}, errors.New("data-set retirement input is incomplete")
	}
	return input, nil
}

func ValidateCoordinateInput(input CoordinateInput) error {
	if input.ReplacementID < 1 || input.Generation < 1 {
		return errors.New("replacement_id and generation are required")
	}
	return nil
}

func ValidateRetireInput(input RetireInput) error {
	if input.ReplacementID < 1 || input.DataSetID < 1 || input.Generation < 1 {
		return errors.New("replacement_id, data_set_id, and generation are required")
	}
	return nil
}
