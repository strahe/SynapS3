// Package storagecleanup owns durable cleanup task inputs.
package storagecleanup

import (
	"errors"
	"fmt"
)

const TaskKeyPrefix = "storage-cleanup:"

type Input struct {
	ContentID  int64 `json:"content_id"`
	Generation int64 `json:"generation"`
}

func TaskKey(contentID, generation int64) string {
	return fmt.Sprintf("%s%d:%d", TaskKeyPrefix, contentID, generation)
}

func ValidateInput(input Input) error {
	if input.ContentID < 1 || input.Generation < 1 {
		return errors.New("content_id and generation must be positive")
	}
	return nil
}
