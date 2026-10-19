// Package storagepipeline owns the workflow-neutral storage task inputs.
package storagepipeline

import (
	"errors"
)

// UploadPlanReplacementWaitReason isolates uploads blocked by a reserved replacement slot.
const UploadPlanReplacementWaitReason = "replacement"

// UploadPlanInput names the content to ingest. Ingest is a property of the
// bytes, so two versions of identical content share one plan.
type UploadPlanInput struct {
	ContentID int64 `json:"content_id"`
}

type DataSetInput struct {
	ExecutionIdentity
	DataSetID int64 `json:"data_set_id"`
}

type DataSetGenerationInput struct {
	DataSetID  int64 `json:"data_set_id"`
	Generation int64 `json:"generation"`
}

type CopyGenerationInput struct {
	CopyID     int64 `json:"copy_id"`
	Generation int64 `json:"generation"`
}

// CommitRequestInput names the commit request a storage_commit task drives.
type CommitRequestInput struct {
	ExecutionIdentity
	RequestID string `json:"request_id"`
}

func ValidateCommitRequestInput(input CommitRequestInput) error {
	if input.RequestID == "" {
		return errors.New("request_id is required")
	}
	return nil
}

func ValidateUploadPlanInput(input UploadPlanInput) error {
	if input.ContentID < 1 {
		return errors.New("content_id must be positive")
	}
	return nil
}

func ValidateDataSetInput(input DataSetInput) error {
	if input.DataSetID < 1 {
		return errors.New("data_set_id must be positive")
	}
	return nil
}

func ValidateDataSetGenerationInput(input DataSetGenerationInput) error {
	if input.DataSetID < 1 || input.Generation < 1 {
		return errors.New("data_set_id and generation must be positive")
	}
	return nil
}

func ValidateCopyGenerationInput(input CopyGenerationInput) error {
	if input.CopyID < 1 || input.Generation < 1 {
		return errors.New("copy_id and generation must be positive")
	}
	return nil
}
