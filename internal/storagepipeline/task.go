// Package storagepipeline owns the workflow-neutral storage task inputs.
package storagepipeline

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	UploadPlanKeyPrefix       = "upload-plan:"
	DataSetEnsureKeyPrefix    = "storage-dataset-ensure:"
	TransferPlanKeyPrefix     = "storage-transfer-plan:"
	StoreKeyPrefix            = "storage-store:"
	PullKeyPrefix             = "storage-pull:"
	CommitCoordinateKeyPrefix = "storage-commit-coordinate:"
	CommitKeyPrefix           = "storage-commit:"
)

// UploadPlanInput names the content to ingest. Ingest is a property of the
// bytes, so two versions of identical content share one plan.
type UploadPlanInput struct {
	ContentID int64 `json:"content_id"`
}

type DataSetInput struct {
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

func UploadPlanKey(contentID int64) string {
	return UploadPlanKeyPrefix + strconv.FormatInt(contentID, 10)
}

func DataSetEnsureKey(dataSetID int64) string {
	return DataSetEnsureKeyPrefix + strconv.FormatInt(dataSetID, 10)
}

func TransferPlanKey(copyID, generation int64) string {
	return copyGenerationKey(TransferPlanKeyPrefix, copyID, generation)
}

func StoreKey(copyID, generation int64) string {
	return copyGenerationKey(StoreKeyPrefix, copyID, generation)
}

func PullKey(copyID, generation int64) string {
	return copyGenerationKey(PullKeyPrefix, copyID, generation)
}

func CommitCoordinateKey(copyID, generation int64) string {
	return copyGenerationKey(CommitCoordinateKeyPrefix, copyID, generation)
}

func CommitKey(copyID, generation int64) string {
	return copyGenerationKey(CommitKeyPrefix, copyID, generation)
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

func copyGenerationKey(prefix string, id, generation int64) string {
	return fmt.Sprintf("%s%d:%d", prefix, id, generation)
}
