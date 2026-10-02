package storagecommit

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
)

// CommitRequestHistory summarizes every attempt carrying one signed request.
type CommitRequestHistory struct {
	ExtraDataHex         string
	MayHaveBeenSubmitted bool
	Unknown              bool
}

type CommitRequest struct {
	ExtraDataHex         string
	MayHaveBeenSubmitted bool
	HasHistory           bool
}

var ErrCommitRequestConflict = errors.New("previous storage registration requests require verification")

// PrepareCommitRequest reuses the copy's sole request, including one an older
// release cleared from the copy. Only a copy with no request history signs anew.
func PrepareCommitRequest(ctx context.Context, store Store, target synapse.DataSetTarget, copyRow model.StorageCopy, pieces []storage.PieceInput) (CommitRequest, error) {
	request, err := selectCommitRequest(ctx, store, copyRow)
	if err != nil || request.ExtraDataHex != "" {
		return request, err
	}
	extraData, err := target.PresignForCommit(ctx, pieces)
	if err != nil {
		return CommitRequest{}, err
	}
	request.ExtraDataHex = hex.EncodeToString(extraData)
	if err := validateCommitRequest(request.ExtraDataHex); err != nil {
		return CommitRequest{}, fmt.Errorf("validating signed storage commit request: %w", err)
	}
	return request, nil
}

func selectCommitRequest(ctx context.Context, store Store, copyRow model.StorageCopy) (CommitRequest, error) {
	history, err := store.ListCommitRequests(ctx, CopyIdentity{
		StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID,
		CopyIndex: copyRow.CopyIndex, StorageDataSetID: copyRow.StorageDataSetID,
	})
	if err != nil {
		return CommitRequest{}, err
	}
	request := CommitRequest{ExtraDataHex: taskDeref(copyRow.CommitExtraDataHex), HasHistory: len(history) > 0}
	values := make(map[string]struct{})
	if request.ExtraDataHex != "" {
		values[strings.ToLower(request.ExtraDataHex)] = struct{}{}
	}
	unknown := false
	for _, earlier := range history {
		values[strings.ToLower(earlier.ExtraDataHex)] = struct{}{}
		if request.ExtraDataHex == "" {
			request.ExtraDataHex = earlier.ExtraDataHex
		}
		request.MayHaveBeenSubmitted = request.MayHaveBeenSubmitted || earlier.MayHaveBeenSubmitted
		unknown = unknown || earlier.Unknown || earlier.ExtraDataHex == ""
	}
	if len(values) > 1 || unknown {
		return request, ErrCommitRequestConflict
	}
	if request.ExtraDataHex != "" {
		if err := validateCommitRequest(request.ExtraDataHex); err != nil {
			return request, fmt.Errorf("%w: %w", ErrCommitRequestConflict, err)
		}
	}
	return request, nil
}

func validateCommitRequest(extraHex string) error {
	extraData, err := hex.DecodeString(extraHex)
	if err == nil {
		_, err = ExtraDataNonce(extraData)
	}
	return err
}
