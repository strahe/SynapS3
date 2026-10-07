package model

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/types"
)

// DataSetCreationRejection binds a provider refusal and an absence observation
// to the original request. Absence does not exclude a late successful request.
type DataSetCreationRejection struct {
	Version          int             `json:"version"`
	StatusCode       int             `json:"status_code"`
	RejectedAt       time.Time       `json:"rejected_at"`
	AbsenceCheckedAt time.Time       `json:"absence_checked_at"`
	ClientDataSetID  types.OnChainID `json:"client_data_set_id"`
	Payer            common.Address  `json:"payer"`
	ChainID          uint64          `json:"chain_id"`
	RecordKeeper     common.Address  `json:"record_keeper"`
}

func (r DataSetCreationRejection) Valid() bool {
	return r.Version == 1 && (r.StatusCode == 400 || r.StatusCode == 401 || r.StatusCode == 403) &&
		!r.RejectedAt.IsZero() && !r.AbsenceCheckedAt.IsZero() && !r.AbsenceCheckedAt.Before(r.RejectedAt) &&
		!r.ClientDataSetID.IsZero() && r.Payer != (common.Address{}) && r.ChainID != 0 && r.RecordKeeper != (common.Address{})
}

// CreationRejectionEvidence rejects unknown or incomplete persisted evidence.
func (d *StorageDataSet) CreationRejectionEvidence() (*DataSetCreationRejection, error) {
	if len(d.CreationRejection) == 0 {
		return nil, nil
	}
	var evidence DataSetCreationRejection
	if err := json.Unmarshal(d.CreationRejection, &evidence); err != nil {
		return nil, err
	}
	if !evidence.Valid() || d.ClientDataSetID == nil || !evidence.ClientDataSetID.Equal(*d.ClientDataSetID) {
		return nil, errors.New("invalid storage service creation rejection evidence")
	}
	return &evidence, nil
}
