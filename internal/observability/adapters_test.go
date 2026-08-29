package observability

import (
	"context"
	"testing"

	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

func TestStorageDataSetScannerMapsDataSetDetails(t *testing.T) {
	t.Parallel()

	finder := &dataSetDetailsFinder{dataSets: []*storage.DataSetDetails{
		nil,
		{
			DataSetInfo: &warmstorage.DataSetInfo{
				DataSetID:       sdktypes.NewBigInt(1001),
				ClientDataSetID: sdktypes.NewBigInt(9001),
				ProviderID:      sdktypes.NewBigInt(101),
			},
			IsLive:    true,
			IsManaged: true,
			Metadata:  map[string]string{"source": "synaps3", "bucket": "photos"},
		},
	}}

	got, err := NewStorageDataSetScanner(finder).ScanWalletDataSets(t.Context())
	if err != nil {
		t.Fatalf("ScanWalletDataSets: %v", err)
	}
	if finder.onlyManaged {
		t.Fatal("FindDataSets OnlyManaged = true, want the complete wallet inventory")
	}
	if len(got) != 1 || got[0].DataSetID.String() != "1001" || got[0].ProviderID.String() != "101" ||
		got[0].ClientDataSetID == nil || got[0].ClientDataSetID.String() != "9001" || !got[0].IsLive || !got[0].IsManaged {
		t.Fatalf("wallet data sets = %#v, want mapped DataSetDetails identity and state", got)
	}
	if got[0].ActivePieceCount != nil {
		t.Fatalf("active piece count = %v, want unknown when DataSetDetails only supplies presence", got[0].ActivePieceCount)
	}
}

type dataSetDetailsFinder struct {
	dataSets    []*storage.DataSetDetails
	onlyManaged bool
}

func (f *dataSetDetailsFinder) FindDataSets(_ context.Context, opts *storage.FindDataSetsOptions) ([]*storage.DataSetDetails, error) {
	f.onlyManaged = opts != nil && opts.OnlyManaged
	return f.dataSets, nil
}
