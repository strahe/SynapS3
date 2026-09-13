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
			DataSetInfo: warmstorage.DataSetInfo{
				DataSetID:       sdktypes.NewBigInt(1001),
				ClientDataSetID: sdktypes.NewBigInt(9001),
				ProviderID:      sdktypes.NewBigInt(101),
			},
			IsLive:          true,
			IsManaged:       true,
			HasActivePieces: true,
			Metadata:        map[string]string{"source": "synaps3", "bucket": "photos"},
		},
		{
			DataSetInfo: warmstorage.DataSetInfo{
				DataSetID:       sdktypes.NewBigInt(1002),
				ClientDataSetID: sdktypes.NewBigInt(9002),
				ProviderID:      sdktypes.NewBigInt(102),
			},
			IsLive:          true,
			IsManaged:       true,
			HasActivePieces: false,
			Metadata:        map[string]string{"source": "synaps3", "bucket": "empty"},
		},
		{
			DataSetInfo: warmstorage.DataSetInfo{
				DataSetID:       sdktypes.NewBigInt(1003),
				ClientDataSetID: sdktypes.NewBigInt(9003),
				ProviderID:      sdktypes.NewBigInt(103),
			},
			IsLive:          false,
			IsManaged:       true,
			HasActivePieces: false,
			Metadata:        map[string]string{"source": "synaps3", "bucket": "ended"},
		},
	}}

	got, err := NewStorageDataSetScanner(finder).ScanWalletDataSets(t.Context())
	if err != nil {
		t.Fatalf("ScanWalletDataSets: %v", err)
	}
	if finder.onlyManaged {
		t.Fatal("FindDataSets OnlyManaged = true, want the complete wallet inventory")
	}
	if len(got) != 3 || got[0].DataSetID.String() != "1001" || got[0].ProviderID.String() != "101" ||
		got[0].ClientDataSetID == nil || got[0].ClientDataSetID.String() != "9001" || !got[0].IsLive || !got[0].IsManaged {
		t.Fatalf("wallet data sets = %#v, want mapped DataSetDetails identity and state", got)
	}
	if got[0].HasActivePieces == nil || !*got[0].HasActivePieces || got[0].ActivePieceCount != nil {
		t.Fatalf("active data set facts = count:%v presence:%v, want true presence with unknown exact count", got[0].ActivePieceCount, got[0].HasActivePieces)
	}
	if got[1].HasActivePieces == nil || *got[1].HasActivePieces || got[1].ActivePieceCount == nil || *got[1].ActivePieceCount != 0 {
		t.Fatalf("empty data set facts = count:%v presence:%v, want false presence with exact zero count", got[1].ActivePieceCount, got[1].HasActivePieces)
	}
	if got[2].HasActivePieces == nil || *got[2].HasActivePieces || got[2].ActivePieceCount != nil {
		t.Fatalf("ended data set facts = count:%v presence:%v, want false presence with unknown exact count", got[2].ActivePieceCount, got[2].HasActivePieces)
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
