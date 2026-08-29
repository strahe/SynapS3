package synapse

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

func TestStorageServiceAdapterSelectUploadTargetsPreservesPartialSelection(t *testing.T) {
	t.Parallel()

	payer := common.HexToAddress("0x1001")
	recordKeeper := common.HexToAddress("0x2002")
	chainID := sdktypes.ChainID(314159)
	providerID := sdktypes.NewBigInt(101)
	providerContext, err := storage.NewProviderContext(
		storage.Provider{ID: providerID, ServiceURL: "https://provider.example"},
		&inertPDPProviderClient{}, nil,
		storage.WithPayer(payer), storage.WithChainID(chainID), storage.WithRecordKeeper(recordKeeper),
	)
	if err != nil {
		t.Fatalf("NewProviderContext: %v", err)
	}
	selector := &recordingContextSelector{selectUpload: func(opts storage.SelectUploadContextsOptions) (*storage.UploadContextSelection, error) {
		return &storage.UploadContextSelection{
			Contexts:        []storage.StorageContext{providerContext},
			RequestedCopies: opts.Copies,
			Complete:        false,
		}, &storage.InsufficientUploadContextsError{Requested: opts.Copies, Available: 1}
	}}
	service, err := storage.New(storage.Options{
		ContextSelector: selector,
		PayerAddress:    payer,
		ChainID:         chainID,
		RecordKeeper:    recordKeeper,
	})
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}

	targets, err := AdaptStorageService(service).SelectUploadTargets(t.Context(), storage.SelectUploadContextsOptions{Copies: 2})
	if !IsNoProviderCandidates(err) || len(targets) != 1 {
		t.Fatalf("targets = %#v, error = %T %v; want one usable target and NoProviderCandidatesError", targets, err, err)
	}
	if !selector.got.AllowUnendorsedPrimary {
		t.Fatal("AllowUnendorsedPrimary = false, want compatibility override")
	}
	if _, ok := targets[0].(ProviderTarget); !ok {
		t.Fatalf("selected target %T is not a ProviderTarget", targets[0])
	}
	if _, bound := targets[0].DataSetRef(); bound {
		t.Fatal("selected provider target unexpectedly has a data set")
	}
}

func TestStorageServiceAdapterFindMatchingDataSetUsesExactMetadataAndStablePreference(t *testing.T) {
	t.Parallel()

	payer := common.HexToAddress("0x1001")
	providerID := sdktypes.NewBigInt(202)
	finder := &staticDataSetFinder{dataSets: []*storage.DataSetDetails{
		dataSetDetails(20, 1020, 202, true, true, false, 0, map[string]string{"source": dataSetSource, "bucket": "photos"}),
		dataSetDetails(40, 1040, 202, true, true, true, 0, map[string]string{"source": dataSetSource, "bucket": "other"}),
		dataSetDetails(30, 1030, 202, true, true, true, 0, map[string]string{"source": dataSetSource, "bucket": "photos"}),
		dataSetDetails(25, 1025, 202, true, true, true, 0, map[string]string{"source": dataSetSource, "bucket": "photos"}),
		dataSetDetails(15, 1015, 202, true, true, true, 99, map[string]string{"source": dataSetSource, "bucket": "photos"}),
		dataSetDetails(10, 1010, 303, true, true, true, 0, map[string]string{"source": dataSetSource, "bucket": "photos"}),
	}}
	service, err := storage.New(storage.Options{DataSetFinder: finder, PayerAddress: payer})
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}

	ref, err := AdaptStorageService(service).FindMatchingDataSet(t.Context(), providerID, map[string]string{
		"source":  "caller-value-must-not-override-synaps3",
		"bucket":  "photos",
		"withCDN": "caller-value-must-not-enable-cdn",
	}, false)
	if err != nil {
		t.Fatalf("FindMatchingDataSet: %v", err)
	}
	if ref == nil || ref.ProviderID().String() != "202" || ref.DataSetID().String() != "25" || ref.ClientDataSetID().String() != "1025" {
		t.Fatalf("matching ref = %#v, want active lowest data set 25 with complete identity", ref)
	}
	if !finder.onlyManaged || finder.payer != payer {
		t.Fatalf("FindDataSets inputs = payer:%s onlyManaged:%t", finder.payer, finder.onlyManaged)
	}
}

func TestStorageServiceAdapterPrepareUploadReturnsCostsWithoutFunding(t *testing.T) {
	t.Parallel()

	payer := common.HexToAddress("0x1001")
	recordKeeper := common.HexToAddress("0x2002")
	chainID := sdktypes.ChainID(314159)
	providerContext, err := storage.NewProviderContext(
		storage.Provider{ID: sdktypes.NewBigInt(101), ServiceURL: "https://provider.example"},
		&inertPDPProviderClient{}, nil,
		storage.WithPayer(payer), storage.WithChainID(chainID), storage.WithRecordKeeper(recordKeeper),
	)
	if err != nil {
		t.Fatalf("NewProviderContext: %v", err)
	}
	want := &storage.MultiContextCosts{DepositNeeded: big.NewInt(123), NeedsFWSSMaxApproval: true, Ready: false}
	costs := &staticCostCalculator{costs: want}
	funder := &recordingPaymentsFunder{}
	service, err := storage.New(storage.Options{
		CostCalculator: costs,
		PaymentsFunder: funder,
		PayerAddress:   payer,
		ChainID:        chainID,
		RecordKeeper:   recordKeeper,
	})
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	adapter := AdaptStorageService(service)
	target := newProviderTargetAdapter(providerContext)

	got, err := adapter.PrepareUpload(t.Context(), 4096, []StorageTarget{target})
	if err != nil {
		t.Fatalf("PrepareUpload: %v", err)
	}
	if got != want || costs.dataSize.Cmp(big.NewInt(4096)) != 0 || len(costs.refs) != 1 {
		t.Fatalf("costs = %#v, calculator size = %v refs = %#v", got, costs.dataSize, costs.refs)
	}
	if funder.calls != 0 {
		t.Fatalf("funding calls = %d, want none", funder.calls)
	}
}

type inertPDPProviderClient struct{ storage.PDPProviderClient }

type recordingContextSelector struct {
	got          storage.SelectUploadContextsOptions
	selectUpload func(storage.SelectUploadContextsOptions) (*storage.UploadContextSelection, error)
}

func (*recordingContextSelector) SelectProviderContext(context.Context, storage.SelectProviderContextOptions) (*storage.ProviderContext, error) {
	return nil, errors.New("unexpected provider selection")
}

func (s *recordingContextSelector) SelectUploadContexts(_ context.Context, opts storage.SelectUploadContextsOptions) (*storage.UploadContextSelection, error) {
	s.got = opts
	return s.selectUpload(opts)
}

type staticDataSetFinder struct {
	dataSets    []*storage.DataSetDetails
	payer       common.Address
	onlyManaged bool
}

func (f *staticDataSetFinder) FindDataSets(_ context.Context, payer common.Address, onlyManaged bool) ([]*storage.DataSetDetails, error) {
	f.payer = payer
	f.onlyManaged = onlyManaged
	return f.dataSets, nil
}

func dataSetDetails(dataSetID, clientDataSetID, providerID uint64, live, managed, active bool, endEpoch sdktypes.Epoch, metadata map[string]string) *storage.DataSetDetails {
	return &storage.DataSetDetails{
		DataSetInfo: &warmstorage.DataSetInfo{
			DataSetID:       sdktypes.NewBigInt(dataSetID),
			ClientDataSetID: sdktypes.NewBigInt(clientDataSetID),
			ProviderID:      sdktypes.NewBigInt(providerID),
			PDPEndEpoch:     endEpoch,
		},
		IsLive:          live,
		IsManaged:       managed,
		HasActivePieces: active,
		Metadata:        metadata,
	}
}

type staticCostCalculator struct {
	costs    *storage.MultiContextCosts
	dataSize *big.Int
	refs     []storage.ContextCostRef
}

func (c *staticCostCalculator) CalculateMultiContextCosts(_ context.Context, _ common.Address, dataSize *big.Int, refs []storage.ContextCostRef, _ storage.MultiCostOptions) (*storage.MultiContextCosts, error) {
	c.dataSize = new(big.Int).Set(dataSize)
	c.refs = append([]storage.ContextCostRef(nil), refs...)
	return c.costs, nil
}

type recordingPaymentsFunder struct{ calls int }

func (f *recordingPaymentsFunder) FundSync(context.Context, *big.Int, ...payments.WriteOption) (*sdktypes.WriteResult, error) {
	f.calls++
	return nil, errors.New("unexpected funding")
}
