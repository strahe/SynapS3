package synapse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/observability"
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

func TestStorageServiceAdapterDownloadCountsConsumedPayload(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	wrongInfo, err := piece.CalculateFromBytes(bytes.Repeat([]byte{0xcd}, len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		corrupt     bool
		truncated   bool
		chunked     bool
		maximum     int64
		readBytes   int64
		wantBytes   uint64
		wantReadErr bool
		wantOpenErr bool
	}{
		{name: "complete", wantBytes: 512},
		{name: "CID mismatch", corrupt: true, wantBytes: 512, wantReadErr: true},
		{name: "truncated", truncated: true, wantBytes: 512, wantReadErr: true},
		{name: "streamed limit", chunked: true, maximum: 256, wantBytes: 256, wantReadErr: true},
		{name: "declared limit", maximum: 256, wantOpenErr: true},
		{name: "early close", readBytes: 64, wantBytes: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.chunked {
					w.(http.Flusher).Flush()
				} else {
					length := len(payload)
					if tc.truncated {
						length++
					}
					w.Header().Set("Content-Length", strconv.Itoa(length))
				}
				_, _ = w.Write(payload)
			}))
			defer server.Close()
			service, err := storage.New(storage.Options{HTTPClient: server.Client(), DownloadMaxBytes: tc.maximum})
			if err != nil {
				t.Fatal(err)
			}
			adapter := AdaptStorageService(service, nil, storage.ContextIdentity{})
			counters := observability.NewTransferCounters()
			adapter.SetTransferCounters(counters)
			expected := info.CIDv2
			if tc.corrupt {
				expected = wrongInfo.CIDv2
			}
			body, err := adapter.Download(t.Context(), expected, &storage.DownloadOptions{URL: server.URL})
			if (err != nil) != tc.wantOpenErr {
				t.Fatalf("Download error = %v, want error %t", err, tc.wantOpenErr)
			}
			if body != nil {
				if got := counters.Totals().Storage.DownloadBytes; got != 0 {
					t.Fatalf("unread download bytes = %d, want 0", got)
				}
				if tc.readBytes > 0 {
					_, err = io.CopyN(io.Discard, body, tc.readBytes)
				} else {
					_, err = io.Copy(io.Discard, body)
				}
				_ = body.Close()
				if (err != nil) != tc.wantReadErr {
					t.Errorf("download read error = %v, want error %t", err, tc.wantReadErr)
				}
			}
			if got := counters.Totals().Storage.DownloadBytes; got != tc.wantBytes {
				t.Errorf("storage download bytes = %d, want %d", got, tc.wantBytes)
			}
		})
	}
}

func TestStorageServiceAdapterSharesTransferCountersWithDataSetTargets(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 512)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	payer := common.HexToAddress("0x1001")
	recordKeeper := common.HexToAddress("0x2002")
	chainID := sdktypes.ChainID(314159)
	ref, err := storage.NewDataSetRef(sdktypes.NewBigInt(101), sdktypes.NewBigInt(202), sdktypes.NewBigInt(303))
	if err != nil {
		t.Fatal(err)
	}
	dataSet, err := storage.NewDataSetContext(storage.Provider{ID: ref.ProviderID(), ServiceURL: server.URL}, &inertPDPProviderClient{}, nil, ref,
		storage.WithPayer(payer), storage.WithChainID(chainID), storage.WithRecordKeeper(recordKeeper))
	if err != nil {
		t.Fatal(err)
	}
	selector := &recordingContextSelector{selectUpload: func(opts storage.SelectUploadContextsOptions) (*storage.UploadContextSelection, error) {
		return &storage.UploadContextSelection{Contexts: []storage.StorageContext{dataSet}, RequestedCopies: 1, Complete: true}, nil
	}}
	service, err := storage.New(storage.Options{
		ContextSelector: selector,
		ContextResolver: &staticDataSetContextResolver{dataSet: dataSet},
		PayerAddress:    payer,
		ChainID:         chainID,
		RecordKeeper:    recordKeeper,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter := AdaptStorageService(service, nil, storage.ContextIdentity{})
	adapter.providerHTTP = server.Client()
	counters := observability.NewTransferCounters()
	adapter.SetTransferCounters(counters)
	selected, err := adapter.SelectUploadTargets(t.Context(), storage.SelectUploadContextsOptions{Copies: 1})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := adapter.OpenDataSetTarget(t.Context(), ref.DataSetID(), storage.NewDataSetContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []DataSetTarget{selected[0].(DataSetTarget), opened} {
		if _, err := target.UploadStoreSession(t.Context(), server.URL, "12345678-1234-4234-8234-123456789abc", bytes.NewReader(payload), &storage.StoreOptions{PieceCID: info.CIDv2}); err != nil {
			t.Fatal(err)
		}
	}
	if got := counters.Totals().Storage.UploadBytes; got != uint64(2*len(payload)) {
		t.Errorf("storage upload bytes = %d, want %d", got, 2*len(payload))
	}
}

type staticDataSetContextResolver struct {
	storage.ContextResolver
	dataSet *storage.DataSetContext
}

func (r *staticDataSetContextResolver) ResolveDataSetContext(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (*storage.DataSetContext, error) {
	return r.dataSet, nil
}

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

	targets, err := AdaptStorageService(service, nil, storage.ContextIdentity{}).SelectUploadTargets(t.Context(), storage.SelectUploadContextsOptions{Copies: 2})
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

	ref, err := AdaptStorageService(service, nil, storage.ContextIdentity{}).FindMatchingDataSet(t.Context(), providerID, map[string]string{
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

func TestStorageServiceAdapterFindMatchingDataSetRequiresEmptyMetadataKey(t *testing.T) {
	t.Parallel()

	payer := common.HexToAddress("0x1001")
	providerID := sdktypes.NewBigInt(202)
	finder := &staticDataSetFinder{dataSets: []*storage.DataSetDetails{
		dataSetDetails(10, 1010, 202, true, true, true, 0, map[string]string{
			"source": dataSetSource,
			"bucket": "photos",
			"extra":  "",
		}),
		dataSetDetails(20, 1020, 202, true, true, true, 0, map[string]string{
			"source":  dataSetSource,
			"bucket":  "photos",
			"withCDN": "",
		}),
	}}
	service, err := storage.New(storage.Options{DataSetFinder: finder, PayerAddress: payer})
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}

	ref, err := AdaptStorageService(service, nil, storage.ContextIdentity{}).FindMatchingDataSet(
		t.Context(),
		providerID,
		map[string]string{"bucket": "photos"},
		true,
	)
	if err != nil {
		t.Fatalf("FindMatchingDataSet: %v", err)
	}
	if ref == nil || ref.DataSetID().String() != "20" {
		t.Fatalf("matching ref = %#v, want data set 20 with an explicit withCDN key", ref)
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
	want := &sdkcosts.MultiContextCosts{DepositNeeded: big.NewInt(123), NeedsFWSSMaxApproval: true, Ready: false}
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
	adapter := AdaptStorageService(service, nil, storage.ContextIdentity{})
	target := newProviderTargetAdapter(providerContext)

	got, err := adapter.PrepareUpload(t.Context(), 4096, []StorageTarget{target})
	if err != nil {
		t.Fatalf("PrepareUpload: %v", err)
	}
	if got != want || len(costs.pieceSizes) != 1 || costs.pieceSizes[0] != 4096 || len(costs.refs) != 1 {
		t.Fatalf("costs = %#v, calculator piece sizes = %v refs = %#v", got, costs.pieceSizes, costs.refs)
	}
	if !costs.refs[0].IsNewDataSet {
		t.Fatalf("calculator ref = %#v, want a new data set", costs.refs[0])
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
		DataSetInfo: warmstorage.DataSetInfo{
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
	costs      *sdkcosts.MultiContextCosts
	pieceSizes []uint64
	refs       []sdkcosts.MultiContextRef
}

func (c *staticCostCalculator) CalculateMultiContextCosts(_ context.Context, _ common.Address, pieceSizes []uint64, refs []sdkcosts.MultiContextRef, _ *sdkcosts.UploadCostOptions) (*sdkcosts.MultiContextCosts, error) {
	c.pieceSizes = append([]uint64(nil), pieceSizes...)
	c.refs = append([]sdkcosts.MultiContextRef(nil), refs...)
	return c.costs, nil
}

type recordingPaymentsFunder struct{ calls int }

func (f *recordingPaymentsFunder) FundSync(context.Context, *big.Int, ...payments.WriteOption) (*sdktypes.WriteResult, error) {
	f.calls++
	return nil, errors.New("unexpected funding")
}
