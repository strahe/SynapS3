package observability

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
)

func TestCheckProvidersMergesLocalAndRegistrySources(t *testing.T) {
	now := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)
	checker := NewChecker(CheckerOptions{
		Now: func() time.Time { return now },
		ProviderSource: fakeProviderSource{
			active: []Provider{
				{ID: onChainID(t, "202"), Active: true, HasPDP: true, ServiceURL: "https://provider-202.test"},
			},
			byID: map[string]Provider{
				"101": {ID: onChainID(t, "101"), Active: false, HasPDP: true, ServiceURL: "https://provider-101.test"},
			},
		},
		ProviderHealth: func(context.Context, string, time.Duration) string { return "reachable" },
		Timeout:        time.Second,
		Concurrency:    2,
	})

	got, err := checker.CheckProviders(context.Background(), time.Time{}, []LocalDataSet{
		{ProviderID: onChainID(t, "101")},
	})
	if err != nil {
		t.Fatalf("CheckProviders: %v", err)
	}
	byProvider := providerStatesByID(got)

	assertProviderState(t, byProvider["101"], StatusUnavailable, []ReasonCode{ReasonProviderInactive})
	assertProviderState(t, byProvider["202"], StatusAvailable, nil)
}

func TestCheckProvidersDegradesHTTPFailure(t *testing.T) {
	checker := NewChecker(CheckerOptions{
		ProviderSource: fakeProviderSource{
			active: []Provider{
				{ID: onChainID(t, "202"), Active: true, HasPDP: true, ServiceURL: "https://provider-202.test"},
			},
		},
		ProviderHealth: func(context.Context, string, time.Duration) string { return "unreachable" },
		Timeout:        time.Second,
		Concurrency:    1,
	})

	got, err := checker.CheckProviders(context.Background(), time.Time{}, nil)
	if err != nil {
		t.Fatalf("CheckProviders: %v", err)
	}

	assertProviderState(t, providerStatesByID(got)["202"], StatusDegraded, []ReasonCode{ReasonProviderHTTPUnreachable})
}

func TestCheckProvidersDegradesMissingServiceURL(t *testing.T) {
	checker := NewChecker(CheckerOptions{
		ProviderSource: fakeProviderSource{
			active: []Provider{
				{ID: onChainID(t, "202"), Active: true, HasPDP: true},
			},
		},
		ProviderHealth: func(context.Context, string, time.Duration) string { return "n/a" },
		Timeout:        time.Second,
		Concurrency:    1,
	})

	got, err := checker.CheckProviders(context.Background(), time.Time{}, nil)
	if err != nil {
		t.Fatalf("CheckProviders: %v", err)
	}

	assertProviderState(t, providerStatesByID(got)["202"], StatusDegraded, []ReasonCode{ReasonProviderHTTPUnreachable})
}

func TestCheckProvidersReturnsRegistryListFailure(t *testing.T) {
	checker := NewChecker(CheckerOptions{
		ProviderSource: fakeProviderSource{
			listErr: errors.New("registry unavailable"),
		},
		ProviderHealth: func(context.Context, string, time.Duration) string { return "reachable" },
		Timeout:        time.Second,
		Concurrency:    1,
	})

	if _, err := checker.CheckProviders(context.Background(), time.Time{}, []LocalDataSet{
		{ProviderID: onChainID(t, "101")},
	}); err == nil {
		t.Fatal("CheckProviders returned nil error for registry list failure")
	}
}

func TestCheckProvidersMarksLookupFailureUnknownWithSanitizedError(t *testing.T) {
	checker := NewChecker(CheckerOptions{
		ProviderSource: fakeProviderSource{
			lookupErr: errors.New("lookup unavailable https://rpc.example.test?token=secret"),
		},
		ProviderHealth: func(context.Context, string, time.Duration) string { return "reachable" },
		Timeout:        time.Second,
		Concurrency:    1,
	})

	got, err := checker.CheckProviders(context.Background(), time.Time{}, []LocalDataSet{
		{ProviderID: onChainID(t, "101")},
	})
	if err != nil {
		t.Fatalf("CheckProviders: %v", err)
	}

	state := providerStatesByID(got)["101"]
	assertProviderState(t, state, StatusUnknown, []ReasonCode{ReasonRegistryLookupFailed})
	if state.Active != nil || state.HasPDP != nil || state.ServiceURL != nil || state.HealthStatus != nil {
		t.Fatalf("lookup failure facts = active:%v has_pdp:%v service_url:%v health:%v, want all nil", state.Active, state.HasPDP, state.ServiceURL, state.HealthStatus)
	}
	if state.LastError == nil || *state.LastError != "RPC call failed" {
		t.Fatalf("LastError = %#v, want sanitized RPC call failed", state.LastError)
	}
}

func TestCheckProvidersLimitsLookupConcurrency(t *testing.T) {
	source := newConcurrentProviderSource(t, 2)
	checker := NewChecker(CheckerOptions{
		ProviderSource: source,
		Timeout:        time.Second,
		Concurrency:    2,
	})

	_, err := checker.CheckProviders(context.Background(), time.Time{}, []LocalDataSet{
		{ProviderID: onChainID(t, "101")},
		{ProviderID: onChainID(t, "102")},
		{ProviderID: onChainID(t, "103")},
		{ProviderID: onChainID(t, "104")},
	})
	if err != nil {
		t.Fatalf("CheckProviders: %v", err)
	}
	if source.maxActive.Load() > 2 {
		t.Fatalf("max concurrent lookups = %d, want <= 2", source.maxActive.Load())
	}
}

func TestCheckProvidersMarksMissingPDPUnavailable(t *testing.T) {
	checker := NewChecker(CheckerOptions{
		ProviderSource: fakeProviderSource{
			active: []Provider{
				{ID: onChainID(t, "202"), Active: true, HasPDP: false},
			},
		},
		ProviderHealth: func(context.Context, string, time.Duration) string { return "reachable" },
		Timeout:        time.Second,
		Concurrency:    1,
	})

	got, err := checker.CheckProviders(context.Background(), time.Time{}, nil)
	if err != nil {
		t.Fatalf("CheckProviders: %v", err)
	}

	state := providerStatesByID(got)["202"]
	assertProviderState(t, state, StatusUnavailable, []ReasonCode{ReasonProviderMissingPDP})
	if state.Evidence["has_pdp"] != false {
		t.Fatalf("has_pdp evidence = %#v, want false", state.Evidence["has_pdp"])
	}
}

func TestCheckDataSetsClassifiesLocalAndChainEvidence(t *testing.T) {
	now := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)
	local := []LocalDataSet{
		{
			ID:              1,
			BucketID:        10,
			BucketName:      "alpha",
			ProviderID:      onChainID(t, "101"),
			DataSetID:       onChainIDPtr(t, "1001"),
			ClientDataSetID: onChainIDPtr(t, "9001"),
			Status:          model.StorageDataSetStatusReady,
		},
		{
			ID:         2,
			BucketID:   10,
			BucketName: "alpha",
			ProviderID: onChainID(t, "102"),
			Status:     model.StorageDataSetStatusReady,
		},
		{
			ID:         3,
			BucketID:   11,
			BucketName: "beta",
			ProviderID: onChainID(t, "103"),
			DataSetID:  onChainIDPtr(t, "1003"),
			Status:     model.StorageDataSetStatusReady,
		},
		{
			ID:         4,
			BucketID:   12,
			BucketName: "gamma",
			ProviderID: onChainID(t, "104"),
			DataSetID:  onChainIDPtr(t, "1004"),
			Status:     model.StorageDataSetStatusReady,
		},
		{
			ID:         5,
			BucketID:   13,
			BucketName: "delta",
			ProviderID: onChainID(t, "105"),
			DataSetID:  onChainIDPtr(t, "1005"),
			Status:     model.StorageDataSetStatusReady,
		},
		{
			ID:         6,
			BucketID:   14,
			BucketName: "epsilon",
			ProviderID: onChainID(t, "106"),
			DataSetID:  onChainIDPtr(t, "1006"),
			Status:     model.StorageDataSetStatusReady,
		},
		{
			ID:         7,
			BucketID:   15,
			BucketName: "zeta",
			ProviderID: onChainID(t, "107"),
			DataSetID:  onChainIDPtr(t, "1007"),
			Status:     model.StorageDataSetStatusCreating,
		},
		{
			ID:         8,
			BucketID:   16,
			BucketName: "eta",
			ProviderID: onChainID(t, "108"),
			DataSetID:  onChainIDPtr(t, "1008"),
			Status:     model.StorageDataSetStatusFailed,
		},
		{
			ID:         9,
			BucketID:   17,
			BucketName: "theta",
			ProviderID: onChainID(t, "109"),
			DataSetID:  onChainIDPtr(t, "1009"),
			Status:     model.StorageDataSetStatusDraining,
		},
	}
	activePieces := int64(7)
	checker := NewChecker(CheckerOptions{
		Now: func() time.Time { return now },
		DataSetScanner: fakeDataSetScanner{
			dataSets: []ChainDataSet{
				{
					DataSetID:        onChainID(t, "1001"),
					ClientDataSetID:  onChainIDPtr(t, "9001"),
					ProviderID:       onChainID(t, "101"),
					IsLive:           true,
					IsManaged:        true,
					ActivePieceCount: &activePieces,
					Metadata:         map[string]string{"bucket": "alpha"},
				},
				{
					DataSetID:        onChainID(t, "1004"),
					ClientDataSetID:  onChainIDPtr(t, "9004"),
					ProviderID:       onChainID(t, "104"),
					IsLive:           false,
					IsManaged:        true,
					ActivePieceCount: int64Ptr(0),
					Metadata:         map[string]string{"bucket": "gamma"},
				},
				{
					DataSetID:        onChainID(t, "1005"),
					ClientDataSetID:  onChainIDPtr(t, "9005"),
					ProviderID:       onChainID(t, "105"),
					IsLive:           true,
					IsManaged:        false,
					ActivePieceCount: int64Ptr(3),
					Metadata:         map[string]string{"bucket": "delta"},
				},
				{
					DataSetID:        onChainID(t, "1006"),
					ClientDataSetID:  onChainIDPtr(t, "9006"),
					ProviderID:       onChainID(t, "999"),
					IsLive:           true,
					IsManaged:        true,
					ActivePieceCount: int64Ptr(4),
					Metadata:         map[string]string{"bucket": "other"},
				},
				{
					DataSetID:        onChainID(t, "1007"),
					ClientDataSetID:  onChainIDPtr(t, "9007"),
					ProviderID:       onChainID(t, "107"),
					IsLive:           true,
					IsManaged:        true,
					ActivePieceCount: int64Ptr(1),
					Metadata:         map[string]string{"bucket": "zeta"},
				},
				{
					DataSetID:        onChainID(t, "1008"),
					ClientDataSetID:  onChainIDPtr(t, "9008"),
					ProviderID:       onChainID(t, "108"),
					IsLive:           true,
					IsManaged:        true,
					ActivePieceCount: int64Ptr(2),
					Metadata:         map[string]string{"bucket": "eta"},
				},
				{
					DataSetID:        onChainID(t, "1009"),
					ClientDataSetID:  onChainIDPtr(t, "9009"),
					ProviderID:       onChainID(t, "109"),
					IsLive:           true,
					IsManaged:        true,
					ActivePieceCount: int64Ptr(5),
					Metadata:         map[string]string{"bucket": "theta"},
				},
				{
					DataSetID:        onChainID(t, "1999"),
					ClientDataSetID:  onChainIDPtr(t, "9999"),
					ProviderID:       onChainID(t, "199"),
					IsLive:           true,
					IsManaged:        true,
					ActivePieceCount: int64Ptr(99),
					Metadata:         map[string]string{"bucket": "chain-only"},
				},
			},
		},
	})

	got, err := checker.CheckDataSets(context.Background(), time.Time{}, local)
	if err != nil {
		t.Fatalf("CheckDataSets: %v", err)
	}
	if len(got) != len(local) {
		t.Fatalf("CheckDataSets returned %d states, want %d local bindings", len(got), len(local))
	}
	byLocal := dataSetStatesByLocalID(got)

	assertDataSetState(t, byLocal[1], StatusAvailable, nil, int64Ptr(7))
	assertDataSetState(t, byLocal[2], StatusUnavailable, []ReasonCode{ReasonChainDataSetMissing}, nil)
	assertDataSetState(t, byLocal[3], StatusUnavailable, []ReasonCode{ReasonChainDataSetMissing}, nil)
	assertDataSetState(t, byLocal[4], StatusUnavailable, []ReasonCode{ReasonChainDataSetInactive}, int64Ptr(0))
	assertDataSetState(t, byLocal[5], StatusDegraded, []ReasonCode{ReasonChainDataSetUnmanaged}, int64Ptr(3))
	assertDataSetState(t, byLocal[6], StatusDegraded, []ReasonCode{ReasonProviderMismatch, ReasonMetadataMismatch}, int64Ptr(4))
	assertDataSetState(t, byLocal[7], StatusDegraded, []ReasonCode{ReasonLocalStatusNotReady}, int64Ptr(1))
	assertDataSetState(t, byLocal[8], StatusUnavailable, []ReasonCode{ReasonLocalStatusNotReady}, int64Ptr(2))
	assertDataSetState(t, byLocal[9], StatusAvailable, nil, int64Ptr(5))
}

func TestCheckDataSetsMergesWalletScanFailureWithLocalStatus(t *testing.T) {
	checker := NewChecker(CheckerOptions{
		DataSetScanner: fakeDataSetScanner{err: errors.New("rpc unavailable https://rpc.example.test?token=secret")},
	})

	got, err := checker.CheckDataSets(context.Background(), time.Time{}, []LocalDataSet{
		{
			ID:         1,
			BucketID:   10,
			BucketName: "alpha",
			ProviderID: onChainID(t, "101"),
			DataSetID:  onChainIDPtr(t, "1001"),
			Status:     model.StorageDataSetStatusReady,
		},
		{
			ID:         2,
			BucketID:   10,
			BucketName: "alpha",
			ProviderID: onChainID(t, "102"),
			DataSetID:  onChainIDPtr(t, "1002"),
			Status:     model.StorageDataSetStatusCreating,
		},
		{
			ID:         3,
			BucketID:   10,
			BucketName: "alpha",
			ProviderID: onChainID(t, "103"),
			DataSetID:  onChainIDPtr(t, "1003"),
			Status:     model.StorageDataSetStatusUnavailable,
		},
	})
	if err != nil {
		t.Fatalf("CheckDataSets: %v", err)
	}

	byLocal := dataSetStatesByLocalID(got)
	assertDataSetState(t, byLocal[1], StatusUnknown, []ReasonCode{ReasonChainLookupFailed}, nil)
	assertDataSetState(t, byLocal[2], StatusUnknown, []ReasonCode{ReasonChainLookupFailed, ReasonLocalStatusNotReady}, nil)
	assertDataSetState(t, byLocal[3], StatusUnavailable, []ReasonCode{ReasonChainLookupFailed, ReasonLocalStatusNotReady}, nil)
	for _, id := range []int64{1, 2, 3} {
		if byLocal[id].LastError == nil || *byLocal[id].LastError != "RPC call failed" {
			t.Fatalf("local data set %d LastError = %#v, want sanitized RPC call failed", id, byLocal[id].LastError)
		}
	}
}

type fakeProviderSource struct {
	active    []Provider
	byID      map[string]Provider
	listErr   error
	lookupErr error
}

func (f fakeProviderSource) ListActiveProviders(context.Context) ([]Provider, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.active, nil
}

func (f fakeProviderSource) LookupProvider(_ context.Context, id types.OnChainID) (Provider, error) {
	if f.lookupErr != nil {
		return Provider{}, f.lookupErr
	}
	provider, ok := f.byID[id.String()]
	if !ok {
		return Provider{}, ErrProviderNotFound
	}
	return provider, nil
}

type fakeDataSetScanner struct {
	dataSets []ChainDataSet
	err      error
}

func (f fakeDataSetScanner) ScanWalletDataSets(context.Context) ([]ChainDataSet, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.dataSets, nil
}

func providerStatesByID(states []ProviderState) map[string]ProviderState {
	out := make(map[string]ProviderState, len(states))
	for _, state := range states {
		out[state.ProviderID.String()] = state
	}
	return out
}

func dataSetStatesByLocalID(states []DataSetState) map[int64]DataSetState {
	out := make(map[int64]DataSetState, len(states))
	for _, state := range states {
		out[state.LocalDataSetID] = state
	}
	return out
}

func assertProviderState(t *testing.T, got ProviderState, status Status, reasons []ReasonCode) {
	t.Helper()
	if got.Status != status {
		t.Fatalf("provider %s status = %s, want %s", got.ProviderID, got.Status, status)
	}
	if len(got.ReasonCodes) == 0 && len(reasons) == 0 {
		return
	}
	if !reflect.DeepEqual(got.ReasonCodes, reasons) {
		t.Fatalf("provider %s reason codes = %#v, want %#v", got.ProviderID, got.ReasonCodes, reasons)
	}
}

func assertDataSetState(t *testing.T, got DataSetState, status Status, reasons []ReasonCode, activePieces *int64) {
	t.Helper()
	if got.Status != status {
		t.Fatalf("local data set %d status = %s, want %s", got.LocalDataSetID, got.Status, status)
	}
	if len(got.ReasonCodes) != len(reasons) || (len(reasons) > 0 && !reflect.DeepEqual(got.ReasonCodes, reasons)) {
		t.Fatalf("local data set %d reason codes = %#v, want %#v", got.LocalDataSetID, got.ReasonCodes, reasons)
	}
	if !reflect.DeepEqual(got.ActivePieceCount, activePieces) {
		t.Fatalf("local data set %d active pieces = %#v, want %#v", got.LocalDataSetID, got.ActivePieceCount, activePieces)
	}
}

func onChainID(t *testing.T, value string) types.OnChainID {
	t.Helper()
	id, err := types.ParseOnChainID("test id", value)
	if err != nil {
		t.Fatalf("parse on-chain id %q: %v", value, err)
	}
	return id
}

func onChainIDPtr(t *testing.T, value string) *types.OnChainID {
	t.Helper()
	id := onChainID(t, value)
	return &id
}

func int64Ptr(value int64) *int64 {
	return &value
}

type concurrentProviderSource struct {
	t         *testing.T
	active    atomic.Int32
	maxActive atomic.Int32
	limit     int32
}

func newConcurrentProviderSource(t *testing.T, limit int32) *concurrentProviderSource {
	t.Helper()
	return &concurrentProviderSource{t: t, limit: limit}
}

func (f *concurrentProviderSource) ListActiveProviders(context.Context) ([]Provider, error) {
	return nil, nil
}

func (f *concurrentProviderSource) LookupProvider(ctx context.Context, id types.OnChainID) (Provider, error) {
	active := f.active.Add(1)
	defer f.active.Add(-1)
	for {
		maxActive := f.maxActive.Load()
		if active <= maxActive || f.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	if active > f.limit {
		f.t.Errorf("lookup concurrency = %d, want <= %d", active, f.limit)
	}
	select {
	case <-time.After(10 * time.Millisecond):
	case <-ctx.Done():
		return Provider{}, ctx.Err()
	}
	return Provider{ID: id, Active: true, HasPDP: true}, nil
}
