package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestAPIObservabilityRefreshRunsThroughAdminHandler(t *testing.T) {
	var calls int32
	service := observability.NewService(observability.ServiceOptions{
		Checker: &observabilityAPIRefreshChecker{
			providers: func(context.Context, time.Time, []observability.LocalDataSet) ([]observability.ProviderState, error) {
				atomic.AddInt32(&calls, 1)
				return []observability.ProviderState{
					{ProviderID: onChainID(t, "101"), Status: observability.StatusAvailable, LastCheckedAt: time.Now().UTC()},
				}, nil
			},
		},
		LocalDataSets:   observability.LocalDataSetSourceFunc(func(context.Context) ([]observability.LocalDataSet, error) { return nil, nil }),
		Store:           &observabilityAPIStore{},
		RefreshInterval: time.Minute,
	})

	tests := []struct {
		name       string
		addr       string
		wantStatus int
		wantCalls  int32
	}{
		{name: "loopback", addr: "127.0.0.1:9090", wantStatus: http.StatusOK, wantCalls: 1},
		{name: "non-loopback", addr: "0.0.0.0:9090", wantStatus: http.StatusOK, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt32(&calls, 0)
			srv := &Server{addr: tt.addr, observability: service, logger: testLogger()}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/providers/refresh", nil)
			rr := httptest.NewRecorder()

			srv.handleAPIRefreshObservabilityProviders(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if atomic.LoadInt32(&calls) != tt.wantCalls {
				t.Fatalf("refresh calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantStatus == http.StatusOK {
				var body observability.ProviderObservationPage
				if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if len(body.Items) != 1 || body.Items[0].Facts.ProviderID.String() != "101" {
					t.Fatalf("items = %+v, want provider 101", body.Items)
				}
				if body.SummarySignal.Level != observability.SignalOK {
					t.Fatalf("summary signal = %#v, want ok", body.SummarySignal)
				}
			}
		})
	}
}

func TestAPIObservabilityRefreshExtendsWriteDeadline(t *testing.T) {
	service := observability.NewService(observability.ServiceOptions{
		Checker: &observabilityAPIRefreshChecker{
			providers: func(context.Context, time.Time, []observability.LocalDataSet) ([]observability.ProviderState, error) {
				return []observability.ProviderState{
					{ProviderID: onChainID(t, "101"), Status: observability.StatusAvailable, LastCheckedAt: time.Now().UTC()},
				}, nil
			},
		},
		LocalDataSets:  observability.LocalDataSetSourceFunc(func(context.Context) ([]observability.LocalDataSet, error) { return nil, nil }),
		Store:          &observabilityAPIStore{},
		RefreshTimeout: time.Minute,
	})
	srv := &Server{addr: "127.0.0.1:9090", observability: service, logger: testLogger()}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/providers/refresh", nil)
	rr := &writeDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	start := time.Now()

	srv.handleAPIRefreshObservabilityProviders(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if len(rr.deadlines) != 1 {
		t.Fatalf("write deadlines = %d, want 1", len(rr.deadlines))
	}
	if rr.deadlines[0].Before(start.Add(time.Minute)) {
		t.Fatalf("write deadline = %s, want at least refresh timeout from start %s", rr.deadlines[0], start)
	}
}

func TestAPIObservabilityDataSetBucketFilters(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	numericBucket := testutil.SeedBucket(t, db, "123")
	idBucket := testutil.SeedBucket(t, db, "bucket-id")
	store := &observabilityAPIStore{}
	service := observability.NewService(observability.ServiceOptions{
		Store:           store,
		RefreshInterval: time.Minute,
	})
	srv := &Server{
		addr:          "127.0.0.1:9090",
		repos:         repos,
		observability: service,
		logger:        testLogger(),
	}

	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantBucket int64
	}{
		{name: "bucket name", path: "/api/v1/observability/data-sets?bucket=123", wantStatus: http.StatusOK, wantBucket: numericBucket.ID},
		{name: "bucket id", path: "/api/v1/observability/data-sets?bucket_id=" + strconv.FormatInt(idBucket.ID, 10), wantStatus: http.StatusOK, wantBucket: idBucket.ID},
		{name: "mutually exclusive", path: "/api/v1/observability/data-sets?bucket=123&bucket_id=" + strconv.FormatInt(idBucket.ID, 10), wantStatus: http.StatusBadRequest},
		{name: "invalid bucket id", path: "/api/v1/observability/data-sets?bucket_id=0", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.lastDataSetListOptions = observability.ListOptions{}
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rr := httptest.NewRecorder()

			srv.handleAPIObservabilityDataSets(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && store.lastDataSetListOptions.BucketID != tt.wantBucket {
				t.Fatalf("bucket filter = %d, want %d", store.lastDataSetListOptions.BucketID, tt.wantBucket)
			}
		})
	}
}

func TestAPIObservabilityProviders(t *testing.T) {
	service := observability.NewService(observability.ServiceOptions{
		Store: &observabilityAPIStore{
			providers: []observability.ProviderState{
				{ProviderID: onChainID(t, "101"), Status: observability.StatusAvailable},
			},
		},
	})
	srv := &Server{addr: "127.0.0.1:9090", observability: service, logger: testLogger()}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/observability/providers", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIObservabilityProviders(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	var body observability.ProviderObservationPage
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].Facts.ProviderID.String() != "101" {
		t.Fatalf("items = %+v, want provider 101", body.Items)
	}
}

func TestAPIObservabilityRefreshDataSets(t *testing.T) {
	var calls int32
	service := observability.NewService(observability.ServiceOptions{
		Checker: &observabilityAPIRefreshChecker{
			dataSets: func(context.Context, time.Time, []observability.LocalDataSet) ([]observability.DataSetState, error) {
				atomic.AddInt32(&calls, 1)
				return []observability.DataSetState{
					{
						LocalDataSetID: 101,
						BucketID:       7,
						BucketName:     "photos",
						ProviderID:     onChainID(t, "202"),
						LocalStatus:    "ready",
						Status:         observability.StatusAvailable,
					},
				}, nil
			},
		},
		LocalDataSets: observability.LocalDataSetSourceFunc(func(context.Context) ([]observability.LocalDataSet, error) { return nil, nil }),
		Store:         &observabilityAPIStore{},
	})
	srv := &Server{addr: "127.0.0.1:9090", observability: service, logger: testLogger()}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/observability/data-sets/refresh", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIRefreshObservabilityDataSets(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if gotCalls := atomic.LoadInt32(&calls); gotCalls != 1 {
		t.Fatalf("refresh calls = %d, want 1", gotCalls)
	}

	var body observability.DataSetObservationPage
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].Facts.LocalDataSetID != 101 {
		t.Fatalf("items = %+v, want dataset 101", body.Items)
	}
}

type observabilityAPIRefreshChecker struct {
	providers func(context.Context, time.Time, []observability.LocalDataSet) ([]observability.ProviderState, error)
	dataSets  func(context.Context, time.Time, []observability.LocalDataSet) ([]observability.DataSetState, error)
}

func (c *observabilityAPIRefreshChecker) CheckProviders(ctx context.Context, checkedAt time.Time, local []observability.LocalDataSet) ([]observability.ProviderState, error) {
	if c.providers == nil {
		return nil, nil
	}
	return c.providers(ctx, checkedAt, local)
}

func (c *observabilityAPIRefreshChecker) CheckDataSets(ctx context.Context, checkedAt time.Time, local []observability.LocalDataSet) ([]observability.DataSetState, error) {
	if c.dataSets != nil {
		return c.dataSets(ctx, checkedAt, local)
	}
	return nil, nil
}

type observabilityAPIStore struct {
	providers              []observability.ProviderState
	dataSets               []observability.DataSetState
	providerLastCheckedAt  *time.Time
	dataSetLastCheckedAt   *time.Time
	lastDataSetListOptions observability.ListOptions
}

func (s *observabilityAPIStore) ReplaceProviderStates(_ context.Context, checkedAt time.Time, states []observability.ProviderState) error {
	s.providers = states
	s.providerLastCheckedAt = &checkedAt
	return nil
}

func (s *observabilityAPIStore) ListProviderStates(_ context.Context, opts observability.ListOptions) (observability.ProviderStatePage, error) {
	return observability.ProviderStatePage{
		Items:         s.providers,
		Summary:       observability.Summary{Total: len(s.providers), Available: len(s.providers)},
		LastCheckedAt: s.providerLastCheckedAt,
		Total:         len(s.providers),
		Limit:         opts.Limit,
		Offset:        opts.Offset,
	}, nil
}

func (s *observabilityAPIStore) ReplaceDataSetStates(_ context.Context, checkedAt time.Time, states []observability.DataSetState) error {
	s.dataSets = states
	s.dataSetLastCheckedAt = &checkedAt
	return nil
}

func (s *observabilityAPIStore) ListDataSetStates(_ context.Context, opts observability.ListOptions) (observability.DataSetStatePage, error) {
	s.lastDataSetListOptions = opts
	return observability.DataSetStatePage{
		Items:         s.dataSets,
		Summary:       observability.Summary{Total: len(s.dataSets), Available: len(s.dataSets)},
		LastCheckedAt: s.dataSetLastCheckedAt,
		Total:         len(s.dataSets),
		Limit:         opts.Limit,
		Offset:        opts.Offset,
	}, nil
}

func (s *observabilityAPIStore) GetDataSetStatesByLocalIDs(context.Context, []int64) (map[int64]observability.DataSetState, error) {
	return nil, nil
}
