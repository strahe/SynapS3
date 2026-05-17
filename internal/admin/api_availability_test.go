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

	"github.com/strahe/synaps3/internal/availability"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestAPIAvailabilityRefreshRequiresLoopbackAndWriteHeader(t *testing.T) {
	var calls int32
	service := availability.NewService(availability.ServiceOptions{
		Checker: &availabilityAPIRefreshChecker{
			providers: func(context.Context, []availability.LocalDataSet) ([]availability.ProviderSnapshot, error) {
				atomic.AddInt32(&calls, 1)
				return []availability.ProviderSnapshot{
					{ProviderID: onChainID(t, "101"), Status: availability.StatusAvailable, LastCheckedAt: time.Now().UTC()},
				}, nil
			},
		},
		LocalDataSets:   availability.LocalDataSetSourceFunc(func(context.Context) ([]availability.LocalDataSet, error) { return nil, nil }),
		Store:           &availabilityAPIStore{},
		RefreshInterval: time.Minute,
	})

	tests := []struct {
		name       string
		addr       string
		header     string
		wantStatus int
		wantCalls  int32
	}{
		{name: "non-loopback", addr: "0.0.0.0:9090", header: settingsWriteHeaderValue, wantStatus: http.StatusForbidden},
		{name: "missing header", addr: "127.0.0.1:9090", wantStatus: http.StatusBadRequest},
		{name: "allowed", addr: "127.0.0.1:9090", header: settingsWriteHeaderValue, wantStatus: http.StatusOK, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt32(&calls, 0)
			srv := &Server{addr: tt.addr, availability: service, logger: testLogger()}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/availability/providers/refresh", nil)
			if tt.header != "" {
				req.Header.Set(settingsWriteHeader, tt.header)
			}
			rr := httptest.NewRecorder()

			srv.handleAPIRefreshAvailabilityProviders(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if atomic.LoadInt32(&calls) != tt.wantCalls {
				t.Fatalf("refresh calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantStatus == http.StatusOK {
				var body struct {
					Items    []availability.ProviderSnapshot `json:"items"`
					Warnings []string                        `json:"warnings"`
				}
				if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if len(body.Items) != 1 || body.Items[0].ProviderID.String() != "101" {
					t.Fatalf("items = %+v, want provider 101", body.Items)
				}
				if body.Warnings == nil || len(body.Warnings) != 0 {
					t.Fatalf("warnings = %#v, want empty array", body.Warnings)
				}
			}
		})
	}
}

func TestAPIAvailabilityRefreshExtendsWriteDeadline(t *testing.T) {
	service := availability.NewService(availability.ServiceOptions{
		Checker: &availabilityAPIRefreshChecker{
			providers: func(context.Context, []availability.LocalDataSet) ([]availability.ProviderSnapshot, error) {
				return []availability.ProviderSnapshot{
					{ProviderID: onChainID(t, "101"), Status: availability.StatusAvailable, LastCheckedAt: time.Now().UTC()},
				}, nil
			},
		},
		LocalDataSets:  availability.LocalDataSetSourceFunc(func(context.Context) ([]availability.LocalDataSet, error) { return nil, nil }),
		Store:          &availabilityAPIStore{},
		RefreshTimeout: time.Minute,
	})
	srv := &Server{addr: "127.0.0.1:9090", availability: service, logger: testLogger()}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/availability/providers/refresh", nil)
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := &writeDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	start := time.Now()

	srv.handleAPIRefreshAvailabilityProviders(rr, req)

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

func TestAPIAvailabilityDataSetBucketFilters(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	numericBucket := testutil.SeedBucket(t, db, "123")
	idBucket := testutil.SeedBucket(t, db, "bucket-id")
	store := &availabilityAPIStore{}
	service := availability.NewService(availability.ServiceOptions{
		Store:           store,
		RefreshInterval: time.Minute,
	})
	srv := &Server{
		addr:         "127.0.0.1:9090",
		repos:        repos,
		availability: service,
		logger:       testLogger(),
	}

	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantBucket int64
	}{
		{name: "bucket name", path: "/api/v1/availability/data-sets?bucket=123", wantStatus: http.StatusOK, wantBucket: numericBucket.ID},
		{name: "bucket id", path: "/api/v1/availability/data-sets?bucket_id=" + strconv.FormatInt(idBucket.ID, 10), wantStatus: http.StatusOK, wantBucket: idBucket.ID},
		{name: "mutually exclusive", path: "/api/v1/availability/data-sets?bucket=123&bucket_id=" + strconv.FormatInt(idBucket.ID, 10), wantStatus: http.StatusBadRequest},
		{name: "invalid bucket id", path: "/api/v1/availability/data-sets?bucket_id=0", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.lastDataSetListOptions = availability.ListOptions{}
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rr := httptest.NewRecorder()

			srv.handleAPIAvailabilityDataSets(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && store.lastDataSetListOptions.BucketID != tt.wantBucket {
				t.Fatalf("bucket filter = %d, want %d", store.lastDataSetListOptions.BucketID, tt.wantBucket)
			}
		})
	}
}

type availabilityAPIRefreshChecker struct {
	providers func(context.Context, []availability.LocalDataSet) ([]availability.ProviderSnapshot, error)
}

func (c *availabilityAPIRefreshChecker) CheckProviders(ctx context.Context, local []availability.LocalDataSet) ([]availability.ProviderSnapshot, error) {
	return c.providers(ctx, local)
}

func (c *availabilityAPIRefreshChecker) CheckDataSets(context.Context, []availability.LocalDataSet) ([]availability.DataSetSnapshot, error) {
	return nil, nil
}

type availabilityAPIStore struct {
	providers              []availability.ProviderSnapshot
	lastDataSetListOptions availability.ListOptions
}

func (s *availabilityAPIStore) ReplaceProviderSnapshots(_ context.Context, snapshots []availability.ProviderSnapshot) error {
	s.providers = snapshots
	return nil
}

func (s *availabilityAPIStore) ListProviderSnapshots(_ context.Context, opts availability.ListOptions) (availability.ProviderSnapshotPage, error) {
	return availability.ProviderSnapshotPage{
		Items:         s.providers,
		Summary:       availability.Summary{Total: len(s.providers), Available: len(s.providers)},
		LastCheckedAt: latestProviderCheckedAt(s.providers),
		Total:         len(s.providers),
		Limit:         opts.Limit,
		Offset:        opts.Offset,
	}, nil
}

func (s *availabilityAPIStore) ReplaceDataSetSnapshots(context.Context, []availability.DataSetSnapshot) error {
	return nil
}

func (s *availabilityAPIStore) ListDataSetSnapshots(_ context.Context, opts availability.ListOptions) (availability.DataSetSnapshotPage, error) {
	s.lastDataSetListOptions = opts
	return availability.DataSetSnapshotPage{}, nil
}

func (s *availabilityAPIStore) GetDataSetSnapshotsByLocalIDs(context.Context, []int64) (map[int64]availability.DataSetSnapshot, error) {
	return nil, nil
}

func latestProviderCheckedAt(rows []availability.ProviderSnapshot) *time.Time {
	if len(rows) == 0 {
		return nil
	}
	last := rows[0].LastCheckedAt
	return &last
}
