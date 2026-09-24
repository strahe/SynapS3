package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type overviewHealthRepository struct {
	repository.ObservabilityRepository
	dataSets  []model.StorageDataSet
	providers []observability.ProviderState
	states    []observability.DataSetState
	checkedAt time.Time
	err       error
}

func (r *overviewHealthRepository) OverviewStorageStates(context.Context) ([]model.StorageDataSet, []observability.ProviderState, []observability.DataSetState, *time.Time, *time.Time, error) {
	return r.dataSets, r.providers, r.states, &r.checkedAt, &r.checkedAt, r.err
}

func overviewHealthRepo(base repository.ObservabilityRepository, providerStatuses, dataSetStatuses []observability.Status, checkedAt time.Time) repository.ObservabilityRepository {
	r := &overviewHealthRepository{ObservabilityRepository: base, checkedAt: checkedAt}
	for i, status := range providerStatuses {
		r.providers = append(r.providers, observability.ProviderState{ProviderID: types.NewOnChainID(uint64(i + 1)), Status: status})
	}
	for i, status := range dataSetStatuses {
		providerID := types.NewOnChainID(uint64(i%len(providerStatuses) + 1))
		r.dataSets = append(r.dataSets, model.StorageDataSet{ID: int64(i + 1), ProviderID: providerID})
		r.states = append(r.states, observability.DataSetState{LocalDataSetID: int64(i + 1), Status: status})
	}
	return r
}

func TestAPIOverviewFilecoinStorageHealthUsesObservabilitySummaries(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Now().UTC()
	repos.Observability = overviewHealthRepo(repos.Observability, []observability.Status{observability.StatusAvailable, observability.StatusAvailable}, []observability.Status{observability.StatusAvailable, observability.StatusAvailable, observability.StatusAvailable}, checkedAt)
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityStateStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 2, Available: 2},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{
					Summary:       observability.Summary{Total: 3, Available: 3},
					LastCheckedAt: &checkedAt,
				},
			},
			RefreshInterval: time.Minute,
			Now:             func() time.Time { return checkedAt },
		}))

	body := decodeOverviewResponse(t, srv)
	if body.FilecoinStorageHealth.Level != observability.SignalOK {
		t.Fatalf("filecoin storage health level = %s, want ok", body.FilecoinStorageHealth.Level)
	}
	if body.FilecoinStorageHealth.Providers == nil || body.FilecoinStorageHealth.Providers.Summary.Available != 2 {
		t.Fatalf("provider summary = %#v, want available=2", body.FilecoinStorageHealth.Providers)
	}
	if body.FilecoinStorageHealth.DataSets == nil || body.FilecoinStorageHealth.DataSets.Summary.Available != 3 {
		t.Fatalf("data set summary = %#v, want available=3", body.FilecoinStorageHealth.DataSets)
	}
	if len(body.FilecoinStorageHealth.PartialErrors) != 0 {
		t.Fatalf("partial errors = %#v, want empty", body.FilecoinStorageHealth.PartialErrors)
	}
}

func TestAPIOverviewFilecoinStorageHealthWarnsForObservabilitySignalsWithoutReinterpretingSummary(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Now().UTC()
	repos.Observability = overviewHealthRepo(repos.Observability, []observability.Status{observability.StatusDegraded}, []observability.Status{observability.StatusUnknown}, checkedAt)
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityStateStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 1, Degraded: 1},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{
					Summary:       observability.Summary{Total: 1, Unknown: 1},
					LastCheckedAt: &checkedAt,
				},
			},
			RefreshInterval: time.Minute,
			Now:             func() time.Time { return checkedAt },
		}))

	body := decodeOverviewResponse(t, srv)
	if body.FilecoinStorageHealth.Level != observability.SignalWarning {
		t.Fatalf("filecoin storage health level = %s, want warning", body.FilecoinStorageHealth.Level)
	}
}

func TestAPIOverviewKeepsBlockingLevelWhenObservationsAreStale(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Now().UTC().Add(-10 * time.Minute)
	repos.Observability = overviewHealthRepo(repos.Observability, []observability.Status{observability.StatusUnavailable}, []observability.Status{observability.StatusAvailable}, checkedAt)
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityStateStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 1, Unavailable: 1},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{
					Summary:       observability.Summary{Total: 1, Available: 1},
					LastCheckedAt: &checkedAt,
				},
			},
			RefreshInterval: time.Minute,
			Now:             func() time.Time { return checkedAt },
		}))

	body := decodeOverviewResponse(t, srv)
	if body.FilecoinStorageHealth.Level != observability.SignalBlocking {
		t.Fatalf("filecoin storage health level = %s, want blocking", body.FilecoinStorageHealth.Level)
	}
	if !body.FilecoinStorageHealth.Providers.SummarySignal.Freshness.Stale {
		t.Fatal("provider observation should be stale")
	}
}

func TestAPIOverviewFilecoinStorageHealthHandlesMissingObservability(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())

	body := decodeOverviewResponse(t, srv)
	if body.FilecoinStorageHealth.Level != observability.SignalWarning {
		t.Fatalf("filecoin storage health level = %s, want warning", body.FilecoinStorageHealth.Level)
	}
	if body.FilecoinStorageHealth.Providers != nil || body.FilecoinStorageHealth.DataSets != nil {
		t.Fatalf("observability sections = providers:%#v data_sets:%#v, want nil", body.FilecoinStorageHealth.Providers, body.FilecoinStorageHealth.DataSets)
	}
	if got := body.FilecoinStorageHealth.PartialErrors["observability"]; got != "observability not available" {
		t.Fatalf("observability partial error = %q, want sanitized missing service", got)
	}
}

func TestAPIOverviewFilecoinStorageHealthHandlesObservabilityQueryFailures(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	repos.Observability = &overviewHealthRepository{ObservabilityRepository: repos.Observability, err: errors.New("provider rpc failed with sensitive detail")}
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityStateStore{
				providerErr: errors.New("provider rpc failed with sensitive detail"),
				dataSetErr:  errors.New("data set rpc failed with sensitive detail"),
			},
			RefreshInterval: time.Minute,
		}))

	body := decodeOverviewResponse(t, srv)
	if body.FilecoinStorageHealth.Level != observability.SignalWarning {
		t.Fatalf("filecoin storage health level = %s, want warning", body.FilecoinStorageHealth.Level)
	}
	if got := body.FilecoinStorageHealth.PartialErrors["observability"]; got != "storage health query failed" {
		t.Fatalf("storage partial error = %q, want sanitized query failure", got)
	}
}

func TestAPIOverviewFilecoinStorageHealthIgnoresTaskPressure(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	taskService := newAdminTestTaskService(t, repos)
	checkedAt := time.Now().UTC()
	repos.Observability = overviewHealthRepo(repos.Observability, []observability.Status{observability.StatusAvailable}, []observability.Status{observability.StatusAvailable}, checkedAt)
	overviewSeedTask(t, taskService, repos, model.TaskTypeStorageStore, "running", model.TaskStatusRunning)
	overviewSeedTask(t, taskService, repos, model.TaskTypeStorageStore, "failed", model.TaskStatusFailed)
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityStateStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 1, Available: 1},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{
					Summary:       observability.Summary{Total: 1, Available: 1},
					LastCheckedAt: &checkedAt,
				},
			},
			RefreshInterval: time.Minute,
			Now:             func() time.Time { return checkedAt },
		}))

	body, raw := decodeOverviewResponseWithRaw(t, srv)
	if body.FilecoinStorageHealth.Level != observability.SignalOK {
		t.Fatalf("filecoin storage health level = %s, want ok when only task pressure exists", body.FilecoinStorageHealth.Level)
	}
	if _, ok := raw["filecoin_health"]; ok {
		t.Fatalf("overview includes legacy filecoin_health key, want filecoin_storage_health")
	}
	storageHealth, ok := raw["filecoin_storage_health"].(map[string]any)
	if !ok {
		t.Fatalf("filecoin_storage_health raw response = %#v, want object", raw["filecoin_storage_health"])
	}
	if _, ok := storageHealth["task_pressure"]; ok {
		t.Fatalf("filecoin_storage_health includes task_pressure, want Observability-only payload: %#v", storageHealth)
	}
}

func TestAPIOverviewDoesNotCallFilecoinReadiness(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	probe := &fakeFilecoinReadinessProbe{}
	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir()}, 100, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithFilecoinReadiness(probe)

	_ = decodeOverviewResponse(t, srv)
	if probe.runtimeCalls != 0 {
		t.Fatalf("runtime readiness calls = %d, want 0", probe.runtimeCalls)
	}
}

func TestAPIOverviewIncludesAttentionAndActivePipeline(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	taskService := newAdminTestTaskService(t, repos)
	ctx := context.Background()
	bucket := overviewSeedBucket(t, db, "overview-bucket")

	healthy := overviewObjectVersion(t, repos, bucket.ID, "healthy.txt", "01J00000000000000000000B01")
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, healthy); err != nil {
		t.Fatalf("seed healthy object: %v", err)
	}
	// A version needs attention when its content's ingest failed, which is a
	// fact about the copies rather than a state written on the version.
	failed := overviewObjectVersion(t, repos, bucket.ID, "failed.txt", "01J00000000000000000000B02")
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, failed); err != nil {
		t.Fatalf("seed failed object: %v", err)
	}
	overviewSeedFailedCopy(t, db, repos, bucket.ID, *failed.ContentID)

	// A version is unavailable when nothing can serve it: no cached bytes and
	// no readable committed copy.
	unavailable := overviewObjectVersion(t, repos, bucket.ID, "unavailable.txt", "01J00000000000000000000B03")
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unavailable); err != nil {
		t.Fatalf("seed unavailable object: %v", err)
	}
	if err := repos.Objects.ClearContentCachePresence(ctx, *unavailable.ContentID); err != nil {
		t.Fatalf("clear unavailable cache presence: %v", err)
	}

	overviewSeedTask(t, taskService, repos, model.TaskTypeStorageStore, "store-running", model.TaskStatusRunning)
	overviewSeedTask(t, taskService, repos, model.TaskTypeStorageCleanup, "cleanup-running", model.TaskStatusRunning)
	overviewSeedTask(t, taskService, repos, model.TaskTypeUploadPlan, "plan-completed", model.TaskStatusCompleted)
	overviewSeedTask(t, taskService, repos, model.TaskTypeStorageStore, "store-failed", model.TaskStatusFailed)
	dismissed := overviewSeedTask(t, taskService, repos, model.TaskTypeStoragePull, "pull-dismissed", model.TaskStatusFailed)
	if err := repos.Tasks.AcknowledgeFailed(ctx, dismissed.ID, time.Hour); err != nil {
		t.Fatalf("acknowledge failed task: %v", err)
	}
	overviewSeedTask(t, taskService, repos, model.TaskTypeUploadPlan, "plan-pending", model.TaskStatusPending)
	overviewSeedTask(t, taskService, repos, model.TaskTypeStorageCommit, "commit-pending", model.TaskStatusPending)
	overviewSeedTask(t, taskService, repos, model.TaskTypeCacheEvict, "evict-pending", model.TaskStatusPending)

	srv := newTestServer(":0", db, &stubCache{rootDir: t.TempDir(), usedByte: 42}, 100, repos, &stubWorkerHealth{health: map[string]bool{"uploader": true}}, nil, config.DefaultFilecoinCopies, testLogger())
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/overview", nil)
	srv.handleAPIOverview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}

	var body struct {
		Objects struct {
			ByState        map[string]int64 `json:"by_state"`
			TotalSizeBytes int64            `json:"total_size_bytes"`
			Attention      struct {
				NeedsAttention int64 `json:"needs_attention"`
				Unavailable    int64 `json:"unavailable"`
			} `json:"attention"`
		} `json:"objects"`
		Tasks struct {
			ByStatus  map[string]int64 `json:"by_status"`
			Attention struct {
				Failed int64 `json:"failed"`
			} `json:"attention"`
			ActivePipeline []struct {
				Operation string           `json:"operation"`
				ByStatus  map[string]int64 `json:"by_status"`
				Total     int64            `json:"total"`
			} `json:"active_pipeline"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if body.Objects.ByState[string(model.ObjectStateCached)] == 0 {
		t.Fatal("overview should report derived object state counts")
	}
	if body.Objects.TotalSizeBytes != 30 {
		t.Fatalf("total_size_bytes = %d, want 30", body.Objects.TotalSizeBytes)
	}
	if body.Tasks.ByStatus[string(model.TaskStatusCompleted)] == 0 {
		t.Fatal("overview should keep legacy task status counts")
	}
	if body.Objects.Attention.NeedsAttention != 1 {
		t.Fatalf("needs_attention = %d, want 1", body.Objects.Attention.NeedsAttention)
	}
	if body.Objects.Attention.Unavailable != 1 {
		t.Fatalf("unavailable = %d, want 1", body.Objects.Attention.Unavailable)
	}
	if body.Tasks.Attention.Failed != 1 {
		t.Fatalf("task attention failed = %d, want 1", body.Tasks.Attention.Failed)
	}
	pipeline := make(map[string]struct {
		total    int64
		byStatus map[string]int64
	})
	for _, row := range body.Tasks.ActivePipeline {
		pipeline[row.Operation] = struct {
			total    int64
			byStatus map[string]int64
		}{total: row.Total, byStatus: row.ByStatus}
	}
	if pipeline[string(model.TaskTypeUploadPlan)].byStatus[string(model.TaskStatusPending)] != 1 {
		t.Fatalf("upload plan pending = %d, want 1", pipeline[string(model.TaskTypeUploadPlan)].byStatus[string(model.TaskStatusPending)])
	}
	if pipeline[string(model.TaskTypeStorageStore)].byStatus[string(model.TaskStatusRunning)] != 1 {
		t.Fatalf("storage store running = %d, want 1", pipeline[string(model.TaskTypeStorageStore)].byStatus[string(model.TaskStatusRunning)])
	}
	if pipeline[string(model.TaskTypeStorageCommit)].byStatus[string(model.TaskStatusPending)] != 1 {
		t.Fatalf("storage commit pending = %d, want 1", pipeline[string(model.TaskTypeStorageCommit)].byStatus[string(model.TaskStatusPending)])
	}
	if pipeline[string(model.TaskTypeCacheEvict)].byStatus[string(model.TaskStatusPending)] != 1 {
		t.Fatalf("cache evict pending = %d, want 1", pipeline[string(model.TaskTypeCacheEvict)].byStatus[string(model.TaskStatusPending)])
	}
	if pipeline[string(model.TaskTypeStorageCleanup)].byStatus[string(model.TaskStatusRunning)] != 1 {
		t.Fatalf("storage cleanup running = %d, want 1", pipeline[string(model.TaskTypeStorageCleanup)].byStatus[string(model.TaskStatusRunning)])
	}
	if pipeline[string(model.TaskTypeStorageStore)].total != 1 {
		t.Fatalf("storage store total = %d, want only active tasks", pipeline[string(model.TaskTypeStorageStore)].total)
	}
}

// overviewSeedFailedCopy binds one copy for a content and fails it, which is
// how a content's ingest failure is now expressed.
func overviewSeedFailedCopy(t *testing.T, db *bun.DB, repos *repository.Repositories, bucketID, contentID int64) {
	t.Helper()
	ctx := context.Background()
	binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucketID, ProviderID: onChainID(t, "909"), CopyIndex: 0, CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("seed failed copy binding: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, contentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: binding.ProviderID,
	}}); err != nil {
		t.Fatalf("seed failed copy: %v", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("error_message = ?", "ingest failed").
		Where("id = ?", contentID).
		Exec(ctx); err != nil {
		t.Fatalf("record content failure: %v", err)
	}
}

func overviewSeedBucket(t *testing.T, db *bun.DB, name string) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if _, err := db.NewInsert().Model(bucket).Exec(context.Background()); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	testutil.OpenBucketReplicaSlots(t, db, bucket.ID, bucket.DefaultCopies)
	return bucket
}

// overviewObjectVersion seeds the content for one version and returns the
// version pointing at it. A data version cannot exist without its bytes.
func overviewObjectVersion(t *testing.T, repos *repository.Repositories, bucketID int64, key, versionID string) *model.ObjectVersion {
	t.Helper()
	content, err := repos.Contents.EnsureContent(context.Background(), repository.EnsureContentInput{
		BucketID:        bucketID,
		ContentSize:     10,
		Checksum:        testutil.StorageChecksum("checksum-" + versionID),
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("seed overview content: %v", err)
	}
	return &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucketID,
		Key:         key,
		ContentID:   &content.ID,
		Size:        10,
		ETag:        "etag-" + versionID,
		ContentType: "text/plain",
	}
}

func overviewSeedTask(
	t *testing.T,
	service *taskengine.Service,
	repos *repository.Repositories,
	taskType model.TaskType,
	key string,
	status model.TaskStatus,
) *model.Task {
	t.Helper()
	availableAt := time.Now()
	if status == model.TaskStatusPending {
		availableAt = availableAt.Add(time.Hour)
	}
	taskRow, _, err := service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: taskType, IdempotencyKey: key, Input: map[string]any{"key": key}, AvailableAt: availableAt,
	})
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", key, err)
	}
	if status == model.TaskStatusPending {
		return taskRow
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("ClaimNext(%s) = %#v, err=%v", key, claimed, err)
	}
	if status == model.TaskStatusRunning {
		return taskRow
	}
	transition := repository.TaskTransition{Status: status, ResumeMode: model.TaskResumeModeRecover}
	if status == model.TaskStatusCompleted || status == model.TaskStatusCancelled {
		retentionUntil := time.Now().Add(7 * 24 * time.Hour)
		transition.RetentionUntil = &retentionUntil
	}
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, transition); err != nil {
		t.Fatalf("Settle(%s): %v", key, err)
	}
	return taskRow
}

func decodeOverviewResponse(t *testing.T, srv *Server) overviewResponse {
	t.Helper()
	body, _ := decodeOverviewResponseWithRaw(t, srv)
	return body
}

func decodeOverviewResponseWithRaw(t *testing.T, srv *Server) (overviewResponse, map[string]any) {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/overview", nil)
	srv.handleAPIOverview(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	data := rr.Body.Bytes()
	var body overviewResponse
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("decode raw overview: %v", err)
	}
	return body, raw
}
