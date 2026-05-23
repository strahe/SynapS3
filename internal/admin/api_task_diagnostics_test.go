package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

type fakeTaskDiagnosticStatusChecker struct {
	creationResult  synapse.PDPStatusResult
	addPiecesResult synapse.PDPStatusResult
	creationInput   synapse.DataSetCreationStatusInput
	addPiecesInput  synapse.AddPiecesStatusInput
	creationCalls   int
	addPiecesCalls  int
}

func (c *fakeTaskDiagnosticStatusChecker) CheckDataSetCreationStatus(_ context.Context, input synapse.DataSetCreationStatusInput) synapse.PDPStatusResult {
	c.creationCalls++
	c.creationInput = input
	c.creationResult.StatusURL = input.StatusURL
	return c.creationResult
}

func (c *fakeTaskDiagnosticStatusChecker) CheckAddPiecesStatus(_ context.Context, input synapse.AddPiecesStatusInput) synapse.PDPStatusResult {
	c.addPiecesCalls++
	c.addPiecesInput = input
	return c.addPiecesResult
}

func TestAPITaskDiagnosticGETReturnsEvidence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J000000000000000DIAG001",
		IdempotencyKey: "task-diagnostic-non-upload",
		Status:         model.TaskStatusQueued,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	checker := &fakeTaskDiagnosticStatusChecker{}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithTaskDiagnosticStatusChecker(checker)
	body, status := getTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.CurrentState != observability.TaskDiagnosticStateNotApplicable || body.Evidence.Task.Type != model.TaskTypeEvictCache || body.Evidence.LiveCheck != nil {
		t.Fatalf("diagnostic = %#v, want non-upload evidence without live check", body)
	}
	if checker.creationCalls != 0 || checker.addPiecesCalls != 0 {
		t.Fatalf("checker calls = creation:%d add:%d, want none for GET", checker.creationCalls, checker.addPiecesCalls)
	}
}

func TestAPITaskDiagnosticRefreshChecksLiveStatusWithoutMutatingTask(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	task, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-refresh", "01J000000000000000DIAG002")
	before, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID before: %v", err)
	}
	checker := &fakeTaskDiagnosticStatusChecker{
		addPiecesResult: synapse.PDPStatusResult{
			State:             synapse.PDPStatusConfirmed,
			StatusURL:         "https://provider.example/pdp/data-sets/1001/pieces/added/0xcommit",
			TxStatus:          "confirmed",
			DataSetID:         "1001",
			PiecesAdded:       true,
			PieceCount:        1,
			ConfirmedPieceIDs: []string{"2001"},
		},
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos)).
		WithTaskDiagnosticStatusChecker(checker)

	body, status := refreshTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.CurrentState != observability.TaskDiagnosticStateConfirmed || body.Evidence.LiveCheck == nil || body.Evidence.LiveCheck.State != observability.TaskDiagnosticLiveConfirmed {
		t.Fatalf("diagnostic = %#v, want confirmed live check", body)
	}
	if checker.addPiecesInput.DataSetID != "1001" || checker.addPiecesInput.TransactionID != "0xcommit" || checker.addPiecesInput.ExpectedPieceCount != 1 {
		t.Fatalf("checker input = %#v, want add-pieces status facts", checker.addPiecesInput)
	}
	after, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID after: %v", err)
	}
	if after.Status != before.Status || after.RetryCount != before.RetryCount || !after.ScheduledAt.Equal(before.ScheduledAt) {
		t.Fatalf("task mutated = before:%#v after:%#v", before, after)
	}
}

func TestAPITaskDiagnosticRefreshPassesDataSetCreationEvidence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	task, _, _ := seedTaskDiagnosticCreateDataSetTask(t, db, repos, "task-diagnostic-create-status", "01J000000000000000DIAGCRE")
	checker := &fakeTaskDiagnosticStatusChecker{
		creationResult: synapse.PDPStatusResult{
			State:          synapse.PDPStatusPending,
			StatusURL:      "https://provider.example/pdp/data-sets/created/0xcreate",
			TxStatus:       "pending",
			DataSetCreated: false,
		},
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos)).
		WithTaskDiagnosticStatusChecker(checker)

	body, status := refreshTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.Evidence.LiveCheck == nil || body.Evidence.LiveCheck.State != observability.TaskDiagnosticLivePending {
		t.Fatalf("diagnostic = %#v, want pending creation live check", body)
	}
	if checker.creationInput.StatusURL != "https://provider.example/pdp/data-sets/created/0xcreate" ||
		checker.creationInput.TransactionID != "0xcreate" ||
		checker.creationInput.ExpectedDataSetID != "" {
		t.Fatalf("creation input = %#v, want creation status evidence", checker.creationInput)
	}
}

func TestAPITaskDiagnosticRefreshUnavailableStillReturnsDiagnostic(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	task, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-refresh-unavailable", "01J000000000000000DIAG003")
	checker := &fakeTaskDiagnosticStatusChecker{
		addPiecesResult: synapse.PDPStatusResult{
			State:     synapse.PDPStatusUnavailable,
			StatusURL: "https://provider.example/pdp/data-sets/1001/pieces/added/0xcommit",
			Error:     "context deadline exceeded",
		},
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos)).
		WithTaskDiagnosticStatusChecker(checker)

	body, status := refreshTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.CurrentState != observability.TaskDiagnosticStateUnknown || body.Signal.Status != observability.StatusUnknown || body.Evidence.LiveCheck == nil || body.Evidence.LiveCheck.State != observability.TaskDiagnosticLiveUnavailable {
		t.Fatalf("diagnostic = %#v, want unknown unavailable live check", body)
	}
}

func TestAPITaskDiagnosticRefreshOmitsUncertainLiveEvidence(t *testing.T) {
	cases := []struct {
		name   string
		result synapse.PDPStatusResult
	}{
		{
			name: "unavailable",
			result: synapse.PDPStatusResult{
				State:     synapse.PDPStatusUnavailable,
				StatusURL: "https://provider.example/pdp/data-sets/1001/pieces/added/0xcommit",
				Error:     "context deadline exceeded",
			},
		},
		{
			name: "unknown",
			result: synapse.PDPStatusResult{
				State:             synapse.PDPStatusUnknown,
				StatusURL:         "https://provider.example/pdp/data-sets/1001/pieces/added/0xcommit",
				TxStatus:          "queued",
				DataSetID:         "1001",
				PiecesAdded:       false,
				PieceCount:        0,
				ConfirmedPieceIDs: []string{},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			task, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-refresh-uncertain-"+tc.name, "01J000000000000000DIAG004")
			checker := &fakeTaskDiagnosticStatusChecker{addPiecesResult: tc.result}
			srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
				WithObservability(taskDiagnosticObservabilityService(t, repos)).
				WithTaskDiagnosticStatusChecker(checker)

			raw, status := refreshTaskDiagnosticRaw(t, srv, task.ID)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			var payload map[string]any
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatalf("Unmarshal raw diagnostic: %v", err)
			}
			live := payload["evidence"].(map[string]any)["live_check"].(map[string]any)
			if _, ok := live["pieces_added"]; ok {
				t.Fatalf("pieces_added encoded for %s live check: %#v", tc.name, live)
			}
			if _, ok := live["data_set_created"]; ok {
				t.Fatalf("data_set_created encoded for %s live check: %#v", tc.name, live)
			}
			if _, ok := live["piece_count"]; ok {
				t.Fatalf("piece_count encoded for %s live check: %#v", tc.name, live)
			}
			if _, ok := live["confirmed_piece_ids"]; ok {
				t.Fatalf("confirmed_piece_ids encoded for %s live check: %#v", tc.name, live)
			}
		})
	}
}

func TestAPITaskDiagnosticRefreshEncodesFalseLiveCheckFields(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	task, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-refresh-mismatch", "01J000000000000000DIAG004")
	checker := &fakeTaskDiagnosticStatusChecker{
		addPiecesResult: synapse.PDPStatusResult{
			State:       synapse.PDPStatusMismatch,
			StatusURL:   "https://provider.example/pdp/data-sets/1001/pieces/added/0xcommit",
			TxStatus:    "confirmed",
			DataSetID:   "1001",
			PiecesAdded: false,
			PieceCount:  1,
		},
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos)).
		WithTaskDiagnosticStatusChecker(checker)

	raw, status := refreshTaskDiagnosticRaw(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("Unmarshal raw diagnostic: %v", err)
	}
	live := payload["evidence"].(map[string]any)["live_check"].(map[string]any)
	if got, ok := live["pieces_added"].(bool); !ok || got {
		t.Fatalf("pieces_added = %#v, want encoded false", live["pieces_added"])
	}
}

func TestAPITaskDiagnosticRefreshEncodesZeroAndEmptyAddPiecesEvidence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	task, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-refresh-zero-evidence", "01J000000000000000DIAGZRO")
	checker := &fakeTaskDiagnosticStatusChecker{
		addPiecesResult: synapse.PDPStatusResult{
			State:             synapse.PDPStatusMismatch,
			StatusURL:         "https://provider.example/pdp/data-sets/1001/pieces/added/0xcommit",
			TxStatus:          "confirmed",
			DataSetID:         "1001",
			PiecesAdded:       true,
			PieceCount:        0,
			ConfirmedPieceIDs: []string{},
		},
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos)).
		WithTaskDiagnosticStatusChecker(checker)

	raw, status := refreshTaskDiagnosticRaw(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("Unmarshal raw diagnostic: %v", err)
	}
	live := payload["evidence"].(map[string]any)["live_check"].(map[string]any)
	if got, ok := live["piece_count"].(float64); !ok || got != 0 {
		t.Fatalf("piece_count = %#v, want encoded zero", live["piece_count"])
	}
	confirmed, ok := live["confirmed_piece_ids"].([]any)
	if !ok || len(confirmed) != 0 {
		t.Fatalf("confirmed_piece_ids = %#v, want encoded empty array", live["confirmed_piece_ids"])
	}
}

func TestAPITaskDiagnosticRefreshMissingProviderServiceURLReturnsDiagnosticUnavailable(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	task, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-missing-service-url", "01J000000000000000DIAGURL")
	before, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID before: %v", err)
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())

	body, status := refreshTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.Evidence.LiveCheck == nil || body.Evidence.LiveCheck.State != observability.TaskDiagnosticLiveUnavailable {
		t.Fatalf("diagnostic = %#v, want unavailable live check", body)
	}
	if body.Signal.Status != observability.StatusUnavailable || len(body.ReasonCodes) != 1 || body.ReasonCodes[0] != observability.ReasonTaskDiagnosticUnavailable {
		t.Fatalf("diagnostic signal = status:%s reasons:%v, want diagnostic unavailable", body.Signal.Status, body.ReasonCodes)
	}
	after, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID after: %v", err)
	}
	if after.Status != before.Status || after.RetryCount != before.RetryCount || !after.ScheduledAt.Equal(before.ScheduledAt) {
		t.Fatalf("task mutated = before:%#v after:%#v", before, after)
	}
}

func TestAPITaskDiagnosticRefreshSkipsLiveCheckWithoutTransactionEvidence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	stage := "prepare_upload"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J000000000000000DIAG005",
		IdempotencyKey: "task-diagnostic-refresh-prepare",
		Status:         model.TaskStatusQueued,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	checker := &fakeTaskDiagnosticStatusChecker{}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithTaskDiagnosticStatusChecker(checker)

	body, status := refreshTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.CurrentState != observability.TaskDiagnosticStatePreparing || body.Evidence.LiveCheck == nil || body.Evidence.LiveCheck.State != observability.TaskDiagnosticLiveSkipped {
		t.Fatalf("diagnostic = %#v, want preparing diagnosis with skipped live check", body)
	}
	if checker.creationCalls != 0 || checker.addPiecesCalls != 0 {
		t.Fatalf("checker calls = creation:%d add:%d, want none", checker.creationCalls, checker.addPiecesCalls)
	}
}

func TestAPITaskDiagnosticRefreshSkipsAddPiecesWithoutCommitTransactionID(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	task, upload := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-refresh-missing-tx", "01J000000000000000DIAGMTX")
	if _, err := db.NewUpdate().
		Model((*model.StorageUploadCopy)(nil)).
		Set("commit_transaction_id = NULL").
		Where("upload_id = ? AND copy_index = ?", upload.ID, 0).
		Exec(ctx); err != nil {
		t.Fatalf("clear commit transaction: %v", err)
	}
	checker := &fakeTaskDiagnosticStatusChecker{}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos)).
		WithTaskDiagnosticStatusChecker(checker)

	body, status := refreshTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.CurrentState != observability.TaskDiagnosticStateWaitingForChain || body.Evidence.LiveCheck == nil || body.Evidence.LiveCheck.State != observability.TaskDiagnosticLiveSkipped {
		t.Fatalf("diagnostic = %#v, want waiting diagnosis with skipped live check", body)
	}
	if body.Evidence.Transaction != nil {
		t.Fatalf("transaction evidence = %#v, want none without commit transaction ID", body.Evidence.Transaction)
	}
	if checker.creationCalls != 0 || checker.addPiecesCalls != 0 {
		t.Fatalf("checker calls = creation:%d add:%d, want none", checker.creationCalls, checker.addPiecesCalls)
	}
}

func TestAPITaskDiagnosticMissingCopyIndexDoesNotReadReplicaZero(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	seeded, upload := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-missing-copy-index", "01J000000000000000DIAG006")
	stage := "ingress_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        seeded.RefType,
		RefID:          seeded.RefID,
		RefVersionID:   seeded.RefVersionID,
		IdempotencyKey: "task-diagnostic-missing-copy-index-no-copy",
		Payload:        map[string]interface{}{"upload_id": upload.ID},
		Status:         model.TaskStatusWaiting,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos))

	body, status := getTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.Evidence.Copy != nil || body.Evidence.Transaction != nil {
		t.Fatalf("diagnostic = %#v, want no inferred replica zero copy or transaction", body)
	}
	if body.CurrentState != observability.TaskDiagnosticStateUnknown {
		t.Fatalf("state = %s, want unknown missing evidence", body.CurrentState)
	}
}

func TestAPITaskDiagnosticMissingUploadIDDoesNotFallbackToLatestUpload(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	seeded, _ := seedTaskDiagnosticCommitTask(t, db, repos, "task-diagnostic-missing-upload-id", "01J000000000000000DIAG007")
	stage := "ingress_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        seeded.RefType,
		RefID:          seeded.RefID,
		RefVersionID:   seeded.RefVersionID,
		IdempotencyKey: "task-diagnostic-missing-upload-id-no-fallback",
		Payload:        map[string]interface{}{"copy_index": 0},
		Status:         model.TaskStatusWaiting,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger()).
		WithObservability(taskDiagnosticObservabilityService(t, repos))

	body, status := getTaskDiagnostic(t, srv, task.ID)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body.Evidence.Upload != nil || body.Evidence.Copy != nil || body.Evidence.Transaction != nil {
		t.Fatalf("diagnostic = %#v, want no upload fallback evidence", body)
	}
}

func TestAPITaskDiagnosticInvalidIDReturns400(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())

	_, status := getTaskDiagnostic(t, srv, 0)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}

func TestAPITaskDiagnosticMissingTaskReturns404(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	srv := New(":0", db, nil, 0, repos, nil, nil, config.DefaultFilecoinCopies, testLogger())
	_, status := getTaskDiagnostic(t, srv, 404)
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
}

func seedTaskDiagnosticCommitTask(t *testing.T, db *bun.DB, repos *repository.Repositories, key string, versionID string) (*model.Task, *model.StorageUpload) {
	t.Helper()
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, key+"-bucket")
	objectID, seededVersionID := seedAdminObjectVersion(t, repos, bucket, key+".txt", 20, key+"-etag", key+"-checksum", "text/plain", "", model.ObjectStateUploading)
	if seededVersionID != versionID {
		versionID = seededVersionID
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     20,
		Checksum:        key + "-checksum",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  upload.ID,
		DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-" + key,
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID:            upload.ID,
		CopyIndex:           0,
		CommitExtraDataHex:  "0xextra",
		CommitTransactionID: "0xcommit",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitting: %v", err)
	}
	stage := "ingress_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: key,
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusWaiting,
		RetryCount:     2,
		MaxRetries:     5,
		ScheduledAt:    time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	return task, upload
}

func seedTaskDiagnosticCreateDataSetTask(t *testing.T, db *bun.DB, repos *repository.Repositories, key string, versionID string) (*model.Task, *model.StorageUpload, *model.StorageDataSet) {
	t.Helper()
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, key+"-bucket")
	objectID, seededVersionID := seedAdminObjectVersion(t, repos, bucket, key+".txt", 20, key+"-etag", key+"-checksum", "text/plain", "", model.ObjectStateUploading)
	if seededVersionID != versionID {
		versionID = seededVersionID
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     20,
		Checksum:        key + "-checksum",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID:            binding.ID,
		UploadID:      upload.ID,
		TransactionID: "0xcreate",
		StatusURL:     "https://provider.example/pdp/data-sets/created/0xcreate",
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	binding, err = repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID: %v", err)
	}
	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: key,
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0},
		Status:         model.TaskStatusWaiting,
		MaxRetries:     5,
		ScheduledAt:    time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	return task, upload, binding
}

func taskDiagnosticObservabilityService(t *testing.T, repos *repository.Repositories) *observability.Service {
	t.Helper()
	now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	serviceURL := "https://provider.example"
	if err := repos.Observability.ReplaceProviderStates(context.Background(), now, []observability.ProviderState{{
		ProviderID:    onChainID(t, "101"),
		Status:        observability.StatusAvailable,
		ReasonCodes:   []observability.ReasonCode{},
		ServiceURL:    &serviceURL,
		LastCheckedAt: now,
		Evidence:      map[string]any{},
	}}); err != nil {
		t.Fatalf("ReplaceProviderStates: %v", err)
	}
	return observability.NewService(observability.ServiceOptions{
		Store: repos.Observability,
		Now:   func() time.Time { return now },
	})
}

func getTaskDiagnostic(t *testing.T, srv *Server, taskID int64) (observability.TaskDiagnostic, int) {
	t.Helper()
	return callTaskDiagnostic(t, srv, http.MethodGet, taskID)
}

func refreshTaskDiagnostic(t *testing.T, srv *Server, taskID int64) (observability.TaskDiagnostic, int) {
	t.Helper()
	return callTaskDiagnostic(t, srv, http.MethodPost, taskID)
}

func refreshTaskDiagnosticRaw(t *testing.T, srv *Server, taskID int64) ([]byte, int) {
	t.Helper()
	return callTaskDiagnosticRaw(t, srv, http.MethodPost, taskID)
}

func callTaskDiagnostic(t *testing.T, srv *Server, method string, taskID int64) (observability.TaskDiagnostic, int) {
	t.Helper()
	raw, status := callTaskDiagnosticRaw(t, srv, method, taskID)
	var body observability.TaskDiagnostic
	if status != http.StatusNotFound {
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
	return body, status
}

func callTaskDiagnosticRaw(t *testing.T, srv *Server, method string, taskID int64) ([]byte, int) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tasks/{id}/diagnostic", srv.handleAPITaskDiagnostic)
	mux.HandleFunc("POST /api/v1/tasks/{id}/diagnostic/refresh", srv.handleAPITaskDiagnosticRefresh)
	path := "/api/v1/tasks/" + strconv.FormatInt(taskID, 10) + "/diagnostic"
	if method == http.MethodPost {
		path += "/refresh"
	}
	req := httptest.NewRequest(method, path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr.Body.Bytes(), rr.Code
}
