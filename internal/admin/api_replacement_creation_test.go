package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func pendingReplacementAPISource(t *testing.T, f *replacementAPIFixture) *model.Task {
	t.Helper()
	if _, err := f.srv.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("status = ?", model.StorageDataSetStatusPending).
		Set("data_set_id = NULL").Where("id = ?", f.source.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	ensure, _, err := f.srv.taskService.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(f.source.ID),
		Input: storagepipeline.DataSetInput{DataSetID: f.source.ID},
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, f.source.ID, row.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return ensure
}

func rejectedReplacementAPISource(t *testing.T, f *replacementAPIFixture, ensure *model.Task) {
	t.Helper()
	clientID := onChainIDValue("909")
	if err := f.srv.repos.Contents.RecordDataSetClientID(t.Context(), f.source.ID, ensure.ID, clientID); err != nil {
		t.Fatal(err)
	}
	identity := testutil.DefaultContextIdentity
	evidence := model.DataSetCreationRejection{
		Version: 1, StatusCode: 403, RejectedAt: time.Now().UTC().Add(-time.Minute), AbsenceCheckedAt: time.Now().UTC(),
		ClientDataSetID: clientID, Payer: identity.Payer, ChainID: uint64(identity.ChainID), RecordKeeper: identity.RecordKeeper,
	}
	if err := f.srv.repos.Contents.RecordDataSetCreationRejection(t.Context(), f.source.ID, f.source.Generation, ensure.ID, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ?, failure_reason = ? WHERE id = ?", model.TaskStatusFailed, time.Now(), "dataset_provider_rejected", ensure.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementCreationPreflightAndPresentation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		refusal         bool
		running         bool
		unknown         bool
		found           bool
		lookupError     bool
		identityChanged bool
		wantCode        string
	}{
		{name: "unsent"},
		{name: "refused", refusal: true},
		{name: "service found", refusal: true, found: true, wantCode: storagereplacement.CodeSourceOutcomeUnknown},
		{name: "lookup unavailable", refusal: true, lookupError: true, wantCode: storagereplacement.CodeSourceOutcomeUnknown},
		{name: "identity changed", refusal: true, identityChanged: true, wantCode: storagereplacement.CodeSourceOutcomeUnknown},
		{name: "unknown outcome", unknown: true, wantCode: storagereplacement.CodeSourceOutcomeUnknown},
		{name: "running", running: true, wantCode: storagereplacement.CodeSourceRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
			ensure := pendingReplacementAPISource(t, f)
			if tc.refusal {
				rejectedReplacementAPISource(t, f, ensure)
			}
			if tc.unknown {
				if err := f.srv.repos.Contents.RecordDataSetClientID(t.Context(), f.source.ID, ensure.ID, onChainIDValue("909")); err != nil {
					t.Fatal(err)
				}
			}
			if tc.running {
				now := time.Now()
				if _, err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, claimed_at = ?, lease_until = ?, claim_generation = 1, started_at = ? WHERE id = ?", model.TaskStatusRunning, now, now.Add(time.Minute), now, ensure.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			lookups := 0
			f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				identity := testutil.DefaultContextIdentity
				if tc.identityChanged {
					identity.ChainID++
				}
				return &testutil.MockStorageTarget{ProviderIDValue: f.source.ProviderID.SDK(), ContextIdentityValue: identity, FindDataSetByClientIDFunc: func(_ context.Context, clientID sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					lookups++
					if clientID.String() != "909" {
						t.Fatalf("looked up another request: %s", clientID.String())
					}
					if tc.lookupError {
						return storage.DataSetRef{}, false, errors.New("chain failed")
					}
					if tc.found {
						ref, err := storage.NewDataSetRef(f.source.ProviderID.SDK(), sdktypes.NewBigInt(1001), clientID)
						return ref, err == nil, err
					}
					return storage.DataSetRef{}, false, nil
				}}, nil
			}})
			get := httptest.NewRecorder()
			f.mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/buckets/"+f.bucket.Name, nil))
			var detail bucketDetailResponse
			if get.Code != http.StatusOK {
				t.Fatalf("detail: %d %s", get.Code, get.Body.String())
			}
			if err := json.Unmarshal(get.Body.Bytes(), &detail); err != nil {
				t.Fatal(err)
			}
			if len(detail.DataSets) != 1 || detail.DataSets[0].Replaceable != (!tc.unknown && !tc.running) {
				t.Fatalf("presentation = %#v", detail.DataSets)
			}
			if tc.refusal && detail.DataSets[0].SetupError == "" {
				t.Fatal("provider refusal was not shown")
			}
			rec := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if tc.wantCode != "" {
				response := decodeAPIError(t, rec)
				if rec.Code != http.StatusConflict || response["code"] != tc.wantCode {
					t.Fatalf("blocked: %d %s", rec.Code, rec.Body.String())
				}
				if tc.lookupError && !strings.Contains(response["error"], "network is available") {
					t.Fatalf("lookup failure lacks a concrete reason: %s", rec.Body.String())
				}
				if tc.identityChanged && (lookups != 0 || !strings.Contains(response["error"], "original wallet")) {
					t.Fatalf("identity mismatch: lookups=%d body=%s", lookups, rec.Body.String())
				}
				if tc.found {
					source, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), f.source.ID)
					if err != nil || source.Status != model.StorageDataSetStatusReady || source.DataSetID == nil || len(source.CreationRejection) != 0 {
						t.Fatalf("found service not bound: %#v %v", source, err)
					}
				}
				return
			}
			if rec.Code != http.StatusCreated {
				t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
			}
			row := decodeReplacement(t, rec)
			source, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), f.source.ID)
			if err != nil {
				t.Fatal(err)
			}
			target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.Target.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !source.IsCurrent || source.Status == model.StorageDataSetStatusRetired || source.EnsureTaskID != nil || target.IsCurrent || target.EnsureTaskID == nil {
				t.Fatalf("preparing replacement: source=%#v target=%#v", source, target)
			}
			if tc.refusal && lookups != 1 {
				t.Fatalf("preflight lookups=%d, want 1", lookups)
			}
		})
	}
}

func TestUnsentReplacementConcurrentConfirmations(t *testing.T) {
	for _, sameRequest := range []bool{true, false} {
		t.Run(strconv.FormatBool(sameRequest), func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
			pendingReplacementAPISource(t, f)
			var wg sync.WaitGroup
			results := make(chan *httptest.ResponseRecorder, 8)
			for i := range 8 {
				wg.Go(func() {
					id := "same"
					if !sameRequest {
						id = "request-" + strconv.Itoa(i)
					}
					body := `{"mode":"manual","provider_id":"202","client_request_id":"` + id + `","price_list_fingerprint":"` + f.priceFingerprint + `"}`
					req := httptest.NewRequest(http.MethodPost, "/api/v1/buckets/"+f.bucket.Name+"/data-sets/"+strconv.FormatInt(f.source.ID, 10)+"/replacement", strings.NewReader(body))
					rec := httptest.NewRecorder()
					f.mux.ServeHTTP(rec, req)
					results <- rec
				})
			}
			wg.Wait()
			close(results)
			created := 0
			for result := range results {
				if result.Code == http.StatusCreated {
					created++
				} else if sameRequest && result.Code != http.StatusOK || !sameRequest && result.Code != http.StatusConflict {
					t.Fatalf("concurrent response: %d %s", result.Code, result.Body.String())
				}
			}
			if created != 1 {
				t.Fatalf("created=%d, want 1", created)
			}
		})
	}
}

func TestRefusedReplacementTargetIsRecheckedBeforeChangingProvider(t *testing.T) {
	for _, outcome := range []string{"absent", "found", "lookup failed", "changed fence"} {
		t.Run(outcome, func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
			first := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if first.Code != http.StatusCreated {
				t.Fatalf("first: %d %s", first.Code, first.Body.String())
			}
			row := decodeReplacement(t, first)
			target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.Target.ID)
			if err != nil {
				t.Fatal(err)
			}
			ensure, err := f.srv.repos.Tasks.GetByID(t.Context(), *target.EnsureTaskID)
			if err != nil {
				t.Fatal(err)
			}
			copyFixture := *f
			copyFixture.source = target
			rejectedReplacementAPISource(t, &copyFixture, ensure)
			reason := storagereplacement.FailureReasonTargetRejected
			if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, &reason, "refused"); err != nil {
				t.Fatal(err)
			}
			detail := httptest.NewRecorder()
			f.mux.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/api/v1/buckets/"+f.bucket.Name, nil))
			var summary bucketDetailResponse
			if detail.Code != http.StatusOK {
				t.Fatalf("bucket detail: %d %s", detail.Code, detail.Body.String())
			}
			if err := json.Unmarshal(detail.Body.Bytes(), &summary); err != nil {
				t.Fatal(err)
			}
			foundSource := false
			for _, dataSet := range summary.DataSets {
				if dataSet.ID == f.source.ID {
					foundSource = true
					if !dataSet.ReplacementHasLateServiceRisk {
						t.Fatal("ready source did not disclose its refused target's late service risk")
					}
				}
			}
			if !foundSource {
				t.Fatal("source summary missing")
			}
			lookups := 0
			f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return &testutil.MockStorageTarget{ProviderIDValue: provider, FindDataSetByClientIDFunc: func(_ context.Context, id sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					lookups++
					if provider.String() != "202" || id.String() != "909" {
						t.Fatalf("wrong original identity: %s/%s", provider, id)
					}
					switch outcome {
					case "lookup failed":
						return storage.DataSetRef{}, false, errors.New("unavailable")
					case "found":
						ref, err := storage.NewDataSetRef(provider, sdktypes.NewBigInt(2002), id)
						return ref, err == nil, err
					case "changed fence":
						if err := f.srv.repos.Contents.CompleteDataSetEnsureTask(t.Context(), target.ID, ensure.ID); err != nil {
							t.Fatal(err)
						}
					}
					return storage.DataSetRef{}, false, nil
				}}, nil
			}})
			second := f.start(t, `{"mode":"manual","provider_id":"303"}`)
			if lookups != 1 {
				t.Fatalf("original target queried %d times", lookups)
			}
			stored, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "absent" {
				if second.Code != http.StatusCreated || stored.Status != model.StorageDataSetStatusRetired || stored.DataSetID != nil {
					t.Fatalf("absence: %d %s target=%#v", second.Code, second.Body.String(), stored)
				}
				return
			}
			if second.Code != http.StatusConflict || stored.Status == model.StorageDataSetStatusRetired {
				t.Fatalf("unsafe abandonment: %d %s target=%#v", second.Code, second.Body.String(), stored)
			}
			if outcome == "found" {
				if _, err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", model.TaskStatusFailed, time.Now(), f.replacementTaskID(t, row.ID)).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				if stored.Status != model.StorageDataSetStatusReady || stored.DataSetID == nil || len(stored.CreationRejection) != 0 || stored.EnsureTaskID != nil {
					t.Fatalf("service not recovered: %#v", stored)
				}
				retry := httptest.NewRecorder()
				f.mux.ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(f.replacementTaskID(t, row.ID), 10)+"/retry", nil))
				if retry.Code != http.StatusAccepted {
					t.Fatalf("recovered target could not resume: %d %s", retry.Code, retry.Body.String())
				}
			}
		})
	}
}

func TestStoppedReplacementSourceCanBindAnObservedService(t *testing.T) {
	f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
	ensure := pendingReplacementAPISource(t, f)
	rejectedReplacementAPISource(t, f, ensure)
	copyRow := waitingReplacementAPICopy(t, f)
	f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
		return &testutil.MockStorageTarget{ProviderIDValue: provider, FindDataSetByClientIDFunc: func(context.Context, sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			return storage.DataSetRef{}, false, nil
		}}, nil
	}})
	first := f.start(t, `{"mode":"manual","provider_id":"202"}`)
	if first.Code != http.StatusCreated {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}
	row := decodeReplacement(t, first)
	if _, err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ?, failure_reason = ? WHERE id = (SELECT ensure_task_id FROM storage_data_sets WHERE id = ?)", model.TaskStatusFailed, time.Now(), "provider_unavailable", row.Target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
		return &testutil.MockStorageTarget{ProviderIDValue: provider, FindDataSetByClientIDFunc: func(_ context.Context, id sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			ref, err := storage.NewDataSetRef(provider, sdktypes.NewBigInt(1001), id)
			return ref, err == nil, err
		}}, nil
	}})
	if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "replacement stopped"); err != nil {
		t.Fatal(err)
	}
	second := f.start(t, `{"mode":"manual","provider_id":"303"}`)
	if second.Code != http.StatusConflict {
		t.Fatalf("expected new confirmation: %d %s", second.Code, second.Body.String())
	}
	source, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), f.source.ID)
	if err != nil || source.EnsureTaskID != nil || source.DataSetID == nil || source.Status != model.StorageDataSetStatusReady || !source.IsCurrent || len(source.CreationRejection) != 0 {
		t.Fatalf("stopped source could not recover: %#v %v", source, err)
	}
	recovered, err := f.srv.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || recovered.WorkTaskID() == nil {
		t.Fatalf("observed service left its replica without work: %#v %v", recovered, err)
	}
	if _, err := f.srv.repos.Contents.AuthorizeDataSetEnsureTask(t.Context(), source.ID, ensure.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("observation restored old creation permission: %v", err)
	}
}

func waitingReplacementAPICopy(t *testing.T, f *replacementAPIFixture) *model.StorageCopy {
	t.Helper()
	content, err := f.srv.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{BucketID: f.bucket.ID, ContentSize: 11, Checksum: testutil.StorageChecksum(t.Name()), RequestedCopies: 1})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: f.bucket.ID, Key: "waiting.bin", ContentID: &content.ID, Size: 11, ETag: "waiting", ContentType: "application/octet-stream"}
	if _, err := f.srv.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{StorageDataSetID: f.source.ID, CopyIndex: 0, ProviderID: f.source.ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress}}); err != nil {
		t.Fatal(err)
	}
	copies, err := f.srv.repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("waiting replica: %#v %v", copies, err)
	}
	return &copies[0]
}

func TestObservedReplacementServiceCompletionIsAtomic(t *testing.T) {
	for _, failure := range []string{"enqueue", "dependency"} {
		t.Run(failure, func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
			ensure := pendingReplacementAPISource(t, f)
			rejectedReplacementAPISource(t, f, ensure)
			copyRow := waitingReplacementAPICopy(t, f)
			before, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), f.source.ID)
			if err != nil {
				t.Fatal(err)
			}
			f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				return &testutil.MockStorageTarget{ProviderIDValue: provider, FindDataSetByClientIDFunc: func(_ context.Context, id sdktypes.BigInt) (storage.DataSetRef, bool, error) {
					ref, err := storage.NewDataSetRef(provider, sdktypes.NewBigInt(1001), id)
					return ref, err == nil, err
				}}, nil
			}})
			messages := f.srv.replacementMessages
			if failure == "dependency" {
				f.srv.replacementMessages = nil
			} else if _, err := f.srv.db.ExecContext(t.Context(), `CREATE TRIGGER reject_recovered_copy_task BEFORE INSERT ON tasks WHEN NEW.type = 'storage_transfer_plan' BEGIN SELECT RAISE(ABORT, 'injected enqueue failure'); END`); err != nil {
				t.Fatal(err)
			}
			failed := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if failed.Code != http.StatusInternalServerError {
				t.Fatalf("completion failure: %d %s", failed.Code, failed.Body.String())
			}
			after, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), f.source.ID)
			if err != nil || after.Status != before.Status || after.DataSetID != nil || after.EnsureTaskID == nil || *after.EnsureTaskID != ensure.ID || string(after.CreationRejection) != string(before.CreationRejection) {
				t.Fatalf("partial observation committed: %#v %v", after, err)
			}
			copyRow, err = f.srv.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || copyRow.ActiveTaskID != nil || copyRow.WorkGeneration != 0 {
				t.Fatalf("partial copy work committed: %#v %v", copyRow, err)
			}
			if failure == "enqueue" {
				if _, err := f.srv.db.ExecContext(t.Context(), "DROP TRIGGER reject_recovered_copy_task"); err != nil {
					t.Fatal(err)
				}
			}
			f.srv.replacementMessages = messages
			recovered := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if recovered.Code != http.StatusConflict {
				t.Fatalf("completion retry: %d %s", recovered.Code, recovered.Body.String())
			}
			copyRow, err = f.srv.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || copyRow.ActiveTaskID == nil || copyRow.WorkGeneration != 1 {
				t.Fatalf("completion retry did not schedule one generation: %#v %v", copyRow, err)
			}
		})
	}
}

func TestReadyIncomingReplacementTargetRemainsUnavailable(t *testing.T) {
	f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
	created := f.start(t, `{"mode":"manual","provider_id":"202"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("start: %d %s", created.Code, created.Body.String())
	}
	row, err := f.srv.repos.Replacements.GetByID(t.Context(), decodeReplacement(t, created).ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.srv.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: row.TargetDataSetID, DataSetID: onChainIDValue("2002")}); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.repos.Replacements.Activate(t.Context(), row.ID, row.TaskGeneration, f.replacementTaskID(t, row.ID)); err != nil {
		t.Fatal(err)
	}
	for _, stopped := range []bool{false, true} {
		if stopped {
			if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "migration stopped"); err != nil {
				t.Fatal(err)
			}
		}
		get := httptest.NewRecorder()
		f.mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/buckets/"+f.bucket.Name, nil))
		var detail bucketDetailResponse
		if get.Code != http.StatusOK || json.Unmarshal(get.Body.Bytes(), &detail) != nil {
			t.Fatalf("detail: %d %s", get.Code, get.Body.String())
		}
		found := false
		for _, dataSet := range detail.DataSets {
			if dataSet.ID == row.TargetDataSetID {
				found = true
				if dataSet.Replaceable || dataSet.ReplacementBlockedReason != storagereplacement.CodeActive {
					t.Fatalf("unfinished target offered for replacement: %#v", dataSet)
				}
			}
		}
		if !found {
			t.Fatal("unfinished replacement target missing from bucket detail")
		}
		f.source, err = f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
		if err != nil {
			t.Fatal(err)
		}
		blocked := f.start(t, `{"mode":"manual","provider_id":"303"}`)
		if blocked.Code != http.StatusConflict || decodeAPIError(t, blocked)["code"] != storagereplacement.CodeActive {
			t.Fatalf("unfinished target reauthorized: %d %s", blocked.Code, blocked.Body.String())
		}
	}
}

func TestConcurrentObservedReplacementServiceSchedulesOneCopyGeneration(t *testing.T) {
	f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
	ensure := pendingReplacementAPISource(t, f)
	rejectedReplacementAPISource(t, f, ensure)
	copyRow := waitingReplacementAPICopy(t, f)
	if _, err := f.srv.db.NewUpdate().Model((*model.Bucket)(nil)).Set("status = ?", model.BucketStatusProvisioning).Where("id = ?", f.bucket.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	provision, _, err := f.srv.taskService.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeBucketProvision, IdempotencyKey: bucketlifecycle.ProvisionKey(f.bucket.ID, f.bucket.DefaultCopies),
		Input: bucketlifecycle.ProvisionInput{BucketID: f.bucket.ID}, AvailableAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	check, err := f.srv.repos.Replacements.Preflight(t.Context(), f.source.ID)
	if err != nil {
		t.Fatal(err)
	}
	lookedUp, release := make(chan struct{}, 2), make(chan struct{})
	f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(_ context.Context, provider sdktypes.BigInt, _ storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
		return &testutil.MockStorageTarget{ProviderIDValue: provider, FindDataSetByClientIDFunc: func(ctx context.Context, id sdktypes.BigInt) (storage.DataSetRef, bool, error) {
			lookedUp <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return storage.DataSetRef{}, false, ctx.Err()
			}
			ref, err := storage.NewDataSetRef(provider, sdktypes.NewBigInt(1001), id)
			return ref, err == nil, err
		}}, nil
	}})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := f.srv.checkReplacementRefusal(t.Context(), f.bucket, check, &check.Source)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-lookedUp:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("observations did not reach the remote check")
		}
	}
	close(release)
	for range 2 {
		if err := <-results; !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
			t.Fatalf("concurrent observation result: %v", err)
		}
	}
	copyRow, err = f.srv.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil || copyRow.ActiveTaskID == nil || copyRow.WorkGeneration != 1 {
		t.Fatalf("observation scheduled duplicate work: %#v %v", copyRow, err)
	}
	bucket, err := f.srv.repos.Buckets.GetByID(t.Context(), f.bucket.ID)
	if err != nil || bucket.Status != model.BucketStatusReady {
		t.Fatalf("observation did not advance bucket readiness: %#v %v", bucket, err)
	}
	provision, err = f.srv.repos.Tasks.GetByID(t.Context(), provision.ID)
	if err != nil || provision.AvailableAt.After(time.Now()) {
		t.Fatalf("observation did not wake provisioning: %#v %v", provision, err)
	}
	if err := f.srv.repos.Replacements.BindObservedService(t.Context(), repository.BindObservedReplacementServiceInput{
		Preflight: *check, StorageDataSetID: check.Source.ID, DataSetID: onChainIDValue("1001"), ClientDataSetID: *check.Source.ClientDataSetID,
	}); !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
		t.Fatalf("replayed stale observation accepted: %v", err)
	}
	if err := f.srv.repos.Contents.RecordDataSetClientID(t.Context(), f.source.ID, ensure.ID, onChainIDValue("999")); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("old worker regained creation permission: %v", err)
	}
}

func TestReadyReplacementTargetKeepsSourceReserved(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, phase := range []storagereplacement.Status{storagereplacement.StatusPreparingTarget, storagereplacement.StatusWaiting, storagereplacement.StatusFailed} {
			t.Run(fmt.Sprintf("local=%v/%s", local, phase), func(t *testing.T) {
				f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
				if local {
					pendingReplacementAPISource(t, f)
				}
				created := f.start(t, `{"mode":"manual","provider_id":"202"}`)
				if created.Code != http.StatusCreated {
					t.Fatalf("start: %d %s", created.Code, created.Body.String())
				}
				row, err := f.srv.repos.Replacements.GetByID(t.Context(), decodeReplacement(t, created).ID)
				if err != nil {
					t.Fatal(err)
				}
				target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
				if err != nil || target.EnsureTaskID == nil {
					t.Fatalf("target: %#v %v", target, err)
				}
				ensureID := *target.EnsureTaskID
				if err := f.srv.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: target.ID, DataSetID: onChainIDValue("2002")}); err != nil {
					t.Fatal(err)
				}
				if err := f.srv.repos.Contents.CompleteDataSetEnsureTask(t.Context(), target.ID, ensureID); err != nil {
					t.Fatal(err)
				}
				now := time.Now()
				var generation int64
				if err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, claimed_at = ?, lease_until = ?, claim_generation = claim_generation + 1 WHERE id = ? RETURNING claim_generation", model.TaskStatusRunning, now, now.Add(time.Minute), ensureID).Scan(t.Context(), &generation); err != nil {
					t.Fatal(err)
				}
				if err := f.srv.repos.Tasks.Settle(t.Context(), ensureID, generation, repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute}); err != nil {
					t.Fatal(err)
				}
				switch phase {
				case storagereplacement.StatusWaiting:
					if err := f.srv.repos.Replacements.MarkWaiting(t.Context(), row.ID, storagereplacement.WaitReasonSourceWrites); err != nil {
						t.Fatal(err)
					}
				case storagereplacement.StatusFailed:
					if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "replacement stopped"); err != nil {
						t.Fatal(err)
					}
					if _, err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?", model.TaskStatusFailed, time.Now(), f.replacementTaskID(t, row.ID)).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				get := httptest.NewRecorder()
				f.mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/buckets/"+f.bucket.Name, nil))
				var detail bucketDetailResponse
				if get.Code != http.StatusOK || json.Unmarshal(get.Body.Bytes(), &detail) != nil {
					t.Fatalf("detail: %d %s", get.Code, get.Body.String())
				}
				wantReplaceable := phase == storagereplacement.StatusFailed
				found := false
				for _, dataSet := range detail.DataSets {
					if dataSet.ID == f.source.ID {
						found = true
						if dataSet.Replaceable != wantReplaceable || (!wantReplaceable && dataSet.ReplacementBlockedReason != storagereplacement.CodeActive) {
							t.Fatalf("source admission: %#v", dataSet)
						}
					}
				}
				if !found {
					t.Fatal("replacement source missing")
				}
				confirmation := `{"mode":"manual","provider_id":"303","client_request_id":"successor"}`
				if wantReplaceable {
					// A cleanup enqueue failure must also roll back the new target and
					// the predecessor's supersession, generation, and coordinator fence.
					trigger := fmt.Sprintf("CREATE TRIGGER reject_cleanup_coordinator BEFORE INSERT ON tasks WHEN NEW.type = 'provider_replacement_coordinate' AND NEW.subject_key = '%d' BEGIN SELECT RAISE(ABORT, 'injected cleanup enqueue failure'); END", row.ID)
					if _, err := f.srv.db.ExecContext(t.Context(), trigger); err != nil {
						t.Fatal(err)
					}
					failed := f.start(t, confirmation)
					before, err := f.srv.repos.Replacements.GetByID(t.Context(), row.ID)
					if failed.Code != http.StatusInternalServerError || err != nil || before.Status != phase || before.TaskGeneration != row.TaskGeneration || before.TaskID == nil || *before.TaskID != f.replacementTaskID(t, row.ID) {
						t.Fatalf("partial supersession: %d replacement=%#v err=%v", failed.Code, before, err)
					}
					count, err := f.srv.db.NewSelect().Model((*model.StorageDataSet)(nil)).Where("bucket_id = ?", f.bucket.ID).Count(t.Context())
					if err != nil || count != 2 {
						t.Fatalf("replacement rollback left another target: count=%d err=%v", count, err)
					}
					if _, err := f.srv.db.ExecContext(t.Context(), "DROP TRIGGER reject_cleanup_coordinator"); err != nil {
						t.Fatal(err)
					}
				}
				confirmed := f.start(t, confirmation)
				if wantReplaceable {
					if confirmed.Code != http.StatusCreated {
						t.Fatalf("stopped replacement cannot choose another provider: %d %s", confirmed.Code, confirmed.Body.String())
					}
					predecessor, err := f.srv.repos.Replacements.GetByID(t.Context(), row.ID)
					if err != nil || predecessor.Status != storagereplacement.StatusSuperseded || predecessor.TaskGeneration != row.TaskGeneration+1 || predecessor.TaskID == nil || *predecessor.TaskID == *row.TaskID {
						t.Fatalf("stopped coordinator was not replaced: %#v err=%v", predecessor, err)
					}
					cleanup, err := f.srv.repos.Tasks.GetByID(t.Context(), *predecessor.TaskID)
					if err != nil || cleanup.Status != model.TaskStatusPending || cleanup.IdempotencyKey != storagereplacement.CoordinateTaskKey(row.ID, predecessor.TaskGeneration) {
						t.Fatalf("abandoned service has no cleanup work: %#v err=%v", cleanup, err)
					}
					replay := f.start(t, confirmation)
					if replay.Code != http.StatusOK || decodeReplacement(t, replay).ID != decodeReplacement(t, confirmed).ID {
						t.Fatalf("successor replay: %d %s", replay.Code, replay.Body.String())
					}
				} else if confirmed.Code != http.StatusConflict || decodeAPIError(t, confirmed)["code"] != storagereplacement.CodeActive {
					t.Fatalf("active source reauthorized: %d %s", confirmed.Code, confirmed.Body.String())
				}
			})
		}
	}
}
