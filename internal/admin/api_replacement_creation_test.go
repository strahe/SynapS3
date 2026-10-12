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

func failedReplacementAPISource(t *testing.T, f *replacementAPIFixture) *model.Task {
	t.Helper()
	if _, err := f.srv.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("status = ?", model.StorageDataSetStatusPending).
		Set("data_set_id = NULL").Where("id = ?", f.source.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	ensure, _, err := f.srv.taskService.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
		Type:  model.TaskTypeStorageDataSetEnsure,
		Input: storagepipeline.DataSetInput{DataSetID: f.source.ID},
	}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Contents.BindDataSetEnsureTask(ctx, f.source.ID, row.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
		Set("finished_at = ?", time.Now()).Where("id = ?", ensure.ID).Exec(t.Context()); err != nil {
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

func TestUnsentReplacementConcurrentConfirmations(t *testing.T) {
	for _, sameRequest := range []bool{true, false} {
		t.Run(strconv.FormatBool(sameRequest), func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
			failedReplacementAPISource(t, f)
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

func TestReadyReplacementTargetKeepsSourceReserved(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, phase := range []storagereplacement.Status{storagereplacement.StatusPreparingTarget, storagereplacement.StatusWaiting, storagereplacement.StatusFailed} {
			t.Run(fmt.Sprintf("local=%v/%s", local, phase), func(t *testing.T) {
				f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
				if local {
					failedReplacementAPISource(t, f)
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
				if err := f.srv.db.NewRaw("UPDATE tasks SET status = ?, lease_until = ?, claim_generation = claim_generation + 1 WHERE id = ? RETURNING claim_generation", model.TaskStatusRunning, now.Add(time.Minute), ensureID).Scan(t.Context(), &generation); err != nil {
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
				wantReplaceable := !local && phase == storagereplacement.StatusFailed
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
					restoreRejectCleanupCoordinator := testutil.FailWrites(t, f.srv.db, "tasks", "INSERT",
						fmt.Sprintf("NEW.type = 'provider_replacement_coordinate' AND NEW.subject_key = '%d'", row.ID), "injected cleanup enqueue failure")
					failed := f.start(t, confirmation)
					before, err := f.srv.repos.Replacements.GetByID(t.Context(), row.ID)
					if failed.Code != http.StatusInternalServerError || err != nil || before.Status != phase || before.TaskGeneration != row.TaskGeneration || before.TaskID == nil || *before.TaskID != f.replacementTaskID(t, row.ID) {
						t.Fatalf("partial supersession: %d replacement=%#v err=%v", failed.Code, before, err)
					}
					count, err := f.srv.db.NewSelect().Model((*model.StorageDataSet)(nil)).Where("bucket_id = ?", f.bucket.ID).Count(t.Context())
					if err != nil || count != 2 {
						t.Fatalf("replacement rollback left another target: count=%d err=%v", count, err)
					}
					restoreRejectCleanupCoordinator()
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
					if err != nil || cleanup.Status != model.TaskStatusPending || cleanup.SubjectKey != strconv.FormatInt(row.ID, 10) || cleanup.Generation == nil || *cleanup.Generation != predecessor.TaskGeneration {
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

func TestFailedInitializationReplacementDoesNotCheckProviderAbsence(t *testing.T) {
	for _, evidence := range []string{"none", "client", "transaction", "refusal"} {
		t.Run(evidence, func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
			ensure := failedReplacementAPISource(t, f)
			if evidence == "client" || evidence == "transaction" {
				if err := f.srv.repos.Contents.RecordDataSetClientID(t.Context(), f.source.ID, ensure.ID, onChainIDValue("909")); err != nil {
					t.Fatal(err)
				}
			}
			if evidence == "transaction" {
				if err := f.srv.repos.Contents.MarkDataSetCreating(t.Context(), repository.MarkDataSetCreatingInput{ID: f.source.ID, TransactionID: "0xcreate", StatusURL: "https://provider.example/status", ClientDataSetID: new(onChainIDValue("909"))}); err != nil {
					t.Fatal(err)
				}
			}
			if evidence == "refusal" {
				rejectedReplacementAPISource(t, f, ensure)
			}
			f.srv.WithObjectStorage(&testutil.MockStorageClient{OpenProviderTargetFunc: func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
				t.Fatal("replacement queried stopped provider")
				return nil, nil
			}})
			response := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if response.Code != http.StatusCreated {
				t.Fatalf("replacement: %d %s", response.Code, response.Body.String())
			}
			stored, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), f.source.ID)
			if err != nil || stored.EnsureTaskID != nil || !stored.IsCurrent {
				t.Fatalf("source owner: %#v %v", stored, err)
			}
			if evidence != "none" && stored.ClientDataSetID == nil {
				t.Fatal("creation identity was discarded")
			}
		})
	}
}

func TestReplacementTargetRetryRestoresBothOwners(t *testing.T) {
	f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
	failedReplacementAPISource(t, f)
	created := f.start(t, `{"mode":"manual","provider_id":"202"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("first replacement: %d %s", created.Code, created.Body.String())
	}
	row, err := f.srv.repos.Replacements.GetByID(t.Context(), decodeReplacement(t, created).ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
	if err != nil {
		t.Fatal(err)
	}
	childID := *target.EnsureTaskID
	parentID := *row.TaskID
	if _, err := f.srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Where("id = ?", childID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.srv.repos.Replacements.SourceEligibility(t.Context(), f.source.ID); err != nil {
		t.Fatalf("failed child waited for parent: %v", err)
	}
	child, err := f.srv.repos.Tasks.GetByID(t.Context(), childID)
	if err != nil {
		t.Fatal(err)
	}
	if item := f.srv.taskListItem(t.Context(), child); item.Retryable {
		t.Fatal("running parent exposed retry")
	}
	if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "setup stopped"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Where("id = ?", parentID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if item := f.srv.taskListItem(t.Context(), child); !item.Retryable || item.RetryTaskID == nil || *item.RetryTaskID != parentID {
		t.Fatalf("retry projection: %#v", item)
	}
	var results [2]*model.Task
	var errs [2]error
	var group sync.WaitGroup
	for i, id := range []int64{childID, parentID} {
		group.Add(1)
		go func() { defer group.Done(); results[i], errs[i] = f.srv.retryTaskRequest(t.Context(), id) }()
	}
	group.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	childNext, err := f.srv.repos.Tasks.GetDirectSuccessor(t.Context(), childID)
	if err != nil {
		t.Fatal(err)
	}
	parentNext, err := f.srv.repos.Tasks.GetDirectSuccessor(t.Context(), parentID)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].ID != childNext.ID || results[1].ID != parentNext.ID || childNext.RetryCount != 0 || parentNext.RetryCount != 0 {
		t.Fatalf("successors: %#v %#v", results, errs)
	}
	stored, err := f.srv.repos.Replacements.GetByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err = f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
	if err != nil {
		t.Fatal(err)
	}
	if *stored.TaskID != parentNext.ID || *target.EnsureTaskID != childNext.ID || stored.Status != storagereplacement.StatusPreparingTarget {
		t.Fatalf("partial retry: %#v %#v", stored, target)
	}
	if _, _, err := f.srv.repos.Replacements.SourceEligibility(t.Context(), f.source.ID); !errors.Is(err, storagereplacement.ErrTargetCreating) {
		t.Fatalf("retry allowed replacement: %v", err)
	}
	if replay, err := f.srv.retryTaskRequest(t.Context(), childID); err != nil || replay.ID != childNext.ID {
		t.Fatalf("child replay: %#v %v", replay, err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(childID, 10)+"/retry", nil)
	response := httptest.NewRecorder()
	f.mux.ServeHTTP(response, request)
	var replay struct {
		TaskID int64 `json:"task_id"`
	}
	decodeJSON(t, response, &replay)
	if response.Code != http.StatusAccepted || replay.TaskID != childNext.ID {
		t.Fatalf("child HTTP replay: %d %#v", response.Code, replay)
	}
}

func TestInitializationRetryAndReplacementAreAtomic(t *testing.T) {
	for _, scenario := range []string{"retry rollback", "replace first", "retry first", "concurrent"} {
		t.Run(scenario, func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
			failedReplacementAPISource(t, f)
			created := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if created.Code != http.StatusCreated {
				t.Fatalf("first replacement: %d %s", created.Code, created.Body.String())
			}
			row, err := f.srv.repos.Replacements.GetByID(t.Context(), decodeReplacement(t, created).ID)
			if err != nil {
				t.Fatal(err)
			}
			target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			childID, parentID := *target.EnsureTaskID, *row.TaskID
			if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "setup stopped"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
				Set("finished_at = ?", time.Now()).Where("id IN (?, ?)", parentID, childID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if scenario == "retry rollback" {
				restore := testutil.FailWrites(t, f.srv.db, "storage_replacements", "UPDATE",
					"NEW.task_id IS DISTINCT FROM OLD.task_id", "coordinator retry interrupted")
				defer restore()
				if _, err := f.srv.retryTaskRequest(t.Context(), childID); err == nil {
					t.Fatal("retry survived failed coordinator handoff")
				}
				for _, id := range []int64{parentID, childID} {
					if next, err := f.srv.repos.Tasks.GetDirectSuccessor(t.Context(), id); err != nil || next != nil {
						t.Fatalf("partial successor: %#v %v", next, err)
					}
					if old, err := f.srv.repos.Tasks.GetByID(t.Context(), id); err != nil || old.SupersededAt != nil || old.Status != model.TaskStatusFailed {
						t.Fatalf("partial task handoff: %#v %v", old, err)
					}
				}
				stored, err := f.srv.repos.Replacements.GetByID(t.Context(), row.ID)
				if err != nil || stored.TaskID == nil || *stored.TaskID != parentID || stored.Status != storagereplacement.StatusFailed {
					t.Fatalf("partial coordinator handoff: %#v %v", stored, err)
				}
				target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
				if err != nil || target.EnsureTaskID == nil || *target.EnsureTaskID != childID {
					t.Fatalf("partial child handoff: %#v %v", target, err)
				}
				return
			}
			var retried *model.Task
			var retryErr error
			var replaced *httptest.ResponseRecorder
			retry := func() { retried, retryErr = f.srv.retryTaskRequest(t.Context(), childID) }
			replace := func() { replaced = f.start(t, `{"mode":"manual","provider_id":"303"}`) }
			switch scenario {
			case "replace first":
				replace()
				retry()
			case "retry first":
				retry()
				replace()
			case "concurrent":
				start := make(chan struct{})
				var group sync.WaitGroup
				for _, action := range []func(){retry, replace} {
					group.Add(1)
					go func() { defer group.Done(); <-start; action() }()
				}
				close(start)
				group.Wait()
			}
			if retryErr == nil {
				if retried == nil || replaced.Code != http.StatusConflict {
					t.Fatalf("retry and replacement both authorized: %#v %d %s", retried, replaced.Code, replaced.Body.String())
				}
			} else if replaced.Code != http.StatusCreated || !errors.Is(retryErr, repository.ErrConflict) {
				t.Fatalf("neither operation authorized: retry=%v replace=%d %s", retryErr, replaced.Code, replaced.Body.String())
			}
		})
	}
}

func TestInitializationCoordinatorRetryReusesActiveChild(t *testing.T) {
	for _, status := range []model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning} {
		t.Run(string(status), func(t *testing.T) {
			f := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
			failedReplacementAPISource(t, f)
			created := f.start(t, `{"mode":"manual","provider_id":"202"}`)
			if created.Code != http.StatusCreated {
				t.Fatalf("first replacement: %d %s", created.Code, created.Body.String())
			}
			row, err := f.srv.repos.Replacements.GetByID(t.Context(), decodeReplacement(t, created).ID)
			if err != nil {
				t.Fatal(err)
			}
			target, err := f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			childID, parentID := *target.EnsureTaskID, *row.TaskID
			if status == model.TaskStatusRunning {
				if _, err := f.srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", status).
					Set("claim_generation = 1").Set("lease_until = ?", time.Now().Add(time.Minute)).Where("id = ?", childID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.srv.repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "coordinator stopped"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
				Set("finished_at = ?", time.Now()).Where("id = ?", parentID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			finishChild := func() error { return nil }
			if status == model.TaskStatusRunning {
				childTx, err := f.srv.db.BeginTx(t.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = childTx.Rollback() }()
				childRepos := repository.NewRepositories(childTx)
				if _, err := childRepos.Tasks.GetForUpdate(t.Context(), childID); err != nil {
					t.Fatal(err)
				}
				finishChild = func() error {
					if err := childRepos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: target.ID, DataSetID: onChainIDValue("2002")}); err != nil {
						return err
					}
					return childTx.Commit()
				}
			}
			retryCtx, retryCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer retryCancel()
			parent, err := f.srv.retryTaskRequest(retryCtx, parentID)
			if err != nil || parent == nil || parent.ID == parentID {
				t.Fatalf("coordinator retry: %#v %v", parent, err)
			}
			if next, err := f.srv.repos.Tasks.GetDirectSuccessor(t.Context(), childID); err != nil || next != nil {
				t.Fatalf("active child got another round: %#v %v", next, err)
			}
			target, err = f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
			if err != nil || target.EnsureTaskID == nil || *target.EnsureTaskID != childID {
				t.Fatalf("active child owner changed: %#v %v", target, err)
			}
			child, err := f.srv.repos.Tasks.GetByID(t.Context(), childID)
			if err != nil || child.Status != status || child.SupersededAt != nil {
				t.Fatalf("active child interrupted: %#v %v", child, err)
			}
			if err := finishChild(); err != nil {
				t.Fatalf("active child readiness after coordinator retry: %v", err)
			}
			if status == model.TaskStatusRunning {
				target, err = f.srv.repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
				if err != nil || target.Status != model.StorageDataSetStatusReady || target.EnsureTaskID == nil || *target.EnsureTaskID != childID {
					t.Fatalf("active child result lost after coordinator retry: %#v %v", target, err)
				}
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
