package repository_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func localReplacementSource(t *testing.T, db *bun.DB, name string) (*repository.Repositories, *model.StorageDataSet, *model.Task) {
	t.Helper()
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	ensure := localEnsureTask(t, repos, source.ID)
	return repos, source, ensure
}

func localEnsureTask(t *testing.T, repos *repository.Repositories, dataSetID int64) *model.Task {
	t.Helper()
	key := fmt.Sprintf("local-ensure/%d", dataSetID)
	row, _, err := repos.Tasks.Enqueue(t.Context(), &model.Task{Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: key, InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.BindDataSetEnsureTask(t.Context(), dataSetID, row.ID); err != nil {
		t.Fatal(err)
	}
	return row
}

func localCoordinatorTask(t *testing.T, repos *repository.Repositories, row *storagereplacement.Replacement) *model.Task {
	t.Helper()
	key := fmt.Sprintf("local-coordinate/%d/%d", row.ID, row.TaskGeneration)
	coordinator, _, err := repos.Tasks.Enqueue(t.Context(), &model.Task{Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: key, InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Replacements.BindTask(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func localRefusal(t *testing.T, db *bun.DB, repos *repository.Repositories, source *model.StorageDataSet, ensure *model.Task) *model.DataSetCreationRejection {
	t.Helper()
	clientID := onChainID(t, fmt.Sprintf("%d", 700000+source.ID))
	if err := repos.Contents.RecordDataSetClientID(t.Context(), source.ID, ensure.ID, clientID); err != nil {
		t.Fatal(err)
	}
	identity := testutil.DefaultContextIdentity
	evidence := &model.DataSetCreationRejection{
		Version: 1, StatusCode: 403, RejectedAt: time.Now().UTC().Add(-time.Minute), AbsenceCheckedAt: time.Now().UTC(),
		ClientDataSetID: clientID, Payer: identity.Payer, ChainID: uint64(identity.ChainID), RecordKeeper: identity.RecordKeeper,
	}
	if err := repos.Contents.RecordDataSetCreationRejection(t.Context(), source.ID, source.Generation, ensure.ID, *evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewRaw("UPDATE tasks SET status = ?, failure_reason = ?, finished_at = ? WHERE id = ?", model.TaskStatusFailed, "dataset_provider_rejected", time.Now(), ensure.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func TestUncreatedReplacementAdmissionAndFencing(t *testing.T) {
	uncreatedReplacementAdmissionAndFencing(t, testDB(t))
}

func uncreatedReplacementAdmissionAndFencing(t *testing.T, db *bun.DB) {
	for _, tc := range []struct {
		name    string
		refusal bool
		mutate  string
		want    error
	}{
		{name: "unsent"},
		{name: "provider rejected", refusal: true},
		{name: "unknown request", mutate: "request", want: storagereplacement.ErrSourceOutcomeUnknown},
		{name: "running", mutate: "running", want: storagereplacement.ErrSourceRunning},
		{name: "known transaction", mutate: "transaction", want: storagereplacement.ErrSourceOutcomeUnknown},
		{name: "future evidence", refusal: true, mutate: "version", want: storagereplacement.ErrSourceOutcomeUnknown},
		{name: "wrong identity", refusal: true, mutate: "identity", want: storagereplacement.ErrSourceOutcomeUnknown},
		{name: "external effect", mutate: "effect", want: storagereplacement.ErrSourceOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repos, source, ensure := localReplacementSource(t, db, "admission-"+tc.name)
			var evidence *model.DataSetCreationRejection
			if tc.refusal {
				evidence = localRefusal(t, db, repos, source, ensure)
			}
			switch tc.mutate {
			case "request":
				if err := repos.Contents.RecordDataSetClientID(t.Context(), source.ID, ensure.ID, onChainID(t, "909")); err != nil {
					t.Fatal(err)
				}
			case "running":
				now := time.Now()
				if _, err := db.NewRaw("UPDATE tasks SET status = ?, claim_generation = 1, claimed_at = ?, lease_until = ?, started_at = ? WHERE id = ?", model.TaskStatusRunning, now, now.Add(time.Minute), now, ensure.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "transaction":
				if err := repos.Contents.MarkDataSetCreating(t.Context(), repository.MarkDataSetCreatingInput{ID: source.ID, TransactionID: "0xcreate"}); err != nil {
					t.Fatal(err)
				}
			case "version", "identity":
				changed := *evidence
				if tc.mutate == "version" {
					changed.Version++
				} else {
					changed.ClientDataSetID = onChainID(t, "909")
				}
				encoded, err := json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("creation_rejection = ?", json.RawMessage(encoded)).Where("id = ?", source.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "effect":
				copyRow := localWaitingCopy(t, repos, source, "effect")
				if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("ingress_store_attempt = 1").Where("id = ?", copyRow.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			_, local, err := repos.Replacements.SourceEligibility(t.Context(), source.ID)
			if !errors.Is(err, tc.want) || (err == nil && !local) {
				t.Fatalf("eligibility local=%v err=%v, want %v", local, err, tc.want)
			}
			input := repository.AuthorizeReplacementInput{
				BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: onChainID(t, "202"), ClientRequestID: "replace", VerifiedCreationRejection: evidence,
			}
			if tc.refusal && tc.want == nil {
				unchecked := input
				unchecked.VerifiedCreationRejection = nil
				if _, _, err := repos.Replacements.Authorize(t.Context(), unchecked); !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
					t.Fatalf("unchecked refusal accepted: %v", err)
				}
			}
			row, created, err := repos.Replacements.Authorize(t.Context(), input)
			if !errors.Is(err, tc.want) {
				t.Fatalf("authorization err=%v, want %v", err, tc.want)
			}
			if err != nil {
				return
			}
			if !created {
				t.Fatal("replacement was not created")
			}
			ended, err := repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !ended.IsCurrent || ended.Status == model.StorageDataSetStatusRetired || ended.EnsureTaskID != nil {
				t.Fatalf("ended source = %#v", ended)
			}
			if err := repos.Contents.BindDataSetEnsureTask(t.Context(), source.ID, ensure.ID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("stopped source rebound ensure: %v", err)
			}
			if err := repos.Contents.RecordDataSetClientID(t.Context(), source.ID, ensure.ID, onChainID(t, "999")); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("stale create fence accepted: %v", err)
			}
			target, err := repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			if target.IsCurrent || target.Status != model.StorageDataSetStatusPending || target.Generation <= source.Generation {
				t.Fatalf("target = %#v", target)
			}
			replayed, created, err := repos.Replacements.Authorize(t.Context(), input)
			if err != nil || created || replayed.ID != row.ID {
				t.Fatalf("replay row=%#v created=%v err=%v", replayed, created, err)
			}
		})
	}
}

func localWaitingCopy(t *testing.T, repos *repository.Repositories, source *model.StorageDataSet, key string) *model.StorageCopy {
	t.Helper()
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{BucketID: source.BucketID, ContentSize: 11, Checksum: testutil.StorageChecksum(key), RequestedCopies: source.CopyIndex + 1})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: source.BucketID, Key: key, ContentID: &content.ID, Size: 11, ETag: key, ContentType: "application/octet-stream"}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{StorageDataSetID: source.ID, CopyIndex: source.CopyIndex, ProviderID: source.ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress}}); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil {
		t.Fatal(err)
	}
	return &copies[0]
}

func TestLocalReplacementBatchesPreserveFrozenCopyObligations(t *testing.T) {
	localReplacementBatchesPreserveFrozenCopyObligations(t, testDB(t))
}

func localReplacementBatchesPreserveFrozenCopyObligations(t *testing.T, db *bun.DB) {
	repos, original, _ := localReplacementSource(t, db, "local-copy-batches")
	historical := localWaitingCopy(t, repos, original, "historical-one-copy")
	bucket, err := repos.Buckets.GetByID(t.Context(), original.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	two := 2
	if _, err := repos.Buckets.UpdateCopyPolicy(t.Context(), repository.UpdateBucketCopyPolicyInput{Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &two}); err != nil {
		t.Fatal(err)
	}
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "102"), CopyIndex: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 129 {
		localWaitingCopy(t, repos, source, fmt.Sprintf("waiting-%d", i))
	}
	row, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "batch"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := localCoordinatorTask(t, repos, row)
	markSourceReady(t, repos, row.TargetDataSetID)
	if err := repos.Replacements.Activate(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	done, err := repos.Replacements.AbandonUncreatedCopiesBatch(t.Context(), row.ID, row.TaskGeneration, coordinator.ID, 128)
	if err != nil || done {
		t.Fatalf("first batch done=%v err=%v", done, err)
	}
	pending, err := db.NewSelect().Model((*model.StorageCopy)(nil)).Where("storage_data_set_id = ? AND status = ?", source.ID, model.StorageCopyStatusPending).Count(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("remaining old copies=%d, want 1", pending)
	}
	done, err = repos.Replacements.AbandonUncreatedCopiesBatch(t.Context(), row.ID, row.TaskGeneration, coordinator.ID, 128)
	if err != nil || !done {
		t.Fatalf("second batch done=%v err=%v", done, err)
	}
	_, seeded, err := repos.Replacements.SeedMigrationBatch(t.Context(), row.ID, 128)
	if err != nil {
		t.Fatal(err)
	}
	for !seeded {
		_, seeded, err = repos.Replacements.SeedMigrationBatch(t.Context(), row.ID, 128)
		if err != nil {
			t.Fatal(err)
		}
	}
	item, err := repos.Replacements.NextPendingReplacementItem(t.Context(), row.ID)
	if err != nil || item == nil {
		t.Fatalf("missing recovery item: %#v %v", item, err)
	}
	snapshot, err := repos.Replacements.AcquireItem(t.Context(), repository.AcquireReplacementItemInput{ReplacementID: row.ID, ItemID: item.ID})
	if err != nil || snapshot == nil {
		t.Fatalf("failed old copy cancelled its required recovery: %#v %v", snapshot, err)
	}
	targetCopies, err := db.NewSelect().Model((*model.StorageCopy)(nil)).Where("storage_data_set_id = ?", row.TargetDataSetID).Count(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if targetCopies != 129 {
		t.Fatalf("target copies=%d, want 129", targetCopies)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), historical.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(copies) != 1 {
		t.Fatalf("historical policy was backfilled: %#v", copies)
	}
	errorsCount, err := db.NewSelect().Model((*model.StorageContent)(nil)).Where("bucket_id = ? AND error_message IS NOT NULL", bucket.ID).Count(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if errorsCount != 0 {
		t.Fatalf("handoff marked %d uploads failed", errorsCount)
	}
	if err := repos.Replacements.CompleteWithoutRemoteSource(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("pending target completed: %v", err)
	}
}

func TestLocalReplacementRetryAndCompletion(t *testing.T) {
	localReplacementRetryAndCompletion(t, testDB(t))
}

func localReplacementRetryAndCompletion(t *testing.T, db *bun.DB) {
	repos, source, _ := localReplacementSource(t, db, "local-completion")
	row, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "complete"})
	if err != nil {
		t.Fatal(err)
	}
	localCoordinatorTask(t, repos, row)
	if err := repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "interrupted"); err != nil {
		t.Fatal(err)
	}
	row, err = repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{ReplacementID: row.ID})
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != storagereplacement.StatusPreparingTarget {
		t.Fatalf("pending target resumed at %s", row.Status)
	}
	coordinator := localCoordinatorTask(t, repos, row)
	if err := repos.Replacements.MarkMigrating(t.Context(), row.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("pending target migrated: %v", err)
	}
	markSourceReady(t, repos, row.TargetDataSetID)
	if err := repos.Replacements.Activate(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	if err := repos.Replacements.CompleteWithoutRemoteSource(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("unseeded replacement completed: %v", err)
	}
	if _, _, err := repos.Replacements.SeedMigrationBatch(t.Context(), row.ID, 128); err != nil {
		t.Fatal(err)
	}
	if err := repos.Replacements.CompleteWithoutRemoteSource(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := repos.Replacements.GetByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != storagereplacement.StatusCompleted || completed.TaskID != nil || completed.TerminationEpoch != nil {
		t.Fatalf("completion=%#v", completed)
	}
}

func TestRejectedLocalTargetCanBeReplacedAgain(t *testing.T) {
	rejectedLocalTargetCanBeReplacedAgain(t, testDB(t))
}

func TestReplacementUploadWakeScope(t *testing.T) {
	replacementUploadWakeScope(t, testDB(t))
}

func replacementUploadWakeScope(t *testing.T, db *bun.DB) {
	repos := repository.NewRepositories(db)
	buckets := []*model.Bucket{{Name: "wake-replacement", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 1}, {Name: "wake-other", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 1}}
	for _, bucket := range buckets {
		if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
			t.Fatal(err)
		}
	}
	var expected int64
	for _, c := range []struct {
		name     string
		bucket   int
		copies   int
		reason   string
		accepted bool
	}{
		{"selected", 0, 2, storagepipeline.UploadPlanReplacementWaitReason, false},
		{"frozen-policy", 0, 1, storagepipeline.UploadPlanReplacementWaitReason, false},
		{"funding", 0, 2, "funding", false},
		{"providers", 0, 2, "providers", false},
		{"other-bucket", 1, 2, storagepipeline.UploadPlanReplacementWaitReason, false},
		{"accepted", 0, 2, storagepipeline.UploadPlanReplacementWaitReason, true},
	} {
		content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{BucketID: buckets[c.bucket].ID, ContentSize: 11, Checksum: testutil.StorageChecksum(c.name), RequestedCopies: c.copies})
		if err != nil {
			t.Fatal(err)
		}
		if c.accepted {
			if _, err := db.NewUpdate().Model((*model.StorageContent)(nil)).Set("accepted_at = ?", time.Now()).Where("id = ?", content.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		subjectType, subjectKey := string(model.TaskSubjectStorageContent), fmt.Sprint(content.ID)
		input := json.RawMessage(fmt.Sprintf(`{"content_id":%d}`, content.ID))
		row, _, err := repos.Tasks.Enqueue(t.Context(), &model.Task{Type: model.TaskTypeUploadPlan, IdempotencyKey: storagepipeline.UploadPlanKey(content.ID), InputVersion: 1, Input: input, InputHash: c.name, SubjectType: &subjectType, SubjectKey: &subjectKey})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.NewUpdate().Model((*model.Task)(nil)).Set("wait_reason = ?", c.reason).Set("available_at = ?", time.Now().Add(time.Hour)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		if c.name == "selected" {
			expected = row.ID
		}
	}
	ids, err := repos.Contents.PendingUploadPlansForReplica(t.Context(), buckets[0].ID, 1)
	if err != nil || len(ids) != 1 || ids[0] != expected {
		t.Fatalf("wake escaped replacement scope: ids=%v want=%d err=%v", ids, expected, err)
	}
}

func rejectedLocalTargetCanBeReplacedAgain(t *testing.T, db *bun.DB) {
	repos, source, _ := localReplacementSource(t, db, "local-repeated")
	localWaitingCopy(t, repos, source, "first-object")
	localWaitingCopy(t, repos, source, "second-object")
	first, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	firstTask := localCoordinatorTask(t, repos, first)
	if _, err := repos.Replacements.AbandonUncreatedCopiesBatch(t.Context(), first.ID, first.TaskGeneration, firstTask.ID, 1); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("pending target transferred copies: %v", err)
	}
	refused, err := repos.Contents.GetDataSetBindingByID(t.Context(), first.TargetDataSetID)
	if err != nil {
		t.Fatal(err)
	}
	ensure := localEnsureTask(t, repos, refused.ID)
	evidence := localRefusal(t, db, repos, refused, ensure)
	summaries, err := repos.Contents.ListDataSetSummaries(t.Context(), source.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	for _, summary := range summaries {
		if summary.ID == source.ID && !summary.ReplacementHasLateServiceRisk {
			t.Fatal("unsent source omitted its refused target's late service risk")
		}
	}
	reason := storagereplacement.FailureReasonTargetRejected
	if err := repos.Replacements.MarkFailed(t.Context(), first.ID, &reason, "refused"); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{ReplacementID: first.ID}); !errors.Is(err, storagereplacement.ErrNotRetryable) {
		t.Fatalf("refused target retried: %v", err)
	}
	check, err := repos.Replacements.Preflight(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	check.Targets[0].VerifiedCreationRejection = evidence
	second, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "303"), ClientRequestID: "second", Preflight: check})
	if err != nil {
		t.Fatal(err)
	}
	old, err := repos.Replacements.GetByID(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if old.Status != storagereplacement.StatusSuperseded || old.SupersededByID == nil || *old.SupersededByID != second.ID {
		t.Fatalf("predecessor=%#v", old)
	}
	if _, err := repos.Replacements.AbandonUncreatedCopiesBatch(t.Context(), first.ID, first.TaskGeneration, firstTask.ID, 128); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("superseded cleanup wrote: %v", err)
	}
	secondTask := localCoordinatorTask(t, repos, second)
	markSourceReady(t, repos, second.TargetDataSetID)
	if err := repos.Replacements.Activate(t.Context(), second.ID, second.TaskGeneration, secondTask.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Replacements.AbandonUncreatedCopiesBatch(t.Context(), second.ID, second.TaskGeneration, secondTask.ID, 128); err != nil {
		t.Fatal(err)
	}
	count, err := db.NewSelect().Model((*model.StorageCopy)(nil)).Where("storage_data_set_id = ?", second.TargetDataSetID).Count(t.Context())
	if err != nil || count != 2 {
		t.Fatalf("successor copies=%d err=%v", count, err)
	}
	if _, err := db.NewRaw("UPDATE tasks SET acknowledged_at = ?, retention_until = ? WHERE id = ?", time.Now().Add(-time.Hour), time.Now().Add(-time.Minute), ensure.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Tasks.DeleteRetained(t.Context(), time.Now(), 128); err != nil {
		t.Fatal(err)
	}
	kept, err := repos.Contents.GetDataSetBindingByID(t.Context(), refused.ID)
	if err != nil {
		t.Fatal(err)
	}
	if proof, err := kept.CreationRejectionEvidence(); err != nil || proof == nil {
		t.Fatalf("retained evidence=%#v err=%v", proof, err)
	}
}

func activateReplacement(t *testing.T, repos *repository.Repositories, id int64) error {
	t.Helper()
	row, err := repos.Replacements.GetByID(t.Context(), id)
	if err != nil {
		return err
	}
	if row.TaskID != nil {
		return repos.Replacements.Activate(t.Context(), id, row.TaskGeneration, *row.TaskID)
	}
	task := localCoordinatorTask(t, repos, row)
	if err := repos.Replacements.Activate(t.Context(), id, row.TaskGeneration, task.ID); err != nil {
		return err
	}
	return repos.Replacements.CompleteTask(t.Context(), id, row.TaskGeneration, task.ID)
}

func TestReplacementObservationAuthorization(t *testing.T) {
	replacementObservationAuthorization(t, testDB(t))
}

func TestReadyTargetRejectsStaleRefusalSettlement(t *testing.T) {
	readyTargetRejectsStaleRefusalSettlement(t, testDB(t))
}

func readyTargetRejectsStaleRefusalSettlement(t *testing.T, db *bun.DB) {
	for _, observed := range []bool{false, true} {
		t.Run(fmt.Sprintf("observed=%v", observed), func(t *testing.T) {
			repos, source, _ := localReplacementSource(t, db, fmt.Sprintf("stale-refusal-%v", observed))
			row, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "first"})
			if err != nil {
				t.Fatal(err)
			}
			localCoordinatorTask(t, repos, row)
			target, err := repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			ensure := localEnsureTask(t, repos, target.ID)
			evidence := localRefusal(t, db, repos, target, ensure)
			serviceID := onChainID(t, fmt.Sprintf("%d", 900000+target.ID))
			if observed {
				source, err = repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
				if err != nil {
					t.Fatal(err)
				}
				target, err = repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
				if err != nil {
					t.Fatal(err)
				}
				row, err = repos.Replacements.GetByID(t.Context(), row.ID)
				if err != nil {
					t.Fatal(err)
				}
				check := repository.ReplacementPreflight{Source: *source, Targets: []repository.ReplacementTargetCheck{{Replacement: *row, DataSet: *target}}}
				err = repos.Replacements.BindObservedService(t.Context(), repository.BindObservedReplacementServiceInput{Preflight: check, StorageDataSetID: target.ID, DataSetID: serviceID, ClientDataSetID: evidence.ClientDataSetID})
			} else {
				err = repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: target.ID, DataSetID: serviceID, ClientDataSetID: &evidence.ClientDataSetID})
			}
			if err != nil {
				t.Fatal(err)
			}
			reason := storagereplacement.FailureReasonTargetRejected
			if err := repos.Replacements.MarkFailed(t.Context(), row.ID, &reason, "obsolete refusal"); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("ready target accepted refusal settlement: %v", err)
			}
			current, err := repos.Replacements.GetByID(t.Context(), row.ID)
			if err != nil || current.Status != storagereplacement.StatusPreparingTarget || current.FailureReason != nil {
				t.Fatalf("stale refusal changed replacement: %#v %v", current, err)
			}
		})
	}
}

func TestRemoteSourceCannotCompleteLocally(t *testing.T) {
	remoteSourceCannotCompleteLocally(t, testDB(t))
}

func remoteSourceCannotCompleteLocally(t *testing.T, db *bun.DB) {
	repos, source, _ := localReplacementSource(t, db, "remote-completion-guard")
	markSourceReady(t, repos, source.ID)
	row, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := localCoordinatorTask(t, repos, row)
	markSourceReady(t, repos, row.TargetDataSetID)
	if err := repos.Replacements.Activate(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repos.Replacements.SeedMigrationBatch(t.Context(), row.ID, 128); err != nil {
		t.Fatal(err)
	}
	if err := repos.Replacements.CompleteWithoutRemoteSource(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("remote source bypassed retirement: %v", err)
	}
	current, err := repos.Replacements.GetByID(t.Context(), row.ID)
	if err != nil || current.Status != storagereplacement.StatusMigrating {
		t.Fatalf("local completion changed remote replacement: %#v %v", current, err)
	}
}

func replacementObservationAuthorization(t *testing.T, db *bun.DB) {
	for _, role := range []string{"source", "target"} {
		for _, operation := range []string{"fresh", "stale", "concurrent"} {
			t.Run(fmt.Sprintf("%s/%s", role, operation), func(t *testing.T) {
				repos, source, ensure := localReplacementSource(t, db, fmt.Sprintf("observation-%s-%s", role, operation))
				var sourceEvidence *model.DataSetCreationRejection
				if role == "source" {
					sourceEvidence = localRefusal(t, db, repos, source, ensure)
				}
				row, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "observation", VerifiedCreationRejection: sourceEvidence})
				if err != nil {
					t.Fatal(err)
				}
				observedID := source.ID
				if role == "target" {
					observedID = row.TargetDataSetID
					target, err := repos.Contents.GetDataSetBindingByID(t.Context(), observedID)
					if err != nil {
						t.Fatal(err)
					}
					localRefusal(t, db, repos, target, localEnsureTask(t, repos, observedID))
					reason := storagereplacement.FailureReasonTargetRejected
					if err := repos.Replacements.MarkFailed(t.Context(), row.ID, &reason, "refused"); err != nil {
						t.Fatal(err)
					}
				}
				if role == "source" {
					if err := repos.Replacements.MarkFailed(t.Context(), row.ID, nil, "replacement stopped"); err != nil {
						t.Fatal(err)
					}
				}
				check, err := repos.Replacements.Preflight(t.Context(), source.ID)
				if err != nil {
					t.Fatal(err)
				}
				binding, err := repos.Contents.GetDataSetBindingByID(t.Context(), observedID)
				if err != nil {
					t.Fatal(err)
				}
				if operation == "stale" {
					if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("task_generation = task_generation + 1").Where("id = ?", row.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				input := repository.BindObservedReplacementServiceInput{Preflight: *check, StorageDataSetID: observedID, DataSetID: onChainID(t, fmt.Sprintf("%d", 800000+observedID)), ClientDataSetID: *binding.ClientDataSetID}
				if operation == "concurrent" {
					results := make(chan error, 2)
					for range 2 {
						go func() { results <- repos.Replacements.BindObservedService(t.Context(), input) }()
					}
					bound := 0
					for range 2 {
						if result := <-results; result == nil {
							bound++
						} else if !errors.Is(result, storagereplacement.ErrSourceOutcomeUnknown) {
							t.Fatalf("concurrent observation: %v", result)
						}
					}
					if bound != 1 {
						t.Fatalf("observation accepted %d concurrent bindings", bound)
					}
				} else {
					err = repos.Replacements.BindObservedService(t.Context(), input)
				}
				if operation == "stale" {
					if !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
						t.Fatalf("changed ownership accepted: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := repos.Replacements.BindObservedService(t.Context(), input); !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
					t.Fatalf("replayed observation accepted: %v", err)
				}
				binding, err = repos.Contents.GetDataSetBindingByID(t.Context(), observedID)
				if err != nil || binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.EnsureTaskID != nil || len(binding.CreationRejection) != 0 {
					t.Fatalf("observed service=%#v %v", binding, err)
				}
				if err := repos.Contents.RecordDataSetClientID(t.Context(), observedID, ensure.ID, onChainID(t, "999")); !errors.Is(err, repository.ErrConflict) {
					t.Fatalf("observation restored creation permission: %v", err)
				}
				if role == "target" {
					resumed, err := repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{ReplacementID: row.ID})
					if err != nil || resumed.Status != storagereplacement.StatusPreparingTarget || resumed.FailureReason != nil {
						t.Fatalf("recovered target did not resume: %#v %v", resumed, err)
					}
				}
			})
		}
	}
}
