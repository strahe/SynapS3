package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestStartObjectUploadAttemptValidatesRequiredIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "upload-identity")
	valid := repository.EnsureContentInput{
		BucketID:        bucket.ID,
		ContentSize:     1,
		Checksum:        testutil.StorageChecksum("checksum-1"),
		RequestedCopies: 1,
	}
	tests := []struct {
		name   string
		mutate func(*repository.EnsureContentInput)
	}{
		{name: "bucket", mutate: func(input *repository.EnsureContentInput) { input.BucketID = 0 }},
		{name: "content size", mutate: func(input *repository.EnsureContentInput) { input.ContentSize = -1 }},
		{name: "empty checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = "" }},
		{name: "short checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = strings.Repeat("a", 63) }},
		{name: "uppercase checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = strings.Repeat("A", 64) }},
		{name: "prefixed checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = "sha256:" + strings.Repeat("a", 64) }},
		{name: "non hex checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = strings.Repeat("a", 63) + "g" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := valid
			tt.mutate(&input)
			if _, err := repos.Contents.EnsureContent(t.Context(), input); !errors.Is(err, repository.ErrInvalidInput) {
				t.Fatalf("EnsureContent error = %v, want ErrInvalidInput", err)
			}
		})
	}
	count, err := db.NewSelect().Model((*model.StorageContent)(nil)).Count(t.Context())
	if err != nil {
		t.Fatalf("count storage uploads: %v", err)
	}
	if count != 0 {
		t.Fatalf("storage upload count = %d after invalid writes, want 0", count)
	}
}

func TestListCopiesDerivesNewDataSetFromCreatorContent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "copy-data-set-creator")
	creator, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 1,
		Checksum: testutil.StorageChecksum("creator-content"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent(creator): %v", err)
	}
	reused, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 2,
		Checksum: testutil.StorageChecksum("reused-content"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent(reused): %v", err)
	}
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
		CreatedByContentID: creator.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	for _, tc := range []struct {
		content        *model.StorageContent
		transferMethod model.StorageCopyTransferMethod
		wantNew        bool
	}{
		{content: creator, transferMethod: model.StorageCopyTransferMethodIngress, wantNew: true},
		{content: reused, transferMethod: model.StorageCopyTransferMethodPeerPull, wantNew: false},
	} {
		if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), tc.content.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID,
			CopyIndex:        binding.CopyIndex,
			ProviderID:       binding.ProviderID,
			TransferMethod:   tc.transferMethod,
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings(%d): %v", tc.content.ID, err)
		}
		copies, err := repos.Contents.ListCopies(t.Context(), tc.content.ID)
		if err != nil {
			t.Fatalf("ListCopies(%d): %v", tc.content.ID, err)
		}
		if len(copies) != 1 || copies[0].IsNewDataSet != tc.wantNew {
			t.Fatalf("ListCopies(%d) = %+v, want one copy with is_new_data_set=%t", tc.content.ID, copies, tc.wantNew)
		}
	}
}

// A replacement resumes only through the Data Sets retry. When the Engine
// fails the coordinator claim itself, the replacement moves to the state that
// retry accepts for its phase; a finished replacement or another task's claim
// changes nothing.
func TestFailForEngineTaskLeavesTheReplacementRetryable(t *testing.T) {
	for _, tt := range []struct {
		status     storagereplacement.Status
		waitReason string
		want       storagereplacement.Status
	}{
		{status: storagereplacement.StatusPreparingTarget, want: storagereplacement.StatusFailed},
		{status: storagereplacement.StatusMigrating, want: storagereplacement.StatusFailed},
		{status: storagereplacement.StatusWaiting, waitReason: string(storagereplacement.WaitReasonTargetCreating), want: storagereplacement.StatusFailed},
		{status: storagereplacement.StatusRetiring, want: storagereplacement.StatusCleanupAttention},
		{status: storagereplacement.StatusCompleted, want: storagereplacement.StatusCompleted},
	} {
		t.Run(string(tt.status), func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := t.Context()
			bucket := seedBucket(t, db, "engine-failed-coordinator")
			source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
			})
			if err != nil {
				t.Fatalf("EnsureDataSetBinding: %v", err)
			}
			markSourceReady(t, repos, source.ID)
			replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: onChainID(t, "202"), ClientRequestID: "engine-failed-coordinator",
			})
			if err != nil {
				t.Fatalf("Authorize: %v", err)
			}
			coordinator, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{
				Type:         model.TaskTypeProviderReplacementCoordinate,
				InputVersion: 1, Input: []byte(fmt.Sprintf(`{"replacement_id":%d,"generation":%d}`, replacement.ID, replacement.TaskGeneration)),
				SubjectType: "storage_replacement", SubjectKey: strconv.FormatInt(replacement.ID, 10),
			}))
			if err != nil {
				t.Fatalf("enqueue coordinator: %v", err)
			}
			if err := repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, coordinator.ID); err != nil {
				t.Fatalf("BindTask: %v", err)
			}
			var waitReason *string
			if tt.waitReason != "" {
				waitReason = &tt.waitReason
			}
			if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
				Set("status = ?", tt.status).Set("wait_reason = ?", waitReason).
				Where("id = ?", replacement.ID).Exec(ctx); err != nil {
				t.Fatalf("set replacement status: %v", err)
			}

			if err := repos.Replacements.FailForEngineTask(ctx, coordinator.ID+1, "stopped"); err != nil {
				t.Fatalf("FailForEngineTask(another task): %v", err)
			}
			if stored, err := repos.Replacements.GetByID(ctx, replacement.ID); err != nil || stored.Status != tt.status {
				t.Fatalf("replacement after another task's failure = %#v, err=%v, want %s", stored, err, tt.status)
			}
			if err := repos.Replacements.FailForEngineTask(ctx, coordinator.ID, "stopped"); err != nil {
				t.Fatalf("FailForEngineTask: %v", err)
			}
			stored, err := repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.Status != tt.want {
				t.Fatalf("replacement after engine failure = %#v, err=%v, want %s", stored, err, tt.want)
			}
		})
	}
}

func TestAuthorizeReplacementPreservesCurrentSource(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-current-source")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID:   bucket.ID,
		ProviderID: onChainID(t, "101"),
		CopyIndex:  0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}

	markSourceReady(t, repos, source.ID)
	replacement, created, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"),
		ClientRequestID:  "replacement-current-source",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if !created {
		t.Fatal("Authorize created = false, want true")
	}
	target, err := repos.Contents.GetDataSetBindingByID(t.Context(), replacement.TargetDataSetID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID(target): %v", err)
	}
	if target == nil {
		t.Fatal("replacement target is nil")
	}
	if target.IsCurrent {
		t.Fatal("replacement target is current before activation")
	}
	currentSource, err := repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID(source): %v", err)
	}
	if currentSource == nil || !currentSource.IsCurrent {
		t.Fatalf("replacement source = %#v, want current source", currentSource)
	}
}

func TestAuthorizeReplacementWaitsForUnknownCreation(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "replacement-source-creating")
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("client_data_set_id = ?", onChainID(t, "909")).Where("id = ?", source.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	input := repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "source-creating",
	}
	if _, _, err := repos.Replacements.Authorize(ctx, input); !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
		t.Fatalf("Authorize(pending source) error = %v, want ErrSourceOutcomeUnknown", err)
	}
	if err := repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID: source.ID, TxHash: "0xcreate",
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	if _, _, err := repos.Replacements.Authorize(ctx, input); !errors.Is(err, storagereplacement.ErrSourceOutcomeUnknown) {
		t.Fatalf("Authorize(creating source) error = %v, want ErrSourceOutcomeUnknown", err)
	}

	markSourceReady(t, repos, source.ID)
	replacement, created, err := repos.Replacements.Authorize(ctx, input)
	if err != nil || !created {
		t.Fatalf("Authorize(ready source) = %#v, created=%v, err=%v", replacement, created, err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: replacement.TargetDataSetID, DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady(target): %v", err)
	}
	if err := activateReplacement(t, repos, replacement.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	replayed, created, err := repos.Replacements.Authorize(ctx, input)
	if err != nil || created || replayed == nil || replayed.ID != replacement.ID {
		t.Fatalf("Authorize(replay) = %#v, created=%v, err=%v, want replacement %d", replayed, created, err, replacement.ID)
	}
}

// An earlier replacement whose target may still be created on chain cannot be
// abandoned: superseding it would leave that service untracked. Once the
// target's creation fence is released, a new request proceeds as before.
func TestAuthorizeReplacementWaitsForEarlierTargetCreation(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-target-creating")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	markSourceReady(t, repos, source.ID)
	first, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "target-creating-first",
	})
	if err != nil {
		t.Fatalf("Authorize(first): %v", err)
	}
	ensureTask, _, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
		Type:         model.TaskTypeStorageDataSetEnsure,
		InputVersion: 1, Input: []byte(fmt.Sprintf(`{"data_set_id":%d}`, first.TargetDataSetID)),
		SubjectType: "storage_data_set", SubjectKey: strconv.FormatInt(first.TargetDataSetID, 10),
	}))
	if err != nil {
		t.Fatalf("enqueue target ensure: %v", err)
	}
	if err := repos.Contents.BindDataSetEnsureTask(t.Context(), first.TargetDataSetID, ensureTask.ID); err != nil {
		t.Fatalf("bind target ensure: %v", err)
	}
	successor := repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "303"), ClientRequestID: "target-creating-successor",
	}
	if _, _, err := repos.Replacements.Authorize(t.Context(), successor); !errors.Is(err, storagereplacement.ErrTargetCreating) {
		t.Fatalf("Authorize(successor) error = %v, want ErrTargetCreating", err)
	}
	if kept, err := repos.Replacements.GetByID(t.Context(), first.ID); err != nil || kept.Status == storagereplacement.StatusSuperseded {
		t.Fatalf("first replacement = %#v err=%v, want it still in charge of its target", kept, err)
	}

	failLocalEnsure(t, db, ensureTask)
	if _, created, err := repos.Replacements.Authorize(t.Context(), successor); err != nil || !created {
		t.Fatalf("Authorize(successor after failed initialization) created=%v err=%v", created, err)
	}
	if superseded, err := repos.Replacements.GetByID(t.Context(), first.ID); err != nil || superseded.Status != storagereplacement.StatusSuperseded {
		t.Fatalf("first replacement = %#v err=%v, want it superseded", superseded, err)
	}
}

func TestAuthorizeReplacementAllowsFailedInitializationWithRequestEvidence(t *testing.T) {
	for _, tt := range []struct {
		name     string
		bucket   string
		recordID bool
	}{
		{name: "nothing was sent", bucket: "replacement-dead-fence-unsent", recordID: false},
		{name: "a request may have gone out", bucket: "replacement-dead-fence-sent", recordID: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := t.Context()
			bucket := seedBucket(t, db, tt.bucket)
			source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
			})
			if err != nil {
				t.Fatalf("EnsureDataSetBinding: %v", err)
			}
			markSourceReady(t, repos, source.ID)
			first, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: onChainID(t, "202"), ClientRequestID: "dead-fence-first",
			})
			if err != nil {
				t.Fatalf("Authorize(first): %v", err)
			}
			ensureTask, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{
				Type:         model.TaskTypeStorageDataSetEnsure,
				InputVersion: 1, Input: []byte(fmt.Sprintf(`{"data_set_id":%d}`, first.TargetDataSetID)),
				SubjectType: "storage_data_set", SubjectKey: strconv.FormatInt(first.TargetDataSetID, 10),
			}))
			if err != nil {
				t.Fatalf("enqueue target ensure: %v", err)
			}
			if err := repos.Contents.BindDataSetEnsureTask(ctx, first.TargetDataSetID, ensureTask.ID); err != nil {
				t.Fatalf("bind target ensure: %v", err)
			}
			if tt.recordID {
				if err := repos.Contents.RecordDataSetClientID(ctx, first.TargetDataSetID, ensureTask.ID, onChainID(t, "909")); err != nil {
					t.Fatalf("record client data set ID: %v", err)
				}
			}
			// The creation task ended without releasing its fence.
			if _, err := db.NewRaw(`UPDATE tasks SET status = ?, finished_at = ? WHERE id = ?`,
				model.TaskStatusFailed, time.Now(), ensureTask.ID).Exec(ctx); err != nil {
				t.Fatalf("fail target ensure: %v", err)
			}

			if err := repos.Replacements.MarkFailed(t.Context(), first.ID, nil, "replacement stopped"); err != nil {
				t.Fatal(err)
			}

			successor := repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: onChainID(t, "303"), ClientRequestID: "dead-fence-successor",
			}
			_, created, err := repos.Replacements.Authorize(ctx, successor)
			if err != nil || !created {
				t.Fatalf("Authorize(successor) created=%v err=%v, want the slot released", created, err)
			}
			// The earlier target is given up together with its fence, so retrying
			// the dead creation task finds nothing left to create.
			abandoned, err := repos.Contents.GetDataSetBindingByID(ctx, first.TargetDataSetID)
			if err != nil || abandoned == nil || abandoned.Status != model.StorageDataSetStatusRetired || abandoned.EnsureTaskID != nil {
				t.Fatalf("earlier target = %#v err=%v, want it retired with its creation fence released", abandoned, err)
			}
			if tt.recordID && (abandoned.ClientDataSetID == nil || !abandoned.ClientDataSetID.Equal(onChainID(t, "909"))) {
				t.Fatalf("abandoned target lost request evidence: %#v", abandoned)
			}
			if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, first.TargetDataSetID, ensureTask.ID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("authorizing the dead creation task error = %v, want ErrConflict", err)
			}
		})
	}
}

func TestAttachReplacementTargetCopyRejectsMismatchedUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-item-upload-identity")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	markSourceReady(t, repos, source.ID)
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "replacement-item-upload-identity",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	target, err := repos.Contents.GetDataSetBindingByID(t.Context(), replacement.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("GetDataSetBindingByID(target) = %#v, err=%v", target, err)
	}
	itemUpload := startCopyHealthUpload(t, repos, bucket.ID, "replacement-item-upload", 1, "replacement-item-checksum", 1)
	wrongUpload := startCopyHealthUpload(t, repos, bucket.ID, "replacement-wrong-upload", 1, "replacement-wrong-checksum", 1)
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), itemUpload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID, CopyIndex: target.CopyIndex,
		ProviderID: target.ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(item upload): %v", err)
	}
	item := &storagereplacement.Item{
		ReplacementID:   replacement.ID,
		ContentID:       itemUpload.ID,
		TargetDataSetID: target.ID,
		Status:          storagereplacement.ItemStatusPending,
	}
	if _, err := db.NewInsert().Model(item).Exec(t.Context()); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}

	if _, err := repos.Replacements.AttachTargetCopy(t.Context(), repository.AttachReplacementTargetCopyInput{
		ReplacementID: replacement.ID,
		ItemID:        item.ID,
		ContentID:     wrongUpload.ID,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("AttachTargetCopy error = %v, want ErrConflict", err)
	}
	count, err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Where("content_id = ? AND storage_data_set_id = ?", wrongUpload.ID, target.ID).
		Count(t.Context())
	if err != nil {
		t.Fatalf("count rolled-back wrong target copies: %v", err)
	}
	if count != 0 {
		t.Fatalf("wrong upload target copy count = %d, want 0", count)
	}
}

func TestEnsureDataSetBindingDoesNotReuseProviderBeforeRetirement(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "provider-retirement-occupancy")
	providerID := onChainID(t, "101")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding(source): %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Contents.MarkDataSetDraining(t.Context(), source.ID, "replacement in progress"); err != nil {
		t.Fatalf("MarkDataSetDraining: %v", err)
	}

	if _, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	}); !errors.Is(err, repository.ErrAlreadyExists) {
		t.Fatalf("reuse draining provider error = %v, want ErrAlreadyExists", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusRetired).
		Where("id = ?", source.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("retire source data set: %v", err)
	}

	reused, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("reuse retired provider: %v", err)
	}
	if reused.ID == source.ID || reused.Generation <= source.Generation {
		t.Fatalf("reused data set = %#v, want a newer generation than %#v", reused, source)
	}
}

// Replacement refuses a source that is still being created, but a drained
// generation whose creation is in flight must still record what it sent and
// what it created, so the service can be ended, and never take its slot back.
func TestDrainedGenerationRecordsItsCreationWithoutReturningToItsSlot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "drained-while-creating")
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding(source): %v", err)
	}
	markSourceReady(t, repos, source.ID)
	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"),
		ClientRequestID:  "drained-while-creating",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: replacement.TargetDataSetID, DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady(target): %v", err)
	}
	if err := activateReplacement(t, repos, replacement.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	// Authorize no longer drains a generation mid-creation, so build that
	// state directly: the drained source has not learned its data set yet.
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("data_set_id = NULL").
		Where("id = ?", source.ID).
		Exec(ctx); err != nil {
		t.Fatalf("clearing drained source identity: %v", err)
	}

	clientID := onChainID(t, "9001")
	if err := repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID: source.ID, TxHash: "0xcreate", StatusURL: "https://provider.example/status", ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatalf("MarkDataSetCreating(drained source): %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatalf("MarkDataSetReady(drained source): %v", err)
	}
	drained, err := repos.Contents.GetDataSetBindingByID(ctx, source.ID)
	if err != nil || drained == nil {
		t.Fatalf("GetDataSetBindingByID(source) = %#v, %v", drained, err)
	}
	if drained.Status != model.StorageDataSetStatusDraining || drained.IsCurrent ||
		drained.DataSetID == nil || !drained.DataSetID.Equal(onChainID(t, "1001")) ||
		drained.TxHash == nil || *drained.TxHash != "0xcreate" {
		t.Fatalf("drained source = %#v, want it draining with its creation recorded", drained)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: onChainID(t, "1003"),
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkDataSetReady(another data set) error = %v, want ErrConflict", err)
	}
	// The target owns the slot now, and a creation step cannot move it back.
	if err := repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID: replacement.TargetDataSetID, TxHash: "0xlate",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkDataSetCreating(ready target) error = %v, want ErrConflict", err)
	}
}

func TestRetryReplacementRejectsUnknownFailureReason(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-unknown-reason")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	markSourceReady(t, repos, source.ID)
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "unknown-failure-reason",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusFailed).
		Set("failure_reason = ?", "future_failure_reason").
		Set("last_error = ?", "written by a newer binary").
		Where("id = ?", replacement.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("store future failure reason: %v", err)
	}
	if _, err := repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{
		ReplacementID: replacement.ID,
	}); !errors.Is(err, storagereplacement.ErrNotRetryable) {
		t.Fatalf("Retry error = %v, want ErrNotRetryable", err)
	}
}

// Retiring a rejected generation frees its provider without erasing anything.
// Deleting it would have to refuse whenever a copy was bound to it, which is
// exactly when the provider is most worth getting back.
func TestStorageContentBindingRejectsCrossBucketIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	sourceBucket := seedBucket(t, db, "upload-source-bucket")
	targetBucket := seedBucket(t, db, "upload-target-bucket")
	version := newObjectVersion(targetBucket.ID, "cross-bucket.txt", model.NewVersionID(), 11)
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	content := startCopyHealthUpload(t, repos, sourceBucket.ID, "source-version", version.Size, "cross-bucket-checksum", 1)
	commitStorageHealthCopy(t, db, repos, sourceBucket.ID, content.ID, 0, "901", "1901", "2901", "https://provider.example/cross-bucket")

	if _, err := repos.Contents.BindReadableUploadForVersion(t.Context(), repository.BindReadableUploadForVersionInput{
		ContentID: content.ID, BucketID: targetBucket.ID, VersionID: version.VersionID,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("BindReadableUploadForVersion error = %v, want ErrConflict", err)
	}

	if _, err := db.NewRaw(`UPDATE object_versions
		SET content_id = ?
		WHERE version_id = ?`, content.ID, version.VersionID).Exec(t.Context()); err == nil {
		t.Fatal("direct cross-bucket object version binding succeeded")
	}
}

func startCopyHealthUpload(
	t *testing.T,
	repos *repository.Repositories,
	bucketID int64,
	versionID string,
	size int64,
	checksum string,
	requestedCopies int,
) *model.StorageContent {
	t.Helper()
	origin, err := repos.Objects.GetVersionByID(t.Context(), versionID)
	if err != nil {
		t.Fatalf("GetVersionByID(origin): %v", err)
	}
	if origin == nil {
		origin = newObjectVersion(bucketID, "origin-"+versionID, versionID, size)
		origin.Checksum = checksum
		if _, err := createVersion(t, repos, origin); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(origin): %v", err)
		}
	}
	upload, err := repos.Contents.EnsureContent(context.Background(), repository.EnsureContentInput{
		BucketID: bucketID, ContentSize: size,
		Checksum: testutil.StorageChecksum(checksum), RequestedCopies: requestedCopies,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	return upload
}

func ensureCopyHealthBinding(
	t *testing.T,
	repos *repository.Repositories,
	bucketID, contentID int64,
	copyIndex int,
	providerID string,
) *model.StorageDataSet {
	t.Helper()
	binding, err := repos.Contents.EnsureDataSetBinding(context.Background(), repository.EnsureDataSetBindingInput{
		BucketID: bucketID, ProviderID: onChainID(t, providerID), CopyIndex: copyIndex,
		CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	return binding
}

func commitStorageHealthCopy(
	t *testing.T,
	db *bun.DB,
	repos *repository.Repositories,
	bucketID, contentID int64,
	copyIndex int,
	providerID, dataSetID, pieceID, retrievalURL string,
) *model.StorageDataSet {
	t.Helper()
	binding := ensureCopyHealthBinding(t, repos, bucketID, contentID, copyIndex, providerID)
	if err := repos.Contents.MarkDataSetReady(context.Background(), repository.MarkDataSetReadyInput{
		ID: binding.ID, DataSetID: onChainID(t, dataSetID),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(context.Background(), contentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: copyIndex,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, providerID),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	testutil.CommitStorageCopy(t, db, repos, testutil.CommitCopyInput{
		ContentID: contentID, CopyIndex: copyIndex, PieceCID: "bafk2bzacestorhealth",
		PieceID: onChainIDPtr(t, pieceID), RetrievalURL: retrievalURL,
	})
	got, err := repos.Contents.GetDataSetBindingByID(context.Background(), binding.ID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID: %v", err)
	}
	return got
}

// A failed copy flags its content only once no copy is left that holds it or
// may still hold it.
func TestCopyFailureFlagsContentOnlyWhenNoCopyCanServeIt(t *testing.T) {
	tests := []struct {
		name     string
		other    string
		wantFlag bool
	}{
		{name: "a readable copy remains", other: "readable"},
		{name: "another copy is still pending", other: "pending"},
		{name: "the last copy fails", other: "failed", wantFlag: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			bucket := seedBucket(t, db, "copy-failure-"+tt.other)
			upload := startCopyHealthUpload(t, repos, bucket.ID, model.NewVersionID(), 11, "copy-failure-"+tt.other, 2)
			if tt.other == "readable" {
				commitStorageHealthCopy(t, db, repos, bucket.ID, upload.ID, 0, "101", "1001", "3001", "https://provider-101.example/piece")
			} else {
				ensureCopyHealthBinding(t, repos, bucket.ID, upload.ID, 0, "101")
			}
			failing := ensureCopyHealthBinding(t, repos, bucket.ID, upload.ID, 1, "102")
			bindings := []repository.UploadCopyBindingInput{{
				StorageDataSetID: failing.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "102"),
			}}
			if tt.other != "readable" {
				other, err := repos.Contents.GetDataSetBindingByCopyIndex(t.Context(), bucket.ID, 0)
				if err != nil || other == nil {
					t.Fatalf("GetDataSetBindingByCopyIndex = %#v, err=%v", other, err)
				}
				bindings = append(bindings, repository.UploadCopyBindingInput{
					StorageDataSetID: other.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101"),
				})
			}
			if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), upload.ID, bindings); err != nil {
				t.Fatalf("CreateUploadCopiesForBindings: %v", err)
			}
			if tt.other == "failed" {
				if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
					ContentID: upload.ID, CopyIndex: 0, LastError: "first provider rejected the transfer",
				}); err != nil {
					t.Fatalf("MarkUploadCopyFailed(0): %v", err)
				}
			}
			if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
				ContentID: upload.ID, CopyIndex: 1, LastError: "second provider rejected the transfer",
			}); err != nil {
				t.Fatalf("MarkUploadCopyFailed(1): %v", err)
			}
			stored, err := repos.Contents.GetByID(t.Context(), upload.ID)
			if err != nil || stored == nil {
				t.Fatalf("GetByID = %#v, err=%v", stored, err)
			}
			if flagged := stored.ErrorMessage != nil; flagged != tt.wantFlag {
				t.Fatalf("content error = %v, want flagged=%t", stored.ErrorMessage, tt.wantFlag)
			}
		})
	}
}

func terminationTestInput(t *testing.T, repos *repository.Repositories, replacement *storagereplacement.Replacement, role storagereplacement.TerminationRole) repository.RecordTerminationEvidenceInput {
	t.Helper()
	dataSetID := replacement.SourceDataSetID
	if role == storagereplacement.TerminationRoleAbandonedTarget {
		dataSetID = replacement.TargetDataSetID
	}
	generation, err := repos.Contents.NextDataSetRetirementGeneration(t.Context(), dataSetID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(storagereplacement.RetireInput{ReplacementID: replacement.ID, DataSetID: dataSetID, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{Type: model.TaskTypeStorageDataSetRetire, InputVersion: 1, Input: raw, SubjectType: "storage_data_set", SubjectKey: strconv.FormatInt(dataSetID, 10)}))
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.BindDataSetRetirementTask(t.Context(), dataSetID, generation, task.ID); err != nil {
		t.Fatal(err)
	}
	return repository.RecordTerminationEvidenceInput{ReplacementID: replacement.ID, DataSetID: dataSetID, Generation: generation, TaskID: task.ID, Role: role}
}

func TestAbandonedTerminationEpochReplayPreservesEvidence(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		t.Run(fmt.Sprintf("transaction=%t", transactional), func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := t.Context()
			bucket := seedBucket(t, db, "abandoned-termination-replay")
			source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0})
			if err != nil {
				t.Fatal(err)
			}
			markSourceReady(t, repos, source.ID)
			replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
				BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: onChainID(t, "202"), ClientRequestID: "abandoned-replay",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("status = ?", storagereplacement.StatusSuperseded).Where("id = ?", replacement.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			input := terminationTestInput(t, repos, replacement, storagereplacement.TerminationRoleAbandonedTarget)
			input.TxHash = "0xoriginal"
			if err := repos.Replacements.RecordTerminationEvidence(ctx, input); err != nil {
				t.Fatal(err)
			}
			conflictingSubmission := input
			conflictingSubmission.TxHash = "0xother"
			if err := repos.Replacements.RecordTerminationEvidence(ctx, conflictingSubmission); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("conflicting pending submission error = %v", err)
			}
			stored, err := repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || stored.AbandonedTerminationEpoch != nil || stored.AbandonedTerminationTxHash == nil || *stored.AbandonedTerminationTxHash != "0xoriginal" {
				t.Fatalf("pending termination = %#v, err=%v", stored, err)
			}
			if err := repos.Replacements.CompleteAbandonedTargetTermination(ctx, replacement.ID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("pending completion error = %v", err)
			}
			input.Epoch, input.ConfirmedTxHash = new(int64(0)), "0xconfirmed"
			if err := repos.Replacements.RecordTerminationEvidence(ctx, input); err != nil {
				t.Fatal(err)
			}
			var original storagereplacement.Termination
			if err := db.NewSelect().Model(&original).Where("replacement_id = ?", replacement.ID).Scan(ctx); err != nil {
				t.Fatal(err)
			}
			originalAttempts, err := repos.Replacements.ListTerminationAttempts(ctx, []int64{replacement.ID})
			if err != nil || len(originalAttempts) != 1 {
				t.Fatalf("attempt history = %#v, err=%v", originalAttempts, err)
			}
			if originalAttempts[0].TaskID != nil || originalAttempts[0].AttemptedAt != nil || originalAttempts[0].ReceiptStatus != nil {
				t.Fatalf("observed transaction has inferred submission metadata: %#v", originalAttempts[0])
			}
			replay := func(txRepos *repository.Repositories) error {
				if err := txRepos.Replacements.RecordTerminationEvidence(ctx, input); err != nil {
					return err
				}
				for _, mutate := range []func(*repository.RecordTerminationEvidenceInput){
					func(in *repository.RecordTerminationEvidenceInput) { in.Epoch = new(int64(1)) },
					func(in *repository.RecordTerminationEvidenceInput) { in.ConfirmedTxHash = "0xconflicting" },
					func(in *repository.RecordTerminationEvidenceInput) { in.TaskID++ },
					func(in *repository.RecordTerminationEvidenceInput) { in.Generation++ },
				} {
					conflict := input
					mutate(&conflict)
					if err := txRepos.Replacements.RecordTerminationEvidence(ctx, conflict); !errors.Is(err, repository.ErrConflict) {
						return fmt.Errorf("conflict returned %v", err)
					}
				}
				return nil
			}
			if transactional {
				err = repos.WithTx(ctx, replay)
			} else {
				err = replay(repos)
			}
			if err != nil {
				t.Fatal(err)
			}
			var ledger []storagereplacement.Termination
			if err := db.NewSelect().Model(&ledger).Where("replacement_id = ?", replacement.ID).Scan(ctx); err != nil {
				t.Fatal(err)
			}
			if len(ledger) != 1 || ledger[0].ID != original.ID || ledger[0].Epoch == nil || *ledger[0].Epoch != 0 || !ledger[0].UpdatedAt.Equal(original.UpdatedAt) {
				t.Fatalf("ledger = %#v, want unchanged confirmed termination %#v", ledger, original)
			}
			attempts, err := repos.Replacements.ListTerminationAttempts(ctx, []int64{replacement.ID})
			if err != nil || len(attempts) != 1 {
				t.Fatalf("attempt history = %#v, err=%v", attempts, err)
			}
			if attempts[0].ID != originalAttempts[0].ID || attempts[0].TxHash == nil || *attempts[0].TxHash != "0xoriginal" ||
				attempts[0].ConfirmedTxHash == nil || *attempts[0].ConfirmedTxHash != "0xconfirmed" || !attempts[0].UpdatedAt.Equal(originalAttempts[0].UpdatedAt) {
				t.Fatalf("attempt = %#v, want unchanged evidence %#v", attempts[0], originalAttempts[0])
			}
		})
	}
}

// Retirement reads its termination evidence from the permanent ledger.
func TestRecordedTerminationEpochIsVisibleToTheRetirementGate(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-termination-evidence")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	markSourceReady(t, repos, source.ID)
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "termination-evidence",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusRetiring).
		Where("id = ?", replacement.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("move replacement to retiring: %v", err)
	}
	input := terminationTestInput(t, repos, replacement, storagereplacement.TerminationRoleSource)
	input.TxHash = "0xterminate"
	if err := repos.Replacements.RecordTerminationEvidence(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	pendingHead := int64(90)
	pendingGate, err := repos.Replacements.EvaluateRetirementGate(t.Context(), replacement.ID, &pendingHead)
	if err != nil || pendingGate.EpochReached || pendingGate.TerminationEpoch != nil {
		t.Fatalf("pending gate = %#v err=%v", pendingGate, err)
	}
	input.ConfirmedTxHash, input.ReceiptStatus = "0xreverted", new(uint64(0))
	if err := repos.Replacements.RecordTerminationEvidence(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	input.Epoch = new(int64(84))
	if err := repos.Replacements.RecordTerminationEvidence(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	input.Epoch = new(int64(85))
	if err := repos.Replacements.RecordTerminationEvidence(t.Context(), input); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("conflicting epoch error = %v", err)
	}

	stored, err := repos.Replacements.GetByID(t.Context(), replacement.ID)
	if err != nil || stored == nil || stored.TerminationEpoch == nil || *stored.TerminationEpoch != 84 {
		t.Fatalf("stored replacement = %#v err=%v, want termination epoch 84", stored, err)
	}
	if stored.TerminationReceiptStatus == nil || *stored.TerminationReceiptStatus != 0 || stored.TerminationConfirmedTxHash == nil || *stored.TerminationConfirmedTxHash != "0xreverted" {
		t.Fatalf("independently observed epoch changed the failed receipt: %#v", stored)
	}
	observed := int64(90)
	gate, err := repos.Replacements.EvaluateRetirementGate(t.Context(), replacement.ID, &observed)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.TerminationEpoch == nil || *gate.TerminationEpoch != 84 || !gate.EpochReached {
		t.Fatalf("retirement gate = %#v, want the recorded epoch 84 reached at 90", gate)
	}
	if slices.Contains(gate.Blockers, "termination_epoch") {
		t.Fatalf("retirement gate blockers = %v, want no epoch blocker", gate.Blockers)
	}
}

// abandonedTermination seeds a superseded replacement whose unused target is
// being retired by a newly bound task.
func abandonedTermination(t *testing.T, db *bun.DB, repos *repository.Repositories, name string) (*storagereplacement.Replacement, repository.RecordTerminationEvidenceInput) {
	t.Helper()
	ctx := t.Context()
	bucket := seedBucket(t, db, name)
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	markSourceReady(t, repos, source.ID)
	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: name,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("status = ?", storagereplacement.StatusSuperseded).Where("id = ?", replacement.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	evidence := terminationTestInput(t, repos, replacement, storagereplacement.TerminationRoleAbandonedTarget)
	evidence.AttemptNo = 1
	return replacement, evidence
}

// retirementSuccessor fails the current retirement owner and binds its retry.
func retirementSuccessor(t *testing.T, db *bun.DB, repos *repository.Repositories, evidence repository.RecordTerminationEvidenceInput) int64 {
	t.Helper()
	ctx := t.Context()
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
		Set("finished_at = ?", time.Now().UTC()).Where("id = ?", evidence.TaskID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	previous, err := repos.Tasks.GetByID(ctx, evidence.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	next := repositorySuccessor(t, repos, previous)
	if err := repos.Contents.TransferRetireTaskOwner(ctx, evidence.DataSetID, evidence.Generation, previous.ID, next.ID); err != nil {
		t.Fatal(err)
	}
	return next.ID
}

func TestTerminationAttemptAdmissionPreservesRevertedEvidence(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	replacement, evidence := abandonedTermination(t, db, repos, "termination-attempts")
	admission := repository.BeginTerminationAttemptInput{
		ReplacementID: evidence.ReplacementID, DataSetID: evidence.DataSetID, Generation: evidence.Generation,
		TaskID: evidence.TaskID, Role: evidence.Role, AttemptNo: 1, AttemptedAt: time.Now().UTC(),
	}
	start := make(chan struct{})
	admitted := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			admitted <- repos.Replacements.BeginTerminationAttempt(ctx, admission)
		}()
	}
	close(start)
	for range 2 {
		if err := <-admitted; err != nil {
			t.Fatalf("admit first request: %v", err)
		}
	}
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); err != nil {
		t.Fatalf("settle an admitted request without a returned hash: %v", err)
	}
	evidence.TxHash = "0xoriginal"
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	previousTaskID := evidence.TaskID
	nextID := retirementSuccessor(t, db, repos, evidence)
	admission.TaskID, admission.PreviousAttemptNo, admission.AttemptNo = nextID, 1, 2
	if err := repos.Replacements.BeginTerminationAttempt(ctx, admission); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("admit another request before a failed receipt: %v", err)
	}
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("record from a superseded owner: %v", err)
	}
	evidence.TaskID, evidence.ConfirmedTxHash, evidence.ReceiptStatus = nextID, "0xreverted", new(uint64(0))
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); err != nil {
		t.Fatalf("retain reverted receipt: %v", err)
	}
	for range 2 {
		if err := repos.Replacements.BeginTerminationAttempt(ctx, admission); err != nil {
			t.Fatalf("admit authorized successor request: %v", err)
		}
	}
	stored, err := repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.AbandonedTerminationAttemptNo != 2 || stored.AbandonedTerminationTxHash != nil || stored.AbandonedTerminationConfirmedTxHash != nil || stored.AbandonedTerminationReceiptStatus != nil {
		t.Fatalf("latest attempt projection = %#v, want the new admitted request without inherited evidence", stored)
	}
	evidence.AttemptNo, evidence.ConfirmedTxHash, evidence.ReceiptStatus = 2, "", nil
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("reusing a predecessor transaction: %v", err)
	}
	evidence.TxHash, evidence.ConfirmedTxHash, evidence.ReceiptStatus = "0xnext", "0xnext-confirmed", new(uint64(1))
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	conflict := evidence
	conflict.ReceiptStatus = new(uint64(0))
	if err := repos.Replacements.RecordTerminationEvidence(ctx, conflict); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("conflicting receipt outcome: %v", err)
	}
	evidence.Epoch = new(int64(84))
	if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	attempts, err := repos.Replacements.ListTerminationAttempts(ctx, []int64{replacement.ID})
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempt history = %#v, err=%v", attempts, err)
	}
	if attempts[0].AttemptNo != 1 || attempts[0].TxHash == nil || *attempts[0].TxHash != "0xoriginal" ||
		attempts[0].ConfirmedTxHash == nil || *attempts[0].ConfirmedTxHash != "0xreverted" || attempts[0].ReceiptStatus == nil || *attempts[0].ReceiptStatus != 0 {
		t.Fatalf("reverted request evidence = %#v", attempts[0])
	}
	if attempts[0].TaskID == nil || *attempts[0].TaskID != previousTaskID {
		t.Fatalf("first admission owner = %v, want %d", attempts[0].TaskID, previousTaskID)
	}
	if attempts[1].AttemptNo != 2 || attempts[1].TxHash == nil || *attempts[1].TxHash != "0xnext" ||
		attempts[1].ConfirmedTxHash == nil || *attempts[1].ConfirmedTxHash != "0xnext-confirmed" || attempts[1].ReceiptStatus == nil || *attempts[1].ReceiptStatus != 1 {
		t.Fatalf("successful request evidence = %#v", attempts[1])
	}
	if attempts[1].ReplacementID != replacement.ID || attempts[1].Role != evidence.Role {
		t.Fatalf("history subject = %#v", attempts[1])
	}
	if empty, err := repos.Replacements.ListTerminationAttempts(ctx, []int64{replacement.ID + 100}); err != nil || len(empty) != 0 {
		t.Fatalf("unrelated replacement history = %#v, err=%v", empty, err)
	}
	stored, err = repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || stored.AbandonedTerminationEpoch == nil || *stored.AbandonedTerminationEpoch != 84 || stored.AbandonedTerminationReceiptStatus == nil || *stored.AbandonedTerminationReceiptStatus != 1 {
		t.Fatalf("completed termination = %#v, err=%v", stored, err)
	}
	admission.PreviousAttemptNo, admission.AttemptNo = 2, 3
	if err := repos.Replacements.BeginTerminationAttempt(ctx, admission); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("admit a request after service termination: %v", err)
	}
}

// An attempt whose relayed request stayed unresolved passes to a successor only
// when it says an operator authorized completing it, and only while the attempt
// holds no transaction evidence. The attempt keeps its number in the history.
func TestUnresolvedTerminationAttemptIsResumedByAnAuthorizedSuccessor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		authorized bool
		txHash     string
		wantErr    error
	}{
		{name: "authorized successor", authorized: true},
		{name: "unauthorized successor", wantErr: repository.ErrConflict},
		{name: "relayed transaction known", authorized: true, txHash: "0xrelayed", wantErr: repository.ErrConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := t.Context()
			replacement, evidence := abandonedTermination(t, db, repos, "unresolved-relay")
			admission := repository.BeginTerminationAttemptInput{
				ReplacementID: evidence.ReplacementID, DataSetID: evidence.DataSetID, Generation: evidence.Generation,
				TaskID: evidence.TaskID, Role: evidence.Role, AttemptNo: 1, AttemptedAt: time.Now().UTC(),
			}
			if err := repos.Replacements.BeginTerminationAttempt(ctx, admission); err != nil {
				t.Fatal(err)
			}
			if tc.txHash != "" {
				evidence.TxHash = tc.txHash
				if err := repos.Replacements.RecordTerminationEvidence(ctx, evidence); err != nil {
					t.Fatal(err)
				}
			}
			firstTaskID := evidence.TaskID
			admission.TaskID = retirementSuccessor(t, db, repos, evidence)
			admission.PreviousAttemptNo, admission.ResumeUnresolved = 1, tc.authorized
			for range 2 {
				if err := repos.Replacements.BeginTerminationAttempt(ctx, admission); !errors.Is(err, tc.wantErr) {
					t.Fatalf("resume the unresolved attempt: %v, want %v", err, tc.wantErr)
				}
			}
			attempts, err := repos.Replacements.ListTerminationAttempts(ctx, []int64{replacement.ID})
			if err != nil || len(attempts) != 1 || attempts[0].AttemptNo != 1 || attempts[0].ReceiptStatus != nil || attempts[0].UnsentAt != nil {
				t.Fatalf("attempt history = %#v err=%v", attempts, err)
			}
			wantOwner := admission.TaskID
			if tc.wantErr != nil {
				wantOwner = firstTaskID
			}
			if attempts[0].TaskID == nil || *attempts[0].TaskID != wantOwner || (tc.txHash == "") != (attempts[0].TxHash == nil) {
				t.Fatalf("attempt = %#v, want owner %d", attempts[0], wantOwner)
			}
		})
	}
}

// A released admission sent nothing that can land, so its task or a successor
// admits the same attempt again. An admission with any evidence stays bound.
// The current owner may release an admission it inherited. A transaction that
// appears after a release reopens the admission with its evidence.
func TestReleasedTerminationAttemptIsAdmittedAgainInPlace(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	replacement, evidence := abandonedTermination(t, db, repos, "termination-release")
	admit := func(taskID int64) error {
		return repos.Replacements.BeginTerminationAttempt(ctx, repository.BeginTerminationAttemptInput{
			ReplacementID: evidence.ReplacementID, DataSetID: evidence.DataSetID, Generation: evidence.Generation,
			TaskID: taskID, Role: evidence.Role, AttemptNo: 1, PreviousAttemptNo: 1, AttemptedAt: time.Now().UTC(),
		})
	}
	attempt := func() storagereplacement.TerminationAttempt {
		t.Helper()
		attempts, err := repos.Replacements.ListTerminationAttempts(ctx, []int64{replacement.ID})
		if err != nil || len(attempts) != 1 {
			t.Fatalf("attempt history = %#v, err=%v", attempts, err)
		}
		return attempts[0]
	}
	if err := repos.Replacements.BeginTerminationAttempt(ctx, repository.BeginTerminationAttemptInput{
		ReplacementID: evidence.ReplacementID, DataSetID: evidence.DataSetID, Generation: evidence.Generation,
		TaskID: evidence.TaskID, Role: evidence.Role, AttemptNo: 1, AttemptedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := repos.Replacements.RecordTerminationUnsent(ctx, evidence); err != nil {
			t.Fatalf("release admission: %v", err)
		}
	}
	if released := attempt(); released.UnsentAt == nil {
		t.Fatalf("released attempt = %#v", released)
	}
	if err := admit(evidence.TaskID); err != nil {
		t.Fatalf("readmit by the same task: %v", err)
	}
	if reopened := attempt(); reopened.UnsentAt != nil || *reopened.TaskID != evidence.TaskID {
		t.Fatalf("reopened attempt = %#v", reopened)
	}
	evidence.TaskID = retirementSuccessor(t, db, repos, evidence)
	if err := repos.Replacements.RecordTerminationUnsent(ctx, evidence); err != nil {
		t.Fatalf("successor releases the inherited admission: %v", err)
	}
	if err := admit(evidence.TaskID); err != nil {
		t.Fatalf("readmit by a successor: %v", err)
	}
	if reopened := attempt(); reopened.UnsentAt != nil || *reopened.TaskID != evidence.TaskID {
		t.Fatalf("successor attempt = %#v", reopened)
	}
	if err := repos.Replacements.RecordTerminationUnsent(ctx, evidence); err != nil {
		t.Fatalf("release the successor's admission: %v", err)
	}
	sent := evidence
	sent.TxHash = "0xsent"
	if err := repos.Replacements.RecordTerminationEvidence(ctx, sent); err != nil {
		t.Fatalf("record a transaction for a released admission: %v", err)
	}
	if err := repos.Replacements.RecordTerminationUnsent(ctx, evidence); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("release an attempt with a transaction: %v", err)
	}
	if err := admit(retirementSuccessor(t, db, repos, evidence)); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("readmit an unreleased attempt by another task: %v", err)
	}
	if kept := attempt(); kept.UnsentAt != nil || kept.TxHash == nil || *kept.TxHash != "0xsent" {
		t.Fatalf("sent attempt = %#v", kept)
	}
}
