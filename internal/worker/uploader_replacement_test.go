package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
)

type replacementEnv struct {
	env       *testWorkerEnv
	bucket    *model.Bucket
	upload    *model.StorageUpload
	versionID string
	source    *model.StorageDataSet
	sourceCtx *fakeUploadContext
	targetCtx *fakeUploadContext
}

// seedReplacementEnv stores one object on a single replica slot and prepares
// fake contexts for both the retiring provider and its replacement.
func seedReplacementEnv(t *testing.T) *replacementEnv {
	t.Helper()
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	bucket, _, versionID := seedCachedObject(t, env)

	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: %v", err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	source := seedReadyBinding(t, env, bucket.ID, upload.ID, 0, "101", "1001")
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: source.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       onChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     testCID(t).String(),
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://source.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := env.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	sourceCtx := readyFakeUploadContext(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), sdktypes.NewBigInt(2001), testCID(t))
	targetCtx := newFakeUploadContext(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3002), testCID(t))
	env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		switch {
		case createContextDataSetIDEqual(opts, sdktypes.NewBigInt(1001)):
			return sourceCtx, nil
		case createContextDataSetIDEqual(opts, sdktypes.NewBigInt(2002)), createContextProviderIDEqual(opts, sdktypes.NewBigInt(202)):
			return targetCtx, nil
		}
		return nil, fmt.Errorf("unexpected CreateContext opts: %#v", opts)
	}
	return &replacementEnv{
		env: env, bucket: bucket, upload: upload, versionID: versionID,
		source: source, sourceCtx: sourceCtx, targetCtx: targetCtx,
	}
}

func (r *replacementEnv) authorize(t *testing.T, provider string) *storagereplacement.Replacement {
	t.Helper()
	row, _, err := r.env.repos.Replacements.Authorize(context.Background(), repository.AuthorizeReplacementInput{
		BucketID:         r.bucket.ID,
		SourceDataSetID:  r.source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, provider),
		ClientRequestID:  "worker-replacement-" + provider,
		MaxRetries:       5,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	return row
}

func (r *replacementEnv) coordinatorTask(t *testing.T, replacementID int64) *model.Task {
	t.Helper()
	task, err := r.env.repos.Tasks.GetByIdempotencyKey(context.Background(), storagereplacement.MigrateTaskKey(replacementID))
	if err != nil || task == nil {
		t.Fatalf("migration coordinator = %#v err=%v", task, err)
	}
	return task
}

func (r *replacementEnv) abandonedTargetTask(t *testing.T, replacementID int64) *model.Task {
	t.Helper()
	task, err := r.env.repos.Tasks.GetByIdempotencyKey(context.Background(), storagereplacement.AbandonedTargetTaskKey(replacementID))
	if err != nil || task == nil {
		t.Fatalf("abandoned-target cleanup = %#v err=%v, want it queued with the later confirmation", task, err)
	}
	return task
}

// runUploaderUntil drives the uploader until cond holds. Waiting and retrying
// both leave the task in an active status, which runWorkerUntilTask never
// returns on.
func (r *replacementEnv) runUploaderUntil(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader := r.newUploader()
	replacementWorker := r.newProviderReplacementWorker(uploader)
	done := make(chan struct{}, 2)
	go func() {
		_ = uploader.Run(ctx)
		done <- struct{}{}
	}()
	go func() {
		_ = replacementWorker.Run(ctx)
		done <- struct{}{}
	}()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			cancel()
			<-done
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	<-done
	t.Fatal("uploader did not reach the expected state before the timeout")
}

func (r *replacementEnv) newUploader() *worker.Uploader {
	return worker.NewUploader(r.env.repos, r.env.cache, r.env.storage, nil, r.env.sm,
		cache.EvictionPolicyAfterUpload, 1, 1, 10*time.Millisecond, slog.Default(),
		worker.WithProviderReplacementMaxRetries(5))
}

func (r *replacementEnv) newProviderReplacementWorker(uploader *worker.Uploader) *worker.ProviderReplacementWorker {
	return worker.NewProviderReplacementWorker(r.env.repos, uploader, 4, 10*time.Millisecond, slog.Default())
}

func (r *replacementEnv) runReplacementUntilTask(t *testing.T, taskID int64, timeout time.Duration) *model.Task {
	t.Helper()
	var final *model.Task
	r.runUploaderUntil(t, func() bool {
		task, err := r.env.repos.Tasks.GetByID(context.Background(), taskID)
		if err != nil || task == nil {
			return false
		}
		if task.Status == model.TaskStatusQueued || task.Status == model.TaskStatusScheduled ||
			task.Status == model.TaskStatusWaiting || task.Status == model.TaskStatusRunning {
			return false
		}
		final = task
		return true
	}, timeout)
	return final
}

// The whole approved flow: prepare the new service, switch the slot, copy the
// stored content across, and hand over to retirement.
func TestUploader_ReplacementPreparesActivatesAndMigrates(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	row := fixture.authorize(t, "202")
	task := fixture.coordinatorTask(t, row.ID)

	final := fixture.runReplacementUntilTask(t, task.ID, 20*time.Second)
	if final == nil || final.Status != model.TaskStatusCompleted {
		t.Fatalf("coordinator task = %#v, want completed", final)
	}

	current, err := fixture.env.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, fixture.bucket.ID, 0)
	if err != nil || current == nil || current.ID != row.TargetDataSetID {
		t.Fatalf("current binding = %#v err=%v, want the replacement target", current, err)
	}
	source, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || source.IsCurrent || source.Status != model.StorageDataSetStatusDraining {
		t.Fatalf("source = %#v err=%v, want draining and not current", source, err)
	}

	// The content must actually exist on the new provider, pulled rather than
	// re-uploaded from this node.
	targetCopy, err := fixture.env.repos.Uploads.GetUploadCopyForDataSet(ctx, fixture.upload.ID, row.TargetDataSetID)
	if err != nil || targetCopy == nil || targetCopy.Status != model.StorageUploadCopyStatusCommitted {
		t.Fatalf("target copy = %#v err=%v, want committed", targetCopy, err)
	}
	if fixture.targetCtx.pullCalls.Load() == 0 {
		t.Fatal("migration did not pull from a remote replica")
	}
	if fixture.targetCtx.storeCalls.Load() != 0 {
		t.Fatal("migration uploaded from local cache while a readable replica existed")
	}

	got, err := fixture.env.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || got == nil || got.Status != storagereplacement.StatusRetiring {
		t.Fatalf("replacement = %#v err=%v, want retiring", got, err)
	}
	if got.ItemsCopied != got.ItemsTotal || got.ItemsTotal != 1 {
		t.Fatalf("progress = %d/%d, want 1/1", got.ItemsCopied, got.ItemsTotal)
	}
	retire, err := fixture.env.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.RetireTaskKey(row.ID))
	if err != nil || retire == nil {
		t.Fatalf("retirement coordinator = %#v err=%v, want it queued", retire, err)
	}
}

// With no readable replica and no retained cache data the item is parked and
// the replacement waits, without burning retry budget.
func TestUploader_ReplacementWaitsWhenNoSourceIsReadable(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	row := fixture.authorize(t, "202")
	task := fixture.coordinatorTask(t, row.ID)

	// The retiring provider can no longer serve reads and the cached copy is
	// gone, so this content has nowhere to come from.
	fixture.targetCtx.pullErr = &synapse.ProviderUnavailableError{Cause: errors.New("source provider unreachable")}
	if err := fixture.env.cache.Delete(ctx, fixture.bucket.Name, ".versions/"+fixture.versionID); err != nil {
		t.Fatalf("cache delete: %v", err)
	}

	fixture.runUploaderUntil(t, func() bool {
		got, err := fixture.env.repos.Replacements.GetByID(ctx, row.ID)
		return err == nil && got != nil && got.Status == storagereplacement.StatusWaiting
	}, 20*time.Second)

	final, err := fixture.env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || final == nil {
		t.Fatalf("GetByID task: %#v err=%v", final, err)
	}
	if final.RetryCount != 0 {
		t.Fatalf("retry count = %d, want waiting to leave the retry budget untouched", final.RetryCount)
	}
	got, err := fixture.env.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: %#v err=%v", got, err)
	}
	if got.WaitReason == nil || *got.WaitReason != storagereplacement.WaitReasonReadableSource {
		t.Fatalf("wait reason = %v, want readable_source", got.WaitReason)
	}
	if got.LastError != nil {
		t.Fatalf("last_error = %v, want waiting to record no failure", *got.LastError)
	}
	var item storagereplacement.Item
	if err := fixture.env.db.NewSelect().Model(&item).
		Where("replacement_id = ?", row.ID).
		Scan(ctx); err != nil || item.Status != storagereplacement.ItemStatusWaitingSource {
		t.Fatalf("parked item = %#v err=%v, want waiting_source", item, err)
	}
}

func TestUploader_ReplacementWaitsForFundingBeforeCreatingTheService(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	row := fixture.authorize(t, "202")
	task := fixture.coordinatorTask(t, row.ID)
	var createCalls atomic.Int32
	fixture.targetCtx.createCalls = &createCalls
	fixture.env.storage.PrepareUploadFunc = func(context.Context, uint64, []synapse.UploadContext) (*storage.MultiContextCosts, error) {
		return &storage.MultiContextCosts{Ready: false}, nil
	}

	fixture.runUploaderUntil(t, func() bool {
		got, err := fixture.env.repos.Replacements.GetByID(ctx, row.ID)
		return err == nil && got != nil && got.Status == storagereplacement.StatusWaiting
	}, 20*time.Second)

	final, err := fixture.env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || final == nil {
		t.Fatalf("GetByID task: %#v err=%v", final, err)
	}
	if final.RetryCount != 0 {
		t.Fatalf("retry count = %d, want funding wait to leave the retry budget untouched", final.RetryCount)
	}
	got, err := fixture.env.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: %#v err=%v", got, err)
	}
	if got.WaitReason == nil || *got.WaitReason != storagereplacement.WaitReasonFunding {
		t.Fatalf("wait reason = %v, want funding", got.WaitReason)
	}
	if createCalls.Load() != 0 {
		t.Fatalf("CreateDataSet calls = %d, want none while funding is not ready", createCalls.Load())
	}
}

// A coordinator that cannot make progress retries the task, but must leave the
// replacement record describing reality rather than inventing a state.
func TestUploader_ReplacementRetriesWithoutRewritingTheRecord(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	row := fixture.authorize(t, "202")
	task := fixture.coordinatorTask(t, row.ID)

	// Put the approved target into a state no preparation step can advance.
	if _, err := fixture.env.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusRetired).
		Where("id = ?", row.TargetDataSetID).
		Exec(ctx); err != nil {
		t.Fatalf("retire target: %v", err)
	}

	fixture.runUploaderUntil(t, func() bool {
		got, err := fixture.env.repos.Tasks.GetByID(ctx, task.ID)
		return err == nil && got != nil && (got.RetryCount > 0 || got.Status == model.TaskStatusFailed)
	}, 20*time.Second)

	got, err := fixture.env.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: %#v err=%v", got, err)
	}
	if got.Status != storagereplacement.StatusPreparingTarget {
		t.Fatalf("replacement status = %s, want it untouched at preparing_target", got.Status)
	}
}

// retirementFixture runs migration to completion so retirement has a realistic
// starting point: a drained source and a target that already holds the data.
type retirementFixture struct {
	*replacementEnv
	replacement *storagereplacement.Replacement
	terminator  *testutil.MockServiceTerminator
	epochs      *testutil.MockChainEpochReader
	epoch       atomic.Int64
	terminated  atomic.Int32
}

func seedRetirementFixture(t *testing.T) *retirementFixture {
	t.Helper()
	base := seedReplacementEnv(t)
	row := base.authorize(t, "202")
	migrate := base.coordinatorTask(t, row.ID)
	if final := base.runReplacementUntilTask(t, migrate.ID, 20*time.Second); final == nil ||
		final.Status != model.TaskStatusCompleted {
		t.Fatalf("migration coordinator = %#v, want completed", final)
	}

	fixture := &retirementFixture{replacementEnv: base, replacement: row}
	fixture.epoch.Store(1000)
	fixture.terminator = &testutil.MockServiceTerminator{
		TerminateServiceFunc: func(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
			fixture.terminated.Add(1)
			return &synapse.TerminationResult{TxHash: "0xterminate", EndEpoch: fixture.epoch.Load() + 5}, nil
		},
	}
	fixture.epochs = &testutil.MockChainEpochReader{
		CurrentEpochFunc: func(context.Context) (int64, error) { return fixture.epoch.Load(), nil },
	}
	return fixture
}

func (f *retirementFixture) newCleanupWorker() *worker.StorageCleanupWorker {
	return worker.NewStorageCleanupWorker(f.env.repos, f.env.storage, 1, 10*time.Millisecond, slog.Default(),
		worker.WithServiceTermination(f.terminator, f.epochs))
}

func (f *retirementFixture) runCleanupUntil(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	w := f.newCleanupWorker()
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("storage cleanup did not reach the expected state before the timeout")
}

// releaseWaitingTasks brings a parked task's schedule forward so a test does not
// have to sleep through the real wait interval.
func (f *retirementFixture) releaseWaitingTasks(t *testing.T) {
	t.Helper()
	if _, err := f.env.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now().Add(-time.Second)).
		Where("status = ?", model.TaskStatusWaiting).
		Exec(context.Background()); err != nil {
		t.Fatalf("release waiting tasks: %v", err)
	}
}

func (f *retirementFixture) reload(t *testing.T) *storagereplacement.Replacement {
	t.Helper()
	got, err := f.env.repos.Replacements.GetByID(context.Background(), f.replacement.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID: %#v err=%v", got, err)
	}
	return got
}

// The old service is only treated as gone once the chain has actually reached
// the epoch it ends at.
func TestStorageCleanup_RetirementWaitsForTheTerminationEpoch(t *testing.T) {
	fixture := seedRetirementFixture(t)
	ctx := context.Background()

	fixture.runCleanupUntil(t, func() bool {
		got, err := fixture.env.repos.Replacements.GetByID(ctx, fixture.replacement.ID)
		return err == nil && got != nil && got.WaitReason != nil &&
			*got.WaitReason == storagereplacement.WaitReasonTerminationEpoch
	}, 20*time.Second)

	waiting := fixture.reload(t)
	if waiting.Status != storagereplacement.StatusWaiting {
		t.Fatalf("status = %s, want waiting for the end of term", waiting.Status)
	}
	if waiting.TerminationEpoch == nil {
		t.Fatal("termination epoch was not recorded before waiting for it")
	}
	source, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || source.Status == model.StorageDataSetStatusRetired {
		t.Fatalf("source = %#v err=%v, want it not retired before the epoch is reached", source, err)
	}

	// Let the chain catch up. The task is parked for the epoch re-check delay,
	// so bring it forward rather than sleeping through it.
	fixture.epoch.Store(*waiting.TerminationEpoch)
	fixture.releaseWaitingTasks(t)
	fixture.runCleanupUntil(t, func() bool {
		got, err := fixture.env.repos.Replacements.GetByID(ctx, fixture.replacement.ID)
		return err == nil && got != nil && got.Status == storagereplacement.StatusCompleted
	}, 20*time.Second)

	done := fixture.reload(t)
	if done.TerminationObservedAt == nil {
		t.Fatal("completed replacement recorded no observation time")
	}
	source, err = fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || source.Status != model.StorageDataSetStatusRetired || source.IsCurrent {
		t.Fatalf("source = %#v err=%v, want retired", source, err)
	}
	if got := fixture.terminated.Load(); got != 1 {
		t.Fatalf("termination calls = %d, want the service terminated exactly once", got)
	}
}

// A version the new provider cannot serve must keep the old one alive.
func TestStorageCleanup_RetirementRefusesWhileCoverageIsIncomplete(t *testing.T) {
	fixture := seedRetirementFixture(t)
	ctx := context.Background()

	// Take the migrated copy away, leaving the retained version with no home on
	// the new provider.
	if _, err := fixture.env.db.NewUpdate().
		Model((*model.StorageUploadCopy)(nil)).
		Set("status = ?", model.StorageUploadCopyStatusFailed).
		Where("storage_data_set_id = ?", fixture.replacement.TargetDataSetID).
		Exec(ctx); err != nil {
		t.Fatalf("break target coverage: %v", err)
	}

	fixture.runCleanupUntil(t, func() bool {
		got, err := fixture.env.repos.Replacements.GetByID(ctx, fixture.replacement.ID)
		return err == nil && got != nil && got.WaitReason != nil
	}, 20*time.Second)

	got := fixture.reload(t)
	if got.Status != storagereplacement.StatusWaiting {
		t.Fatalf("status = %s, want waiting", got.Status)
	}
	if *got.WaitReason != storagereplacement.WaitReasonCoverage {
		t.Fatalf("wait reason = %s, want coverage", *got.WaitReason)
	}
	if fixture.terminated.Load() != 0 {
		t.Fatal("terminated the old service while a version was still uncovered")
	}
	source, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || source.Status == model.StorageDataSetStatusRetired {
		t.Fatalf("source = %#v err=%v, want it kept alive", source, err)
	}
}

// Settling payment debt is the operator's call. The replacement must stop and
// say so instead of retrying, and the two facts must be recorded together.
func TestStorageCleanup_RetirementRaisesAttentionOnPaymentDebt(t *testing.T) {
	fixture := seedRetirementFixture(t)
	ctx := context.Background()
	fixture.terminator.TerminateServiceFunc = func(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
		fixture.terminated.Add(1)
		return nil, &synapse.TerminationBlockedError{Reason: "payment_debt", Shortfall: big.NewInt(500)}
	}

	fixture.runCleanupUntil(t, func() bool {
		got, err := fixture.env.repos.Replacements.GetByID(ctx, fixture.replacement.ID)
		return err == nil && got != nil && got.Status == storagereplacement.StatusCleanupAttention
	}, 20*time.Second)

	got := fixture.reload(t)
	if got.LastError == nil || !strings.Contains(*got.LastError, "payment_debt") {
		t.Fatalf("last_error = %v, want it to name the payment debt", got.LastError)
	}
	retire, err := fixture.env.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.RetireTaskKey(fixture.replacement.ID))
	if err != nil || retire == nil {
		t.Fatalf("retirement task = %#v err=%v", retire, err)
	}
	if retire.Status != model.TaskStatusFailed {
		t.Fatalf("retirement task status = %s, want it stopped rather than retrying", retire.Status)
	}
	source, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || source.Status == model.StorageDataSetStatusRetired {
		t.Fatalf("source = %#v err=%v, want it kept alive", source, err)
	}
}

// The abandoned target coordinator existed but was never dispatched: the
// cleanup worker only routed the source retirement stage, so the task fell into
// ordinary replica cleanup, found nothing, and completed without ending the
// paid service it was created to end.
func TestStorageCleanup_AbandonedTargetIsDispatchedAndTerminated(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	first := fixture.authorize(t, "202")

	// Bring the first target's service into existence, then supersede it.
	if err := fixture.env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        first.TargetDataSetID,
		DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	second := fixture.authorize(t, "303")
	if second.ID == first.ID {
		t.Fatal("the later confirmation reused the record")
	}

	var terminated atomic.Int64
	terminator := &testutil.MockServiceTerminator{
		TerminateServiceFunc: func(_ context.Context, dataSetID sdktypes.BigInt) (*synapse.TerminationResult, error) {
			if dataSetID.String() != "2002" {
				t.Errorf("terminated data set %s, want the abandoned target 2002", dataSetID.String())
			}
			terminated.Add(1)
			return &synapse.TerminationResult{TxHash: "0xabandon", EndEpoch: 1000}, nil
		},
	}
	epochs := &testutil.MockChainEpochReader{
		CurrentEpochFunc: func(context.Context) (int64, error) { return 5000, nil },
	}
	task := fixture.abandonedTargetTask(t, first.ID)

	worker := worker.NewStorageCleanupWorker(fixture.env.repos, fixture.env.storage, 1, 10*time.Millisecond,
		slog.Default(), worker.WithServiceTermination(terminator, epochs))
	runWorkerUntilTask(t, fixture.env, worker, task.ID, 20*time.Second)

	if terminated.Load() != 1 {
		t.Fatalf("termination calls = %d, want the abandoned service ended exactly once", terminated.Load())
	}
	abandoned, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, first.TargetDataSetID)
	if err != nil || abandoned == nil || abandoned.Status != model.StorageDataSetStatusRetired {
		t.Fatalf("abandoned target = %#v err=%v, want retired", abandoned, err)
	}
	source, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || !source.IsCurrent || source.Status == model.StorageDataSetStatusRetired {
		t.Fatalf("source = %#v err=%v, want it untouched and still current", source, err)
	}
}

func TestStorageCleanup_AbandonedTargetRestartObservesStoredTermination(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	first := fixture.authorize(t, "202")
	if err := fixture.env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: first.TargetDataSetID, DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	fixture.authorize(t, "303")

	var terminationCalls atomic.Int64
	terminator := &testutil.MockServiceTerminator{
		TerminateServiceFunc: func(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
			terminationCalls.Add(1)
			return &synapse.TerminationResult{TxHash: "0xpersisted", EndEpoch: 5000}, nil
		},
	}
	var observedEpoch atomic.Int64
	observedEpoch.Store(1000)
	epochs := &testutil.MockChainEpochReader{
		CurrentEpochFunc: func(context.Context) (int64, error) { return observedEpoch.Load(), nil },
	}
	task := fixture.abandonedTargetTask(t, first.ID)
	cleanup := worker.NewStorageCleanupWorker(fixture.env.repos, fixture.env.storage, 1, 10*time.Millisecond,
		slog.Default(), worker.WithServiceTermination(terminator, epochs))
	runWorkerUntilTaskStatus(t, fixture.env, cleanup, task.ID, model.TaskStatusWaiting, 20*time.Second)
	if terminationCalls.Load() != 1 {
		t.Fatalf("termination calls before restart = %d, want 1", terminationCalls.Load())
	}
	replacement, err := fixture.env.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || replacement == nil || replacement.AbandonedTerminationEpoch == nil ||
		*replacement.AbandonedTerminationEpoch != 5000 {
		t.Fatalf("stored abandoned termination = %#v err=%v, want epoch 5000", replacement, err)
	}

	observedEpoch.Store(6000)
	if _, err := fixture.env.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now()).Where("id = ?", task.ID).Exec(ctx); err != nil {
		t.Fatalf("release epoch wait: %v", err)
	}
	cleanup = worker.NewStorageCleanupWorker(fixture.env.repos, fixture.env.storage, 1, 10*time.Millisecond,
		slog.Default(), worker.WithServiceTermination(terminator, epochs))
	runWorkerUntilTaskStatus(t, fixture.env, cleanup, task.ID, model.TaskStatusCompleted, 20*time.Second)
	if terminationCalls.Load() != 1 {
		t.Fatalf("termination calls after restart = %d, want the stored epoch to prevent a second call", terminationCalls.Load())
	}
	replacement, err = fixture.env.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || replacement == nil || replacement.AbandonedTerminationObservedAt == nil {
		t.Fatalf("observed abandoned termination = %#v err=%v", replacement, err)
	}
}

func TestStorageCleanup_AbandonedTargetWaitsOnPaymentDebtThenRetires(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	first := fixture.authorize(t, "202")
	if err := fixture.env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        first.TargetDataSetID,
		DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if second := fixture.authorize(t, "303"); second.ID == first.ID {
		t.Fatal("the later confirmation reused the record")
	}

	var blocked atomic.Bool
	blocked.Store(true)
	var terminated atomic.Int64
	terminator := &testutil.MockServiceTerminator{
		TerminateServiceFunc: func(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
			terminated.Add(1)
			if blocked.Load() {
				return nil, &synapse.TerminationBlockedError{Reason: "payment_debt", Shortfall: big.NewInt(500)}
			}
			return &synapse.TerminationResult{TxHash: "0xabandon", EndEpoch: 1000}, nil
		},
	}
	epochs := &testutil.MockChainEpochReader{
		CurrentEpochFunc: func(context.Context) (int64, error) { return 5000, nil },
	}
	task := fixture.abandonedTargetTask(t, first.ID)

	cleanup := worker.NewStorageCleanupWorker(fixture.env.repos, fixture.env.storage, 1, 10*time.Millisecond,
		slog.Default(), worker.WithServiceTermination(terminator, epochs))
	runWorkerUntilTaskStatus(t, fixture.env, cleanup, task.ID, model.TaskStatusWaiting, 20*time.Second)

	got, err := fixture.env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || got == nil || got.Status != model.TaskStatusWaiting {
		t.Fatalf("task = %#v err=%v, want waiting so cleanup can resume after the debt is settled", got, err)
	}
	abandoned, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, first.TargetDataSetID)
	if err != nil || abandoned == nil || abandoned.Status == model.StorageDataSetStatusRetired {
		t.Fatalf("abandoned target = %#v err=%v, want it kept until termination succeeds", abandoned, err)
	}

	blocked.Store(false)
	if _, err := fixture.env.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("scheduled_at = ?", time.Now()).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("release wait schedule: %v", err)
	}
	cleanup = worker.NewStorageCleanupWorker(fixture.env.repos, fixture.env.storage, 1, 10*time.Millisecond,
		slog.Default(), worker.WithServiceTermination(terminator, epochs))
	runWorkerUntilTaskStatus(t, fixture.env, cleanup, task.ID, model.TaskStatusCompleted, 20*time.Second)

	abandoned, err = fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, first.TargetDataSetID)
	if err != nil || abandoned == nil || abandoned.Status != model.StorageDataSetStatusRetired {
		t.Fatalf("abandoned target = %#v err=%v, want retired after the debt cleared", abandoned, err)
	}
	if terminated.Load() < 2 {
		t.Fatalf("termination calls = %d, want a blocked attempt and a successful one", terminated.Load())
	}
}

func TestStorageCleanup_AbandonedTargetWithoutOnChainServiceReleasesTheProvider(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxRetries int
		prepare    func(*testing.T, *replacementEnv, *storagereplacement.Replacement)
	}{
		{
			name:       "service was never created",
			maxRetries: 5,
		},
		{
			name:       "creation observation exhausts",
			maxRetries: 1,
			prepare: func(t *testing.T, fixture *replacementEnv, first *storagereplacement.Replacement) {
				t.Helper()
				clientDataSetID := onChainIDPtr(t, "9202")
				if err := fixture.env.repos.Uploads.MarkDataSetCreating(context.Background(), repository.MarkDataSetCreatingInput{
					ID:              first.TargetDataSetID,
					TransactionID:   "0xabandoned-create",
					StatusURL:       "https://provider-202.example/status/create",
					ClientDataSetID: clientDataSetID,
				}); err != nil {
					t.Fatalf("MarkDataSetCreating: %v", err)
				}
				fixture.targetCtx.waitErr = errors.New("rpc timeout")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := seedReplacementEnv(t)
			ctx := context.Background()
			first := fixture.authorize(t, "202")
			if tc.prepare != nil {
				tc.prepare(t, fixture, first)
			}
			second := fixture.authorize(t, "303")
			if second.ID == first.ID {
				t.Fatal("the later confirmation reused the record")
			}

			var terminated atomic.Int64
			terminator := &testutil.MockServiceTerminator{
				TerminateServiceFunc: func(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, error) {
					terminated.Add(1)
					return &synapse.TerminationResult{TxHash: "0xabandon", EndEpoch: 1000}, nil
				},
			}
			epochs := &testutil.MockChainEpochReader{
				CurrentEpochFunc: func(context.Context) (int64, error) { return 5000, nil },
			}
			task := fixture.abandonedTargetTask(t, first.ID)
			if tc.maxRetries != task.MaxRetries {
				if _, err := fixture.env.db.NewUpdate().
					Model((*model.Task)(nil)).
					Set("max_retries = ?", tc.maxRetries).
					Where("id = ?", task.ID).
					Exec(ctx); err != nil {
					t.Fatalf("set abandoned-target max retries: %v", err)
				}
				task.MaxRetries = tc.maxRetries
			}

			cleanup := worker.NewStorageCleanupWorker(fixture.env.repos, fixture.env.storage, 1, 10*time.Millisecond,
				slog.Default(), worker.WithServiceTermination(terminator, epochs))
			final := runWorkerUntilTask(t, fixture.env, cleanup, task.ID, 20*time.Second)
			if final == nil {
				t.Fatal("abandoned target task was not processed")
			}

			if terminated.Load() != 0 {
				t.Fatalf("termination calls = %d, want none when no on-chain service exists", terminated.Load())
			}
			abandoned, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, first.TargetDataSetID)
			if err != nil || abandoned == nil || abandoned.Status != model.StorageDataSetStatusRetired {
				t.Fatalf("abandoned target = %#v err=%v, want retired", abandoned, err)
			}
			if _, _, err := fixture.env.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
				BucketID:         fixture.bucket.ID,
				SourceDataSetID:  fixture.source.ID,
				SelectionMode:    storagereplacement.SelectionModeManual,
				TargetProviderID: onChainID(t, "202"),
				ClientRequestID:  "worker-reuse-202",
				MaxRetries:       5,
			}); err != nil {
				t.Fatalf("reusing the released provider: %v", err)
			}
		})
	}
}

// Parked work is only reached once nothing executable is left. Re-queueing the
// coordinator there spins it against the same item at queue-tail rate and
// starves ordinary uploads, so it has to wait for the source instead.
func TestUploader_OnlyParkedItemsPutTheCoordinatorIntoAWait(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	replacement := fixture.authorize(t, "202")
	if err := fixture.env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        replacement.TargetDataSetID,
		DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}

	// The retiring generation is still writing this content, so there is
	// nothing to copy from yet and nothing else to do.
	if _, err := fixture.env.db.NewUpdate().
		Model((*model.StorageUploadCopy)(nil)).
		Set("status = ?", model.StorageUploadCopyStatusPending).
		Where("upload_id = ? AND storage_data_set_id = ?", fixture.upload.ID, fixture.source.ID).
		Exec(ctx); err != nil {
		t.Fatalf("put the source copy back in flight: %v", err)
	}

	task := fixture.coordinatorTask(t, replacement.ID)
	fixture.runUploaderUntil(t, func() bool {
		row, err := fixture.env.repos.Replacements.GetByID(ctx, replacement.ID)
		return err == nil && row != nil && row.Status == storagereplacement.StatusWaiting
	}, 20*time.Second)

	row, err := fixture.env.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || row == nil {
		t.Fatalf("GetByID = %#v err=%v", row, err)
	}
	if row.WaitReason == nil || *row.WaitReason != storagereplacement.WaitReasonReadableSource {
		t.Fatalf("wait reason = %v, want readable_source", row.WaitReason)
	}
	settled, err := fixture.env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || settled == nil {
		t.Fatalf("GetByID task = %#v err=%v", settled, err)
	}
	// Waiting must not burn the retry budget; the item is rechecked next tick.
	if settled.RetryCount != 0 {
		t.Fatalf("retry count = %d, want waiting to leave the budget untouched", settled.RetryCount)
	}
	if settled.Status != model.TaskStatusWaiting {
		t.Fatalf("task status = %s, want waiting", settled.Status)
	}
	if settled.ScheduledAt.Before(time.Now()) {
		t.Fatal("the coordinator was re-queued immediately instead of waiting for the source")
	}
}

type cancelReplacementWaitHook struct {
	cancel context.CancelFunc
	fired  atomic.Bool
}

func (h *cancelReplacementWaitHook) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *cancelReplacementWaitHook) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	query := strings.ToLower(event.Query)
	if event.Err != nil || event.Operation() != "UPDATE" ||
		!strings.Contains(query, "storage_replacements") ||
		!strings.Contains(query, "wait_reason") || !strings.Contains(query, "waiting") {
		return
	}
	if h.fired.CompareAndSwap(false, true) {
		h.cancel()
	}
}

func TestUploader_ReplacementWaitCancellationDoesNotPartiallyPauseLifecycle(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replacement := fixture.authorize(t, "202")
	if err := fixture.env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        replacement.TargetDataSetID,
		DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}

	if _, err := fixture.env.db.NewUpdate().
		Model((*model.StorageUploadCopy)(nil)).
		Set("status = ?", model.StorageUploadCopyStatusPending).
		Where("upload_id = ? AND storage_data_set_id = ?", fixture.upload.ID, fixture.source.ID).
		Exec(ctx); err != nil {
		t.Fatalf("put the source copy back in flight: %v", err)
	}

	task := fixture.coordinatorTask(t, replacement.ID)
	hook := &cancelReplacementWaitHook{cancel: cancel}
	fixture.env.db.AddQueryHook(hook)
	uploader := fixture.newUploader()
	replacementWorker := fixture.newProviderReplacementWorker(uploader)
	done := make(chan struct{}, 2)
	go func() {
		_ = uploader.Run(ctx)
		done <- struct{}{}
	}()
	go func() {
		_ = replacementWorker.Run(ctx)
		done <- struct{}{}
	}()

	select {
	case <-done:
		<-done
	case <-time.After(20 * time.Second):
		cancel()
		<-done
		<-done
		t.Fatal("uploader did not reach the replacement wait before the timeout")
	}
	if !hook.fired.Load() {
		t.Fatal("replacement wait cancellation hook did not run")
	}

	row, err := fixture.env.repos.Replacements.GetByID(context.Background(), replacement.ID)
	if err != nil || row == nil {
		t.Fatalf("GetByID replacement = %#v err=%v", row, err)
	}
	if row.Status == storagereplacement.StatusWaiting || row.WaitReason != nil {
		t.Fatalf("replacement = %#v, want the cancelled wait rolled back", row)
	}
	settled, err := fixture.env.repos.Tasks.GetByID(context.Background(), task.ID)
	if err != nil || settled == nil {
		t.Fatalf("GetByID task = %#v err=%v", settled, err)
	}
	terminal := settled.Status == model.TaskStatusCompleted || settled.Status == model.TaskStatusFailed ||
		settled.Status == model.TaskStatusExhausted || settled.Status == model.TaskStatusCancelled
	if settled.RetryCount != 0 || terminal {
		t.Fatalf("task = %#v, want an active coordinator with retry budget untouched", settled)
	}
}

// A replacement has to open its own paid service. If the chosen provider still
// runs a live service for this bucket, the SDK hands back a context already
// bound to it -- and attaching there would leave the replacement paying for,
// and later retiring, a service it does not own.
//
// The fixture's target context is deliberately unbound, so this branch was
// never reached by any existing test.
func TestUploader_ReplacementNeverAttachesToAnExistingService(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()

	// The target provider already runs a data set carrying this bucket's
	// metadata: a generation released locally without being terminated on chain.
	var createCalls atomic.Int32
	existing := readyFakeUploadContext(
		sdktypes.NewBigInt(202), sdktypes.NewBigInt(9002), sdktypes.NewBigInt(3002), testCID(t))
	existing.createCalls = &createCalls
	fixture.env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(202)) {
			return existing, nil
		}
		return fixture.sourceCtx, nil
	}

	replacement := fixture.authorize(t, "202")
	task := fixture.coordinatorTask(t, replacement.ID)
	fixture.runUploaderUntil(t, func() bool {
		row, err := fixture.env.repos.Replacements.GetByID(ctx, replacement.ID)
		return err == nil && row != nil && row.Status == storagereplacement.StatusFailed
	}, 20*time.Second)

	if calls := createCalls.Load(); calls != 0 {
		t.Fatalf("CreateDataSet calls = %d, want the replacement to refuse rather than reuse", calls)
	}
	target, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("GetDataSetBindingByID = %#v err=%v", target, err)
	}
	if target.DataSetID != nil {
		t.Fatalf("target adopted data set %s, want no service recorded", target.DataSetID.String())
	}
	if target.Status == model.StorageDataSetStatusReady {
		t.Fatal("target was marked ready without a service of its own")
	}

	row, err := fixture.env.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || row == nil || row.LastError == nil {
		t.Fatalf("replacement = %#v err=%v, want a recorded reason", row, err)
	}
	// The operator has to be able to act on it: name the provider and the service.
	if !strings.Contains(*row.LastError, "202") || !strings.Contains(*row.LastError, "9002") {
		t.Fatalf("last error = %q, want the provider and data set named", *row.LastError)
	}

	// The source still owns the replica, so a new confirmation is the way out.
	source, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, fixture.source.ID)
	if err != nil || source == nil || !source.IsCurrent {
		t.Fatalf("source = %#v err=%v, want it to still own the replica", source, err)
	}
	// Retrying cannot make a taken provider free, so the budget is not spent.
	settled, err := fixture.env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || settled == nil {
		t.Fatalf("GetByID task = %#v err=%v", settled, err)
	}
	if settled.RetryCount != 0 {
		t.Fatalf("retry count = %d, want no retries spent on an unusable provider", settled.RetryCount)
	}
	if settled.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want failed", settled.Status)
	}
}

// Refusing a bound context must not refuse this replacement's own service. Once
// a creation has been submitted, the recorded transaction is what identifies
// the service -- and by then the SDK resolves a bound context for it, which is
// exactly the state a crash between submitting and recording leaves behind.
func TestUploader_ReplacementResumesItsOwnSubmittedCreation(t *testing.T) {
	fixture := seedReplacementEnv(t)
	ctx := context.Background()
	replacement := fixture.authorize(t, "202")

	if err := fixture.env.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
		ID:              replacement.TargetDataSetID,
		TransactionID:   "0xcreate2002",
		StatusURL:       "https://provider-202.example/status/create",
		ClientDataSetID: onChainIDPtr(t, "12002"),
	}); err != nil {
		t.Fatalf("MarkDataSetCreating: %v", err)
	}
	// The service this replacement created is now visible to the resolver.
	bound := readyFakeUploadContext(
		sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), sdktypes.NewBigInt(3002), testCID(t))
	fixture.env.storage.CreateContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
		if createContextProviderIDEqual(opts, sdktypes.NewBigInt(202)) ||
			createContextDataSetIDEqual(opts, sdktypes.NewBigInt(2002)) {
			return bound, nil
		}
		return fixture.sourceCtx, nil
	}

	fixture.runUploaderUntil(t, func() bool {
		target, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
		return err == nil && target != nil && target.Status == model.StorageDataSetStatusReady
	}, 20*time.Second)

	target, err := fixture.env.repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil || target == nil || target.DataSetID == nil {
		t.Fatalf("target = %#v err=%v, want the submitted service recorded", target, err)
	}
	if target.DataSetID.String() != "2002" {
		t.Fatalf("target recorded data set %s, want the one it submitted", target.DataSetID.String())
	}
	row, err := fixture.env.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || row == nil {
		t.Fatalf("GetByID = %#v err=%v", row, err)
	}
	if row.Status == storagereplacement.StatusFailed {
		t.Fatalf("replacement failed on its own submitted service: %v", row.LastError)
	}
}
