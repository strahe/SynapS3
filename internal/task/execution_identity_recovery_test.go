package task_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	bindingtask "github.com/strahe/synaps3/internal/task/binding"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/walletoperation"
	taskengine "github.com/strahe/synaps3/internal/worker"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestStorageIdentitySurvivesSuccessfulEnsureOwnerRelease(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), pipeline.targetSet.ID)
	if err != nil || binding.EnsureTaskID == nil {
		t.Fatalf("ready fixture has no creation owner: %#v %v", binding, err)
	}
	creationID := *binding.EnsureTaskID
	if err := runtime.repos.Contents.CompleteDataSetEnsureTask(t.Context(), binding.ID, *binding.EnsureTaskID); err != nil {
		t.Fatal(err)
	}
	binding, err = runtime.repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
	if err != nil || binding.EnsureTaskID != nil {
		t.Fatalf("successful ensure did not release its active owner: %#v %v", binding, err)
	}
	identity, err := bindingtask.RecordedIdentity(t.Context(), runtime.repos, binding)
	if err != nil || identity.Match(testStorageExecutionIdentity()) != nil {
		t.Fatalf("permanent creation identity lost with active owner: %#v %v", identity, err)
	}
	wrongClient := testOnChainID(t, 999999)
	raw, err := json.Marshal(struct {
		storagepipeline.ExecutionIdentity
		ClientDataSetID string `json:"client_data_set_id"`
	}{testStorageExecutionIdentity(), wrongClient.String()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(raw)).Where("id = ?", creationID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := bindingtask.RecordedIdentity(t.Context(), runtime.repos, binding); !errors.Is(err, storagepipeline.ErrExecutionIdentityChanged) {
		t.Fatalf("conflicting creation evidence was accepted: %v", err)
	}
	if _, err := bindingtask.RecordedIdentity(t.Context(), runtime.repos, &model.StorageDataSet{ID: 999999}); !errors.Is(err, storagepipeline.ErrExecutionIdentityMissing) {
		t.Fatalf("missing history borrowed a signing identity: %v", err)
	}
}

func TestRetirementCheckpointIdentityStopsWithOperatorAttention(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing identity=%v", missing), func(t *testing.T) {
			terminator := &observingRetirementTerminator{}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true})
			replacement, row := seedSourceRetirement(t, runtime, storedObjectSequence.Add(1))
			evidence := map[string]any{"sends": 1, "attempted_at": time.Now().UTC().Add(-time.Hour), "pdp_verifier": testutil.DefaultPDPVerifierAddress}
			if !missing {
				identity := testutil.DefaultContextIdentity
				identity.RecordKeeper = common.HexToAddress("0x00000000000000000000000000000000000000ee")
				evidence["identity"] = identity
			}
			raw, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", json.RawMessage(raw)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			failed := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			want := "termination_identity_changed"
			if missing {
				want = "storage_identity_missing"
			}
			if failed.FailureReason == nil || *failed.FailureReason != want || terminator.observations.Load() != 0 || terminator.calls.Load() != 0 || !testutil.JSONEqual(raw, failed.Checkpoint) {
				t.Fatalf("retirement read, sent, or discarded conflicted evidence: task=%#v reads=%d sends=%d", failed, terminator.observations.Load(), terminator.calls.Load())
			}
			stored, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
			if err != nil || stored.Status != storagereplacement.StatusCleanupAttention {
				t.Fatalf("replacement lost operator attention: %#v %v", stored, err)
			}
			binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), replacement.SourceDataSetID)
			if err != nil || binding.RetirementTaskID == nil || *binding.RetirementTaskID != row.ID {
				t.Fatalf("retirement lost its original owner: %#v %v", binding, err)
			}
		})
	}
}

func TestAcceptedWalletRequestRecoveryKeepsUnsentIdentityInInput(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("payment contract changed=%v", changed), func(t *testing.T) {
			identity := testutil.DefaultWalletExecutionIdentity()
			if changed {
				address := common.HexToAddress("0x00000000000000000000000000000000000000ee")
				identity.Payments = &address
			}
			var broadcasts atomic.Int64
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{wallet: testWalletOperator{
				identity: identity,
				fund: func(context.Context, *big.Int) (synapse.WalletSubmission, error) {
					broadcasts.Add(1)
					return synapse.WalletSubmission{}, nil
				},
			}})
			operation, row := seedWalletTask(t, runtime, "accepted-before-restart")
			var input walletoperation.Input
			if err := json.Unmarshal(row.Input, &input); err != nil || !input.Complete() || len(row.Checkpoint) != 0 {
				t.Fatalf("accepted request = %#v checkpoint=%s: %v", input, row.Checkpoint, err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("resume_mode = ?", model.TaskResumeModeRecover).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			want := model.TaskStatusPending
			if changed {
				want = model.TaskStatusFailed
			}
			got := runOneStorageTask(t, runtime, row, want)
			stored, err := runtime.repos.WalletOperations.GetByID(t.Context(), operation.ID)
			if err != nil || stored.BroadcastAttemptedAt != nil || stored.Status != model.WalletOperationStatusPending || stored.TaskID == nil || *stored.TaskID != row.ID || broadcasts.Load() != 0 || len(got.Checkpoint) != 0 {
				t.Fatalf("unsent recovery changed operation or broadcast evidence: operation=%#v task=%#v broadcasts=%d err=%v", stored, got, broadcasts.Load(), err)
			}
			if changed {
				if got.FailureReason == nil || *got.FailureReason != "wallet_identity_changed" {
					t.Fatalf("changed contract was not blocked: %#v", got)
				}
			} else if got.ResumeMode != model.TaskResumeModeExecute {
				t.Fatalf("unsent operation did not return safely to execute: %#v", got)
			}
		})
	}
}

func TestReplacementDoesNotRetryBlockedCommitThroughAnotherMember(t *testing.T) {
	for _, reason := range []string{"storage_identity_changed", "store_identity_mismatch"} {
		t.Run(reason, func(t *testing.T) {
			attempts, dependencyRetries := 1, 2
			f := newReplacementSourceFixture(t, handlerRuntimeOptions{maxAttempts: &attempts, dependencyRetries: &dependencyRetries})
			ctx := t.Context()
			members := []*model.StorageCopy{f.write(t, "blocked.bin", false), f.write(t, "ready.bin", false)}
			seal := make([]repository.SealMember, len(members))
			for i, member := range members {
				pieceCID := testPieceCID(t, fmt.Sprintf("batch-member-%d", i)).String()
				if err := f.runtime.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{StorageCopyID: member.ID, ContentID: member.ContentID, CopyIndex: member.CopyIndex, PieceCID: pieceCID}); err != nil {
					t.Fatal(err)
				}
				seal[i] = repository.SealMember{CopyID: member.ID, ContentID: member.ContentID, PieceCID: pieceCID}
			}
			requestID := "blocked-batch"
			commit, _, err := f.runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeStorageCommit, Input: storagepipeline.CommitRequestInput{RequestID: requestID, ExecutionIdentity: testStorageExecutionIdentity()}})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Contents.CreateCollectingCommitRequest(ctx, repository.CreateCommitRequestInput{RequestID: requestID, TaskID: commit.ID, StorageDataSetID: f.source.ID, CopyIDs: []int64{members[0].ID, members[1].ID}}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.runtime.repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{RequestID: requestID, TaskID: commit.ID, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(82)), Members: seal}); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Contents.ReturnCommitMembersToTransfer(ctx, requestID, commit.ID, []int64{members[0].ID}, time.Now()); err != nil {
				t.Fatal(err)
			}
			owner := bindCopyTask(t, f.runtime, members[0], model.TaskTypeStorageStore)
			if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("failure_reason = ?", reason).Set("finished_at = ?", time.Now()).Where("id = ?", owner.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			failedCommit := runOneStorageTask(t, f.runtime, commit, model.TaskStatusFailed)
			if failedCommit.FailureReason == nil || *failedCommit.FailureReason != "commit_member_recovery_blocked" {
				t.Fatalf("commit did not preserve its blocked dependency: %#v", failedCommit)
			}
			other, err := f.runtime.repos.Contents.GetUploadCopyByID(ctx, members[1].ID)
			if err != nil || other.Status != model.StorageCopyStatusCommitting || other.WorkTaskID() == nil || *other.WorkTaskID() != commit.ID {
				t.Fatalf("other batch member does not expose its failed commit: %#v %v", other, err)
			}
			replacement, coordinator := f.replace(t)
			runStorageTaskClaims(t, f.runtime, coordinator, model.TaskStatusFailed, 3)
			for _, sourceID := range []int64{owner.ID, commit.ID} {
				if successor, err := f.runtime.repos.Tasks.GetDirectSuccessor(ctx, sourceID); err != nil || successor != nil {
					t.Fatalf("replacement created an automatic successor for %d: %#v %v", sourceID, successor, err)
				}
			}
			row, err := f.runtime.repos.Replacements.GetByID(ctx, replacement.ID)
			if err != nil || row.Status != storagereplacement.StatusFailed {
				t.Fatalf("replacement did not stop for operator recovery: %#v %v", row, err)
			}
			manual, err := f.runtime.service.Retry(ctx, failedCommit.ID)
			if err != nil || manual.RetryOfTaskID == nil || *manual.RetryOfTaskID != failedCommit.ID {
				t.Fatalf("explicit commit retry was blocked: %#v %v", manual, err)
			}
			request, err := f.runtime.repos.Contents.GetCommitRequest(ctx, requestID)
			if err != nil || request.Status != storagecommit.RequestStatusReady || request.TaskID == nil || *request.TaskID != manual.ID || request.Sends != 0 {
				t.Fatalf("manual recovery changed original request: %#v %v", request, err)
			}
		})
	}
}

func TestCleanupResolvesEachCopiesOwnCreationIdentity(t *testing.T) {
	var observations atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{storage: &testutil.MockStorageClient{
		DeletionStateFunc: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			observations.Add(1)
			return synapse.CleanupPieceState{Status: synapse.CleanupPieceRemoved}, nil
		},
	}})
	content, row := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending, model.StorageCleanupCopyStatusPending)
	var copies []model.StorageCleanupCopy
	if err := runtime.db.NewSelect().Model(&copies).Where("content_id = ?", content.ID).Order("copy_index").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	binding, err := runtime.repos.Contents.GetDataSetBindingByID(t.Context(), copies[1].StorageDataSetID)
	if err != nil || binding.EnsureTaskID == nil {
		t.Fatalf("second binding=%#v: %v", binding, err)
	}
	identity := testutil.DefaultContextIdentity
	identity.RecordKeeper = common.HexToAddress("0x00000000000000000000000000000000000000ee")
	input, err := json.Marshal(storagepipeline.DataSetInput{DataSetID: binding.ID, ExecutionIdentity: storagepipeline.NewExecutionIdentity(identity, testutil.DefaultPDPVerifierAddress)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("input_json = ?", json.RawMessage(input)).Where("id = ?", *binding.EnsureTaskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	failed := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
	if failed.FailureReason == nil || *failed.FailureReason != "storage_identity_changed" || observations.Load() != 1 {
		t.Fatalf("cleanup reused the first copy's identity: task=%#v observations=%d", failed, observations.Load())
	}
	if err := runtime.db.NewSelect().Model(&copies).Where("content_id = ?", content.ID).Order("copy_index").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if copies[0].Status != model.StorageCleanupCopyStatusRemoved || copies[1].Status != model.StorageCleanupCopyStatusPending {
		t.Fatalf("copy cleanup facts=%#v", copies)
	}
}

func TestReadyCommitDoesNotReplaceIdentityBlockedMemberAtAnyBudget(t *testing.T) {
	for _, attempts := range []int{1, 4} {
		for _, reason := range []string{"storage_identity_changed", "store_identity_mismatch"} {
			t.Run(fmt.Sprintf("attempts=%d/%s", attempts, reason), func(t *testing.T) {
				f := newRegistrationFixture(t, 1, nil, 0, func(options *handlerRuntimeOptions) { options.maxAttempts = &attempts })
				requestID, taskID := f.collect(t)
				copyRow := f.copies[0]
				if _, err := f.runtime.repos.Contents.SealCommitRequest(t.Context(), repository.SealCommitRequestInput{
					RequestID: requestID, TaskID: taskID, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(81)),
					Members: []repository.SealMember{{CopyID: copyRow.ID, ContentID: copyRow.ContentID, PieceCID: f.pieceCID(t, copyRow)}},
				}); err != nil {
					t.Fatal(err)
				}
				if err := f.runtime.repos.Contents.ReturnCommitMembersToTransfer(t.Context(), requestID, taskID, []int64{copyRow.ID}, time.Now()); err != nil {
					t.Fatal(err)
				}
				copyRow, err := f.runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
				if err != nil {
					t.Fatal(err)
				}
				owner := bindCopyTask(t, f.runtime, copyRow, model.TaskTypeStorageStore)
				message := "original request requires operator recovery"
				evidence := json.RawMessage(`{"session_id":"00000000-0000-4000-8000-000000000001","preserve":true}`)
				if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("failure_reason = ?", reason).Set("last_error = ?", message).Set("checkpoint_json = ?", evidence).Set("finished_at = ?", time.Now()).Where("id = ?", owner.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				before := f.request(t, requestID)
				parent, err := f.runtime.repos.Tasks.GetByID(t.Context(), taskID)
				if err != nil {
					t.Fatal(err)
				}
				failed := runOneStorageTask(t, f.runtime, parent, model.TaskStatusFailed)
				if failed.FailureReason == nil || *failed.FailureReason != "commit_member_recovery_blocked" {
					t.Fatalf("blocked batch = %#v", failed)
				}
				after := f.request(t, requestID)
				kept, err := f.runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
				if err != nil || after.Status != storagecommit.RequestStatusReady || after.TaskID == nil || *after.TaskID != taskID || after.ExtraDataHex == nil || *after.ExtraDataHex != *before.ExtraDataHex || after.Sends != 0 || kept.ActiveTaskID == nil || *kept.ActiveTaskID != owner.ID || kept.CommitRequestID == nil || *kept.CommitRequestID != requestID {
					t.Fatalf("blocked batch lost original request or owner: before=%#v after=%#v copy=%#v err=%v", before, after, kept, err)
				}
				old, err := f.runtime.repos.Tasks.GetByID(t.Context(), owner.ID)
				if err != nil || old.SupersededAt != nil || !testutil.JSONEqual(old.Checkpoint, evidence) {
					t.Fatalf("blocked member evidence changed: %#v %v", old, err)
				}
				count, err := f.runtime.db.NewSelect().Model((*model.Task)(nil)).Where("retry_of_task_id IN (?, ?)", owner.ID, taskID).Count(t.Context())
				if err != nil || count != 0 {
					t.Fatalf("blocked request created %d successors: %v", count, err)
				}
				if sends, _ := f.provider.sent(); len(sends) != 0 || f.provider.signed != 0 || f.provider.nonces.Reads() != 0 {
					t.Fatalf("blocked request performed external work: sends=%d signatures=%d proofs=%d", len(sends), f.provider.signed, f.provider.nonces.Reads())
				}
			})
		}
	}
}
