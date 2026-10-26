package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storageremoval"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type testRemovalChain struct {
	preflight func(context.Context, sdktypes.BigInt) (synapse.RemovalPreflight, error)
}

func (r testRemovalChain) Preflight(ctx context.Context, id sdktypes.BigInt) (synapse.RemovalPreflight, error) {
	if r.preflight != nil {
		return r.preflight(ctx, id)
	}
	return synapse.RemovalPreflight{}, nil
}

func (testRemovalChain) ProvingConfig(context.Context) (synapse.RemovalProvingConfig, error) {
	return synapse.RemovalProvingConfig{MaxProvingPeriod: 240, ChallengeWindow: 20}, nil
}

type removalFixture struct {
	runtime  handlerTestRuntime
	contents []*model.StorageContent
	cleanups []*model.Task
	members  []model.StorageCleanupCopy
	request  *storageremoval.Request
	row      *model.Task
}

func newRemovalFixture(t *testing.T, pieces int, configure ...func(*handlerRuntimeOptions)) *removalFixture {
	t.Helper()
	options := handlerRuntimeOptions{
		storage: &testutil.MockStorageClient{},
		epochs:  testEpochReader{epoch: 100},
		deletionState: func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Status: synapse.CleanupPiecePresent, BlockNumber: 100}, nil
		},
	}
	for _, fn := range configure {
		fn(&options)
	}
	runtime := newHandlerTestRuntime(t, options)
	content, cleanup := seedStorageCleanup(t, runtime, model.StorageCleanupCopyStatusPending)
	f := &removalFixture{runtime: runtime, contents: []*model.StorageContent{content}, cleanups: []*model.Task{cleanup}}
	var first model.StorageCleanupCopy
	if err := runtime.db.NewSelect().Model(&first).Where("content_id = ?", content.ID).Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.members = append(f.members, first)
	for i := 1; i < pieces; i++ {
		memberContent := &model.StorageContent{
			BucketID: content.BucketID, ContentSize: 1, RequestedCopies: 1, CleanupGeneration: 1,
			Checksum: testutil.StorageChecksum(fmt.Sprintf("removal-%d-%d", content.ID, i)),
		}
		if _, err := runtime.db.NewInsert().Model(memberContent).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		member := first
		member.ID, member.ContentID, member.Checksum = 0, memberContent.ID, memberContent.Checksum
		member.PieceID = testOnChainID(t, 21000+storedObjectSequence.Add(1))
		member.PieceCID = testPieceCID(t, fmt.Sprintf("removal-%d-%d", content.ID, i)).String()
		if _, err := runtime.db.NewInsert().Model(&member).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		row, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
			Type: model.TaskTypeStorageCleanup, Input: storagecleanup.Input{ContentID: memberContent.ID, Generation: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.repos.StorageCleanup.BindTask(t.Context(), memberContent.ID, 1, row.ID); err != nil {
			t.Fatal(err)
		}
		f.contents, f.cleanups, f.members = append(f.contents, memberContent), append(f.cleanups, row), append(f.members, member)
	}
	for i, row := range f.cleanups {
		f.cleanups[i] = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	}
	requests := []storageremoval.Request{}
	if err := runtime.db.NewSelect().Model(&requests).Where("storage_data_set_id = ?", first.StorageDataSetID).Scan(t.Context()); err != nil || len(requests) != 1 {
		t.Fatalf("collecting requests = %#v, %v", requests, err)
	}
	f.request = &requests[0]
	var err error
	f.row, err = runtime.repos.Tasks.GetByID(t.Context(), *f.request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *removalFixture) step(t *testing.T, status model.TaskStatus) *model.Task {
	t.Helper()
	f.advance(t)
	if f.row.Status != status {
		t.Fatalf("removal task = %#v, want %s", f.row, status)
	}
	return f.row
}

func (f *removalFixture) advance(t *testing.T) {
	t.Helper()
	cancel, done := f.start(t)
	f.row = waitForTask(t, f.runtime.repos, f.row.ID, func(row *model.Task) bool {
		return row.ClaimGeneration > f.row.ClaimGeneration && row.Status != model.TaskStatusRunning
	})
	stopHandlerEngine(t, cancel, done)
	var err error
	f.request, err = f.runtime.repos.StorageRemoval.GetRemovalRequest(t.Context(), f.request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *removalFixture) start(t *testing.T) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	wakeTask(t, f.runtime, f.row.ID)
	var excluded []int64
	if err := f.runtime.db.NewSelect().Model((*model.Task)(nil)).Column("id").Where("id <> ?", f.row.ID).Scan(t.Context(), &excluded); err != nil {
		t.Fatal(err)
	}
	repos := *f.runtime.repos
	repos.Tasks = &limitedClaimRepository{TaskRepository: f.runtime.repos.Tasks, maximum: 1, excludedTaskIDs: excluded}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration}, &repos, handlerExecutionRegistry(t, f.runtime), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	return runEngine(t, engine)
}

func (f *removalFixture) sends(t *testing.T) []storageremoval.Send {
	t.Helper()
	rows, err := f.runtime.repos.StorageRemoval.ListRemovalSends(t.Context(), f.request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func (f *removalFixture) send(t *testing.T) {
	t.Helper()
	before := len(f.sends(t))
	for range 4 {
		f.step(t, model.TaskStatusPending)
		if len(f.sends(t)) > before {
			return
		}
	}
	t.Fatalf("removal task did not submit: %#v", f.row)
}

func (f *removalFixture) retry(t *testing.T) {
	t.Helper()
	var err error
	f.row, err = f.runtime.service.Retry(t.Context(), f.row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.row.Runtime) != 0 && string(f.row.Runtime) != "{}" && string(f.row.Runtime) != "null" {
		t.Fatalf("manual retry inherited operation runtime: %s", f.row.Runtime)
	}
}

func removalUnknownError() error { return errors.New("provider connection closed after request") }

func ageRemovalSend(t *testing.T, f *removalFixture) {
	t.Helper()
	when := time.Now().Add(-time.Hour)
	if _, err := f.runtime.db.NewUpdate().Model((*storageremoval.Send)(nil)).Set("attempted_at = ?", when).Where("request_id = ?", f.request.RequestID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	var runtime model.TaskRuntime
	if len(f.row.Runtime) > 0 && json.Unmarshal(f.row.Runtime, &runtime) == nil && runtime.OperationKey != "" {
		ageHandlerOperation(t, f.runtime, f.row.ID, time.Hour)
	}
}

func TestRemovalBatchSchedulesExactMembersAndWaitsForRemoval(t *testing.T) {
	var status atomic.Value
	status.Store(synapse.CleanupPiecePresent)
	var calls atomic.Int64
	var sent []sdktypes.BigInt
	hash := common.HexToHash("0x123")
	f := newRemovalFixture(t, 3, func(options *handlerRuntimeOptions) {
		options.deletionState = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			return synapse.CleanupPieceState{Status: status.Load().(synapse.CleanupPieceStatus), BlockNumber: 100}, nil
		}
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(_ context.Context, ids []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				calls.Add(1)
				sent = append([]sdktypes.BigInt(nil), ids...)
				status.Store(synapse.CleanupPieceScheduled)
				return &sdktypes.WriteResult{Hash: hash}, errors.New("HTTP 500: https://user:password@provider.example/pdp/data-sets/1?token=secret#fragment")
			}}, nil
		}
	})
	f.send(t)
	if calls.Load() != 1 || len(sent) != len(f.members) {
		t.Fatalf("batch sends = %d, members = %v", calls.Load(), sent)
	}
	evidence := f.sends(t)
	if len(evidence) != 1 || evidence[0].TxHash == nil || *evidence[0].TxHash != hash.Hex() || len(evidence[0].Pieces) != 3 || evidence[0].LastError == nil || !strings.Contains(*evidence[0].LastError, "HTTP 500") || strings.Contains(*evidence[0].LastError, "password") || strings.Contains(*evidence[0].LastError, "secret") {
		t.Fatalf("batch submission evidence = %#v", evidence)
	}
	f.step(t, model.TaskStatusPending)
	if f.request.Status != storageremoval.RequestStatusQueued || f.request.QueuedBlock == nil || *f.request.QueuedBlock != 100 {
		t.Fatalf("scheduled batch = %#v", f.request)
	}
	status.Store(synapse.CleanupPieceDataSetCleanup)
	f.step(t, model.TaskStatusPending)
	for _, content := range f.contents {
		if current, err := f.runtime.repos.Contents.GetByID(t.Context(), content.ID); err != nil || current == nil {
			t.Fatalf("data set cleanup released content: %#v, %v", current, err)
		}
	}
	status.Store(synapse.CleanupPieceRemoved)
	f.step(t, model.TaskStatusPending)
	f.step(t, model.TaskStatusCompleted)
	if f.request.Status != storageremoval.RequestStatusSettled || f.request.TaskID != nil || calls.Load() != 1 {
		t.Fatalf("settled batch = %#v, sends=%d", f.request, calls.Load())
	}
	for i, content := range f.contents {
		f.cleanups[i] = runOneStorageTask(t, f.runtime, f.cleanups[i], model.TaskStatusCompleted)
		if current, err := f.runtime.repos.Contents.GetByID(t.Context(), content.ID); err != nil || current != nil {
			t.Fatalf("removed content = %#v, %v", current, err)
		}
	}
}

func TestRemovalUnknownThen404KeepsEveryMemberAndAuthorization(t *testing.T) {
	var calls, providerReads atomic.Int64
	f := newRemovalFixture(t, 2, func(options *handlerRuntimeOptions) {
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				if calls.Add(1) == 1 {
					return nil, removalUnknownError()
				}
				return nil, &pdp.HTTPError{StatusCode: 404, Body: "piece not found"}
			}}, nil
		}
		options.storage.ProviderPiecesKnownFunc = func(context.Context, string, sdktypes.BigInt, []sdktypes.BigInt) (synapse.ProviderPiecesState, error) {
			providerReads.Add(1)
			return synapse.ProviderPiecesState{}, errors.New("must not split a submitted batch")
		}
	})
	f.send(t)
	f.step(t, model.TaskStatusPending)
	if calls.Load() != 1 || f.row.RetryCount != 0 {
		t.Fatalf("unknown send replayed before deadline: task=%#v sends=%d", f.row, calls.Load())
	}
	ageRemovalSend(t, f)
	f.send(t)
	evidence := f.sends(t)
	if calls.Load() != 2 || providerReads.Load() != 0 || len(evidence) != 2 || evidence[0].RefusedAt != nil || evidence[1].RefusedAt == nil || f.request.Status != storageremoval.RequestStatusSubmitted || f.row.RetryCount != 2 {
		t.Fatalf("404 recovery = %#v, sends=%#v, calls=%d GETs=%d", f.row, evidence, calls.Load(), providerReads.Load())
	}
	members, err := f.runtime.repos.StorageRemoval.ListRemovalMembers(t.Context(), f.request.RequestID)
	if err != nil || len(members) != 2 {
		t.Fatalf("404 lost batch members: %#v, %v", members, err)
	}
	for _, member := range members {
		if member.Status != model.StorageCleanupCopyStatusPending || member.RemovalRequestID == nil || *member.RemovalRequestID != f.request.RequestID {
			t.Fatalf("404 changed member: %#v", member)
		}
	}
}

func TestRemovalObservationErrorsPreserveAttemptBudget(t *testing.T) {
	for _, source := range []string{"chain", "receipt", "preflight"} {
		t.Run(source, func(t *testing.T) {
			var broken atomic.Bool
			var calls atomic.Int64
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.deletionState = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					if source == "chain" && broken.Load() {
						return synapse.CleanupPieceState{}, errors.New("chain unavailable")
					}
					return synapse.CleanupPieceState{Status: synapse.CleanupPiecePresent, BlockNumber: 100}, nil
				}
				options.receipts = testReceiptChecker{check: func(context.Context, common.Hash) (*ethtypes.Receipt, error) {
					if source == "receipt" && broken.Load() {
						return nil, errors.New("receipt unavailable")
					}
					return nil, ethereum.NotFound
				}}
				options.removalChain = testRemovalChain{preflight: func(context.Context, sdktypes.BigInt) (synapse.RemovalPreflight, error) {
					if source == "preflight" && broken.Load() {
						return synapse.RemovalPreflight{}, errors.New("preflight unavailable")
					}
					return synapse.RemovalPreflight{}, nil
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						calls.Add(1)
						return &sdktypes.WriteResult{Hash: common.HexToHash("0x1")}, nil
					}}, nil
				}
			})
			f.send(t)
			broken.Store(true)
			if source == "preflight" {
				ageRemovalSend(t, f)
			}
			for range 3 {
				f.step(t, model.TaskStatusPending)
			}
			if f.row.RetryCount != 0 || calls.Load() != 1 || len(f.sends(t)) != 1 || f.row.LastError == nil || !strings.Contains(*f.row.LastError, "unavailable") {
				t.Fatalf("failed observation spent send budget: task=%#v sends=%d", f.row, calls.Load())
			}
		})
	}
}

func TestRemovalObservationErrorsAfterRetryPreserveAttemptBudget(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		refused bool
		source  string
	}{
		{name: "refused_chain", refused: true, source: "chain"},
		{name: "submitted_chain", source: "chain"},
		{name: "submitted_preflight", source: "preflight"},
		{name: "refused_context", refused: true, source: "context"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var broken atomic.Bool
			var calls atomic.Int64
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.deletionState = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					if scenario.source == "chain" && broken.Load() {
						return synapse.CleanupPieceState{}, errors.New("chain unavailable")
					}
					return synapse.CleanupPieceState{Status: synapse.CleanupPiecePresent, BlockNumber: 100}, nil
				}
				options.removalChain = testRemovalChain{preflight: func(context.Context, sdktypes.BigInt) (synapse.RemovalPreflight, error) {
					if scenario.source == "preflight" && broken.Load() {
						return synapse.RemovalPreflight{}, errors.New("preflight unavailable")
					}
					return synapse.RemovalPreflight{}, nil
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					if scenario.source == "context" && broken.Load() {
						return nil, errors.New("cleanup context unavailable")
					}
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						if calls.Add(1) == 1 && scenario.refused {
							return nil, &pdp.HTTPError{StatusCode: 400, Body: "request rejected"}
						}
						return &sdktypes.WriteResult{Hash: common.HexToHash("0x1")}, nil
					}}, nil
				}
			})
			f.send(t)
			if !scenario.refused {
				ageRemovalSend(t, f)
				f.step(t, model.TaskStatusPending)
			}
			if f.row.RetryCount != 1 || f.row.ResumeMode != model.TaskResumeModeExecute || calls.Load() != 1 {
				t.Fatalf("retry did not prepare the second attempt: task=%#v sends=%d", f.row, calls.Load())
			}
			broken.Store(true)
			f.step(t, model.TaskStatusPending)
			if f.row.RetryCount != 1 || f.row.ResumeMode != model.TaskResumeModeRecover || calls.Load() != 1 || len(f.sends(t)) != 1 || f.row.LastError == nil || !strings.Contains(*f.row.LastError, "unavailable") {
				t.Fatalf("failed observation spent the prepared attempt: task=%#v sends=%d", f.row, calls.Load())
			}
			broken.Store(false)
			f.step(t, model.TaskStatusPending)
			if f.row.RetryCount != 1 || f.row.ResumeMode != model.TaskResumeModeExecute || calls.Load() != 1 || len(f.sends(t)) != 1 {
				t.Fatalf("recovery skipped an unadmitted attempt: task=%#v sends=%d", f.row, calls.Load())
			}
			f.step(t, model.TaskStatusPending)
			var runtime model.TaskRuntime
			if err := json.Unmarshal(f.row.Runtime, &runtime); err != nil {
				t.Fatal(err)
			}
			if f.row.RetryCount != 1 || calls.Load() != 2 || len(f.sends(t)) != 2 || runtime.LastAdmittedAttempt != 2 {
				t.Fatalf("second send did not use the prepared attempt: task=%#v sends=%d runtime=%#v", f.row, calls.Load(), runtime)
			}
		})
	}
}

func TestRemovalMissingServiceRecordCanRecover(t *testing.T) {
	for _, status := range []storageremoval.RequestStatus{storageremoval.RequestStatusReady, storageremoval.RequestStatusSubmitted} {
		t.Run(string(status), func(t *testing.T) {
			var missing atomic.Bool
			var calls atomic.Int64
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.removalChain = testRemovalChain{preflight: func(context.Context, sdktypes.BigInt) (synapse.RemovalPreflight, error) {
					return synapse.RemovalPreflight{DataSetMissing: missing.Load()}, nil
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						calls.Add(1)
						return nil, removalUnknownError()
					}}, nil
				}
			})
			var previousSends int64
			if status == storageremoval.RequestStatusSubmitted {
				f.send(t)
				ageRemovalSend(t, f)
				previousSends = 1
			}
			missing.Store(true)
			for range 3 {
				f.step(t, model.TaskStatusPending)
			}
			if f.request.Status != status || f.request.QueuedAt != nil || f.request.QueuedBlock != nil || calls.Load() != previousSends || int64(len(f.sends(t))) != previousSends || f.row.RetryCount != 0 || f.row.LastError == nil || !strings.Contains(strings.ToLower(*f.row.LastError), "service") {
				t.Fatalf("missing service record stopped recoverable removal: task=%#v batch=%#v sends=%d", f.row, f.request, calls.Load())
			}
			missing.Store(false)
			f.send(t)
			if calls.Load() != previousSends+1 || int64(len(f.sends(t))) != previousSends+1 || f.request.Status != storageremoval.RequestStatusSubmitted || int64(f.row.RetryCount) != previousSends {
				t.Fatalf("restored service record did not resume bounded sending: task=%#v batch=%#v sends=%d", f.row, f.request, calls.Load())
			}
		})
	}
}

func TestRemovalFailedReceiptRequiresStableCurrentState(t *testing.T) {
	for _, scenario := range []string{"state_behind_receipt", "queued_during_receipt_check", "receipt_reorganized"} {
		t.Run(scenario, func(t *testing.T) {
			var sends, receiptReads atomic.Int64
			var queued atomic.Bool
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.deletionState = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					status := synapse.CleanupPiecePresent
					if queued.Load() {
						status = synapse.CleanupPieceScheduled
					}
					return synapse.CleanupPieceState{Status: status, BlockNumber: 100}, nil
				}
				options.receipts = testReceiptChecker{check: func(_ context.Context, tx common.Hash) (*ethtypes.Receipt, error) {
					read := receiptReads.Add(1)
					block, hash := int64(90), common.HexToHash("0x10")
					switch scenario {
					case "state_behind_receipt":
						block = 101
					case "queued_during_receipt_check":
						queued.Store(true)
					case "receipt_reorganized":
						if read > 1 {
							hash = common.HexToHash("0x20")
						}
					}
					return &ethtypes.Receipt{TxHash: tx, Status: ethtypes.ReceiptStatusFailed, BlockNumber: big.NewInt(block), BlockHash: hash}, nil
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						sends.Add(1)
						return &sdktypes.WriteResult{Hash: common.HexToHash("0x1")}, nil
					}}, nil
				}
			})
			f.send(t)
			f.step(t, model.TaskStatusPending)
			evidence := f.sends(t)
			if sends.Load() != 1 || f.row.RetryCount != 0 || len(evidence) != 1 || f.request.Status != storageremoval.RequestStatusSubmitted || f.request.AttentionCode != nil {
				t.Fatalf("unstable receipt stopped or resent removal: task=%#v batch=%#v evidence=%#v", f.row, f.request, evidence)
			}
			if scenario == "queued_during_receipt_check" {
				if evidence[0].ReceiptStatus == nil || *evidence[0].ReceiptStatus != 0 {
					t.Fatalf("collision did not preserve the failed receipt: %#v", evidence[0])
				}
			} else if evidence[0].ReceiptStatus != nil || evidence[0].ConfirmedTxHash != nil {
				t.Fatalf("unconfirmed receipt became durable: %#v", evidence[0])
			}
		})
	}
}

func TestRemovalCollisionReceiptCanResendOnlyPresentMembers(t *testing.T) {
	var calls atomic.Int64
	var queued atomic.Bool
	var queuedPiece string
	f := newRemovalFixture(t, 2, func(options *handlerRuntimeOptions) {
		options.deletionState = func(_ context.Context, _ sdktypes.BigInt, piece sdktypes.BigInt) (synapse.CleanupPieceState, error) {
			status := synapse.CleanupPiecePresent
			if queued.Load() && piece.String() == queuedPiece {
				status = synapse.CleanupPieceScheduled
			}
			return synapse.CleanupPieceState{Status: status, BlockNumber: 100}, nil
		}
		options.receipts = testReceiptChecker{check: func(_ context.Context, tx common.Hash) (*ethtypes.Receipt, error) {
			queued.Store(true)
			return &ethtypes.Receipt{TxHash: tx, Status: ethtypes.ReceiptStatusFailed, BlockNumber: big.NewInt(90), BlockHash: common.HexToHash("0x10")}, nil
		}}
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(_ context.Context, pieces []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				call := calls.Add(1)
				if call > 1 && (len(pieces) != 1 || pieces[0].String() == queuedPiece) {
					t.Errorf("collision recovery sent an already queued piece: %v", pieces)
				}
				return &sdktypes.WriteResult{Hash: common.BigToHash(big.NewInt(call))}, nil
			}}, nil
		}
	})
	queuedPiece = f.members[0].PieceID.String()
	f.send(t)
	f.step(t, model.TaskStatusPending)
	first := f.sends(t)
	if len(first) != 1 || first[0].ReceiptStatus == nil || *first[0].ReceiptStatus != 0 || first[0].TxHash == nil || first[0].ConfirmedTxHash == nil || *first[0].TxHash != *first[0].ConfirmedTxHash || f.request.Status != storageremoval.RequestStatusSubmitted || f.request.AttentionCode != nil || f.row.RetryCount != 0 || calls.Load() != 1 {
		t.Fatalf("collision did not retain its failed receipt for observation: task=%#v batch=%#v sends=%#v", f.row, f.request, first)
	}
	ageRemovalSend(t, f)
	f.step(t, model.TaskStatusPending)
	if f.row.RetryCount != 1 || calls.Load() != 1 || len(f.sends(t)) != 1 {
		t.Fatalf("collision recovery bypassed a new attempt: task=%#v sends=%d", f.row, calls.Load())
	}
	f.send(t)
	evidence := f.sends(t)
	if calls.Load() != 2 || f.row.RetryCount != 1 || len(evidence) != 2 || len(evidence[0].Pieces) != 2 || len(evidence[1].Pieces) != 1 || evidence[1].Pieces[0].PieceID.String() != f.members[1].PieceID.String() || evidence[0].ReceiptStatus == nil || *evidence[0].ReceiptStatus != 0 || evidence[0].TxHash == nil || *evidence[0].TxHash != *first[0].TxHash || evidence[0].ConfirmedTxHash == nil || *evidence[0].ConfirmedTxHash != *first[0].ConfirmedTxHash {
		t.Fatalf("collision recovery expanded authorization or lost evidence: task=%#v sends=%#v", f.row, evidence)
	}
	members, err := f.runtime.repos.StorageRemoval.ListRemovalMembers(t.Context(), f.request.RequestID)
	if err != nil || len(members) != 2 {
		t.Fatalf("collision recovery detached members: %#v, %v", members, err)
	}
}

func TestRemovalStopsBeforeSendingForUnsafeConditions(t *testing.T) {
	for _, condition := range []string{"version", "identity", "payer", "terminated"} {
		t.Run(condition, func(t *testing.T) {
			var calls atomic.Int64
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.storage.DeletionStatesFunc = func(_ context.Context, _ sdktypes.BigInt, ids []sdktypes.BigInt) (synapse.CleanupPiecesState, error) {
					version := "3.5.0"
					if condition == "version" {
						version = "3.6.0"
					}
					pieces := make(map[string]synapse.CleanupPieceStatus)
					for _, id := range ids {
						pieces[id.String()] = synapse.CleanupPiecePresent
					}
					return synapse.CleanupPiecesState{Pieces: pieces, BlockNumber: 100, VerifierVersion: version}, nil
				}
				options.removalChain = testRemovalChain{preflight: func(context.Context, sdktypes.BigInt) (synapse.RemovalPreflight, error) {
					return synapse.RemovalPreflight{PayerUnderfunded: condition == "payer", Reason: "insufficient spending allowance", EndEpoch: func() int64 {
						if condition == "terminated" {
							return 90
						}
						return 0
					}()}, nil
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					calls.Add(1)
					return nil, errors.New("unsafe removal reached provider")
				}
			})
			if condition == "identity" {
				identity := testutil.DefaultContextIdentity
				identity.RecordKeeper = common.HexToAddress("0x00000000000000000000000000000000000000ee")
				f.runtime.storage.ContextIdentityValue = identity
			}
			for range 4 {
				f.advance(t)
				if f.row.Status == model.TaskStatusFailed || f.request.Status == storageremoval.RequestStatusQueued {
					break
				}
			}
			if calls.Load() != 0 || len(f.sends(t)) != 0 {
				t.Fatalf("unsafe condition sent removal: %d, %#v", calls.Load(), f.sends(t))
			}
			if condition == "terminated" {
				if f.row.Status != model.TaskStatusPending || f.request.Status != storageremoval.RequestStatusQueued {
					t.Fatalf("terminated data set = %#v, %#v", f.row, f.request)
				}
			} else if f.row.Status != model.TaskStatusFailed || f.request.TaskID == nil || *f.request.TaskID != f.row.ID {
				t.Fatalf("unsafe batch released its owner: %#v, %#v", f.row, f.request)
			}
		})
	}
}

func TestRemovalRevertedReceiptRequiresManualRetryWithPreservedEvidence(t *testing.T) {
	for _, outcome := range []string{"accepted", "unsent"} {
		t.Run(outcome, func(t *testing.T) {
			maxAttempts := 3
			var calls atomic.Int64
			var removed atomic.Bool
			hash := common.HexToHash("0x11")
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.maxAttempts = &maxAttempts
				options.deletionState = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					status := synapse.CleanupPiecePresent
					if removed.Load() {
						status = synapse.CleanupPieceRemoved
					}
					return synapse.CleanupPieceState{Status: status, BlockNumber: 100}, nil
				}
				options.receipts = testReceiptChecker{check: func(_ context.Context, tx common.Hash) (*ethtypes.Receipt, error) {
					return &ethtypes.Receipt{TxHash: tx, Status: ethtypes.ReceiptStatusFailed, BlockNumber: big.NewInt(90), BlockHash: common.HexToHash("0x10")}, nil
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						if calls.Add(1) > 1 && outcome == "unsent" {
							return nil, storage.ErrInvalidArgument
						}
						return &sdktypes.WriteResult{Hash: hash}, nil
					}}, nil
				}
			})
			f.send(t)
			f.step(t, model.TaskStatusFailed)
			first := f.sends(t)
			if f.row.FailureReason == nil || *f.row.FailureReason != "transaction_reverted" || len(first) != 1 || first[0].ReceiptStatus == nil || *first[0].ReceiptStatus != 0 || f.request.AttentionCode == nil || calls.Load() != 1 {
				t.Fatalf("reverted removal = %#v, %#v", f.row, first)
			}
			f.retry(t)
			f.step(t, model.TaskStatusPending)
			if calls.Load() != 1 {
				t.Fatalf("recover mode sent another deletion: %d", calls.Load())
			}
			ageRemovalSend(t, f)
			f.send(t)
			second := f.sends(t)
			if len(second) != 2 || second[0].TxHash == nil || *second[0].TxHash != hash.Hex() || second[0].ReceiptStatus == nil || *second[0].ReceiptStatus != 0 || f.request.AttentionCode != nil {
				t.Fatalf("manual retry lost receipt or stop marker: %#v, %#v", f.request, second)
			}
			if outcome == "unsent" {
				for range 8 {
					ageRemovalSend(t, f)
					f.advance(t)
					if f.row.Status == model.TaskStatusFailed {
						break
					}
				}
				if f.row.Status != model.TaskStatusFailed || f.row.FailureReason == nil || *f.row.FailureReason != "attempts_exhausted" || f.request.AttentionCode == nil || *f.request.AttentionCode != string(storageremoval.AttentionProviderUnreachable) {
					t.Fatalf("failed historical receipt was treated as unconfirmed: task=%#v request=%#v", f.row, f.request)
				}
				evidence := f.sends(t)
				if calls.Load() != int64(1+maxAttempts) || len(evidence) != 1+maxAttempts || f.row.RetryCount != maxAttempts-1 || evidence[0].TxHash == nil || *evidence[0].TxHash != hash.Hex() || evidence[0].ReceiptStatus == nil || *evidence[0].ReceiptStatus != 0 {
					t.Fatalf("retry exhausted without preserving send outcomes: %#v, sends=%d", evidence, calls.Load())
				}
				for _, send := range evidence[1:] {
					if send.UnsentAt == nil || send.TaskID == nil || *send.TaskID != f.row.ID {
						t.Fatalf("retry lost its unsent evidence or ownership: %#v", send)
					}
				}
				return
			}
			removed.Store(true)
			for range 4 {
				f.advance(t)
				if f.row.Status == model.TaskStatusCompleted {
					break
				}
			}
			if f.row.Status != model.TaskStatusCompleted || calls.Load() != 2 {
				t.Fatalf("retried removal did not finish: %#v, sends=%d", f.row, calls.Load())
			}
		})
	}
}

func TestRemovalSendBudgetRetainsBatchAndCanResumeAfterManualRetry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		first     string
		remaining string
		status    storageremoval.RequestStatus
		attention storageremoval.AttentionCode
	}{
		{name: "unknown", first: "unknown", remaining: "unknown", status: storageremoval.RequestStatusSubmitted, attention: storageremoval.AttentionConfirmationTimeout},
		{name: "hash", first: "hash", remaining: "hash", status: storageremoval.RequestStatusSubmitted, attention: storageremoval.AttentionConfirmationTimeout},
		{name: "unknown then refused", first: "unknown", remaining: "refused", status: storageremoval.RequestStatusSubmitted, attention: storageremoval.AttentionConfirmationTimeout},
		{name: "unsent", first: "unsent", remaining: "unsent", status: storageremoval.RequestStatusReady, attention: storageremoval.AttentionProviderUnreachable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			maxAttempts := 2
			var calls atomic.Int64
			var removed atomic.Bool
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.maxAttempts = &maxAttempts
				options.deletionState = func(context.Context, sdktypes.BigInt, sdktypes.BigInt) (synapse.CleanupPieceState, error) {
					status := synapse.CleanupPiecePresent
					if removed.Load() {
						status = synapse.CleanupPieceRemoved
					}
					return synapse.CleanupPieceState{Status: status, BlockNumber: 100}, nil
				}
				options.receipts = testReceiptChecker{check: func(context.Context, common.Hash) (*ethtypes.Receipt, error) {
					return nil, ethereum.NotFound
				}}
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						call := calls.Add(1)
						outcome := tc.remaining
						if call == 1 {
							outcome = tc.first
						}
						switch outcome {
						case "hash":
							return &sdktypes.WriteResult{Hash: common.BigToHash(big.NewInt(call))}, nil
						case "refused":
							return nil, &pdp.HTTPError{StatusCode: 400, Body: "removal refused"}
						case "unsent":
							return nil, storage.ErrInvalidArgument
						default:
							return nil, removalUnknownError()
						}
					}}, nil
				}
			})
			f.send(t)
			for range 8 {
				ageRemovalSend(t, f)
				f.advance(t)
				if f.row.Status == model.TaskStatusFailed {
					break
				}
			}
			if f.row.Status != model.TaskStatusFailed || f.row.FailureReason == nil || *f.row.FailureReason != "attempts_exhausted" || calls.Load() != 2 || len(f.sends(t)) != 2 || f.request.Status != tc.status || f.request.TaskID == nil || *f.request.TaskID != f.row.ID {
				t.Fatalf("removal exceeded its send budget or lost evidence: task=%#v request=%#v sends=%d", f.row, f.request, calls.Load())
			}
			if f.request.AttentionCode == nil || *f.request.AttentionCode != string(tc.attention) {
				t.Fatalf("exhausted removal diagnostic = %#v, want %s", f.request, tc.attention)
			}
			if err := f.runtime.repos.Tasks.AcknowledgeFailed(t.Context(), f.row.ID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("held removal acknowledgement = %v", err)
			}
			if tc.name != "unknown" {
				return
			}
			f.retry(t)
			removed.Store(true)
			for range 4 {
				f.advance(t)
				if f.row.Status == model.TaskStatusCompleted {
					break
				}
			}
			if f.row.Status != model.TaskStatusCompleted || calls.Load() != 2 || len(f.sends(t)) != 2 {
				t.Fatalf("empty-runtime retry replayed a confirmed removal: %#v, sends=%d", f.row, calls.Load())
			}
		})
	}
}

func TestRemovalAdmissionFailureDoesNotSend(t *testing.T) {
	for _, boundary := range []string{"send ledger", "checkpoint"} {
		t.Run(boundary, func(t *testing.T) {
			var calls atomic.Int64
			f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						calls.Add(1)
						return nil, removalUnknownError()
					}}, nil
				}
			})
			if boundary == "checkpoint" {
				rejectTaskCheckpoint(t, f.runtime.db, f.row.ID)
			} else {
				testutil.FailWrites(t, f.runtime.db, "storage_removal_sends", "INSERT", "", "injected removal admission failure")
			}
			for range 3 {
				f.step(t, model.TaskStatusPending)
			}
			if calls.Load() != 0 || len(f.sends(t)) != 0 || f.row.RetryCount != 0 {
				t.Fatalf("unrecorded authorization reached provider: task=%#v sends=%d", f.row, calls.Load())
			}
		})
	}
}

func TestRemovalRetainsReturnedHashThroughSettlementContention(t *testing.T) {
	var calls atomic.Int64
	hash := common.HexToHash("0x222")
	f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				calls.Add(1)
				return &sdktypes.WriteResult{Hash: hash}, nil
			}}, nil
		}
	})
	submissionContention(t, &f.runtime, model.TaskTypeStorageRemoval, "storage_removal_sends", "UPDATE OF tx_hash", "OLD.tx_hash IS NULL AND NEW.tx_hash IS NOT NULL")
	f.send(t)
	evidence := f.sends(t)
	if calls.Load() != 1 || len(evidence) != 1 || evidence[0].TxHash == nil || *evidence[0].TxHash != hash.Hex() || f.row.RetryCount != 0 {
		t.Fatalf("submission contention lost hash: task=%#v sends=%#v calls=%d", f.row, evidence, calls.Load())
	}
	f.step(t, model.TaskStatusPending)
	if calls.Load() != 1 || f.row.RetryCount != 0 {
		t.Fatalf("hash recovery repeated DELETE: task=%#v calls=%d", f.row, calls.Load())
	}
}

// A hash the provider returns while the gateway shuts down is still recorded,
// so recovery observes it and does not send the request again.
func TestRemovalRetainsHashReturnedDuringShutdown(t *testing.T) {
	var calls atomic.Int64
	hash := common.HexToHash("0x444")
	reached := make(chan struct{})
	f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(ctx context.Context, _ []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				if calls.Add(1) == 1 {
					close(reached)
					<-ctx.Done()
				}
				return &sdktypes.WriteResult{Hash: hash}, nil
			}}, nil
		}
	})
	f.step(t, model.TaskStatusPending)
	f.step(t, model.TaskStatusPending)
	cancel, done := f.start(t)
	select {
	case <-reached:
	case <-time.After(3 * time.Second):
		t.Fatal("removal did not send")
	}
	stopHandlerEngine(t, cancel, done)
	evidence := f.sends(t)
	if calls.Load() != 1 || len(evidence) != 1 || evidence[0].TxHash == nil || *evidence[0].TxHash != hash.Hex() {
		t.Fatalf("shutdown lost the returned hash: sends=%#v calls=%d", evidence, calls.Load())
	}
	if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("lease_until = ?", time.Now().Add(-time.Second)).Where("id = ?", f.row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.step(t, model.TaskStatusPending)
	if calls.Load() != 1 {
		t.Fatalf("recovery resent a removal with a saved hash: task=%#v calls=%d", f.row, calls.Load())
	}
}

func TestRemovalPersistentEvidenceFailureRecoversFromAdmissionWithoutExtraSend(t *testing.T) {
	var calls atomic.Int64
	f := newRemovalFixture(t, 1, func(options *handlerRuntimeOptions) {
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				calls.Add(1)
				return &sdktypes.WriteResult{Hash: common.HexToHash("0x333")}, nil
			}}, nil
		}
	})
	f.step(t, model.TaskStatusPending)
	f.step(t, model.TaskStatusPending)
	if _, err := f.runtime.db.ExecContext(t.Context(), "CREATE SEQUENCE removal_persistent_fault"); err != nil {
		t.Fatal(err)
	}
	removeFault := testutil.FailWrites(t, f.runtime.db, "storage_removal_sends", "UPDATE OF tx_hash", "NEW.tx_hash IS NOT NULL AND nextval('removal_persistent_fault') > 0", "injected removal evidence failure")
	cancel, done := f.start(t)
	f.row = waitForTask(t, f.runtime.repos, f.row.ID, func(row *model.Task) bool {
		var failures int64
		if err := f.runtime.db.NewRaw("SELECT last_value FROM removal_persistent_fault").Scan(t.Context(), &failures); err != nil {
			t.Fatal(err)
		}
		return failures >= 2 && row.Status == model.TaskStatusRunning && calls.Load() == 1
	})
	stopHandlerEngine(t, cancel, done)
	removeFault()
	before := f.sends(t)
	if len(before) != 1 || before[0].TxHash != nil || len(before[0].Pieces) != 1 || before[0].AttemptedAt == nil || before[0].TaskID == nil {
		t.Fatalf("unknown request lost durable send authorization: %#v", before)
	}
	var cp struct {
		RequestID string    `json:"request_id"`
		SendNo    int       `json:"send_no"`
		Attempted time.Time `json:"attempted_at"`
	}
	if err := json.Unmarshal(f.row.Checkpoint, &cp); err != nil || cp.RequestID != f.request.RequestID || cp.SendNo != 1 || !cp.Attempted.Equal(*before[0].AttemptedAt) {
		t.Fatalf("checkpoint disagrees with send ledger: %#v, %v", cp, err)
	}
	if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("lease_until = ?", time.Now().Add(-time.Second)).Where("id = ?", f.row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.step(t, model.TaskStatusPending)
	if calls.Load() != 1 || f.row.RetryCount != 0 || len(f.sends(t)) != 1 {
		t.Fatalf("recovery sent before its deadline: %#v, calls=%d", f.row, calls.Load())
	}
}

func TestRemoval404PartitionsOnlyUnacceptedBatchAndCountsEverySend(t *testing.T) {
	var calls, lookups atomic.Int64
	f := newRemovalFixture(t, 3, func(options *handlerRuntimeOptions) {
		options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
			return testCleanupContext{deletePieces: func(context.Context, []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				if calls.Add(1) == 1 {
					return nil, &pdp.HTTPError{StatusCode: 404, Body: "piece missing"}
				}
				return nil, removalUnknownError()
			}}, nil
		}
		options.storage.ProviderPiecesKnownFunc = func(_ context.Context, _ string, _ sdktypes.BigInt, ids []sdktypes.BigInt) (synapse.ProviderPiecesState, error) {
			switch lookups.Add(1) {
			case 1:
				return synapse.ProviderPiecesState{}, context.DeadlineExceeded
			case 2:
				return synapse.ProviderPiecesState{Known: ids[:1], Errors: map[string]error{
					ids[1].String(): &pdp.HTTPError{StatusCode: 503, Body: "provider busy at https://user:password@provider.example/pdp/check?token=private#fragment"},
					ids[2].String(): errors.New("dial tcp: connection refused"),
				}}, nil
			}
			return synapse.ProviderPiecesState{Known: ids[:1], Unknown: ids[1:]}, nil
		}
		options.storage.OpenProviderTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
			return &testutil.MockStorageTarget{ServiceURLValue: "https://provider.example"}, nil
		}
	})
	f.send(t)
	if calls.Load() != 1 || lookups.Load() != 1 || f.row.RetryCount != 0 || f.row.LastError == nil || !strings.Contains(*f.row.LastError, context.DeadlineExceeded.Error()) {
		t.Fatalf("failed piece lookup consumed a send attempt: task=%#v calls=%d lookups=%d", f.row, calls.Load(), lookups.Load())
	}
	if members, err := f.runtime.repos.StorageRemoval.ListRemovalMembers(t.Context(), f.request.RequestID); err != nil || len(members) != 3 {
		t.Fatalf("piece lookup timeout changed batch members: %#v, %v", members, err)
	}
	f.step(t, model.TaskStatusPending)
	if calls.Load() != 1 || lookups.Load() != 2 || f.row.RetryCount != 0 || f.row.LastError == nil {
		t.Fatalf("partial piece lookup failure consumed an attempt: task=%#v calls=%d lookups=%d", f.row, calls.Load(), lookups.Load())
	}
	for _, detail := range []string{"piece missing", "503", "provider busy", "connection refused", f.members[1].PieceID.String(), f.members[2].PieceID.String()} {
		if !strings.Contains(*f.row.LastError, detail) {
			t.Fatalf("piece lookup diagnostic lost %q: %s", detail, *f.row.LastError)
		}
	}
	for _, secret := range []string{"password", "token=private", "#fragment"} {
		if strings.Contains(*f.row.LastError, secret) {
			t.Fatalf("piece lookup diagnostic exposed a URL secret: %s", *f.row.LastError)
		}
	}
	f.step(t, model.TaskStatusPending)
	if calls.Load() != 1 || lookups.Load() != 3 {
		t.Fatalf("piece lookup recovery sent instead of observing: calls=%d lookups=%d", calls.Load(), lookups.Load())
	}
	first := f.sends(t)
	if f.row.RetryCount != 1 || f.request.Status != storageremoval.RequestStatusReady || first[0].RefusedAt == nil {
		t.Fatalf("refused send did not leave a bounded retry: %#v, %#v", f.row, first)
	}
	f.send(t)
	evidence := f.sends(t)
	if calls.Load() != 2 || len(evidence) != 2 || len(evidence[0].Pieces) != 3 || len(evidence[1].Pieces) != 1 || f.row.RetryCount != 1 {
		t.Fatalf("404 partition used multiple sends in one attempt or lost evidence: task=%#v sends=%#v", f.row, evidence)
	}
	for _, member := range f.members[1:] {
		var failed model.StorageCleanupCopy
		if err := f.runtime.db.NewSelect().Model(&failed).Where("id = ?", member.ID).Scan(t.Context()); err != nil || failed.Status != model.StorageCleanupCopyStatusFailed || failed.RemovalRequestID != nil {
			t.Fatalf("provider-missing member = %#v, %v", failed, err)
		}
		if failed.LastError == nil || !strings.Contains(*failed.LastError, "piece missing") {
			t.Fatalf("provider-missing member lost the DELETE diagnostic: %#v", failed)
		}
	}
}

func TestRemoval404FallbackBoundsSingletonSendsWithoutLosingMembers(t *testing.T) {
	for _, lookup := range []string{"all missing", "all errors"} {
		t.Run(lookup, func(t *testing.T) {
			maxAttempts := 3
			var mu sync.Mutex
			var sent [][]string
			f := newRemovalFixture(t, 2, func(options *handlerRuntimeOptions) {
				options.maxAttempts = &maxAttempts
				options.storage.OpenCleanupContextFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
					return testCleanupContext{deletePieces: func(_ context.Context, pieces []sdktypes.BigInt) (*sdktypes.WriteResult, error) {
						ids := make([]string, len(pieces))
						for i, piece := range pieces {
							ids[i] = piece.String()
						}
						mu.Lock()
						sent = append(sent, ids)
						mu.Unlock()
						return nil, &pdp.HTTPError{StatusCode: 404, Body: "piece missing"}
					}}, nil
				}
				options.storage.OpenProviderTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
					return &testutil.MockStorageTarget{ServiceURLValue: "https://provider.example"}, nil
				}
				options.storage.ProviderPiecesKnownFunc = func(_ context.Context, _ string, _ sdktypes.BigInt, pieces []sdktypes.BigInt) (synapse.ProviderPiecesState, error) {
					if lookup == "all missing" {
						return synapse.ProviderPiecesState{Unknown: pieces}, nil
					}
					errorsByID := make(map[string]error, len(pieces))
					for _, piece := range pieces {
						errorsByID[piece.String()] = &pdp.HTTPError{StatusCode: 503, Body: "piece lookup unavailable: connection refused at https://user:password@provider.example/pdp/check?token=private#fragment"}
					}
					return synapse.ProviderPiecesState{Errors: errorsByID}, nil
				}
			})
			for range 7 {
				f.advance(t)
				if f.row.Status == model.TaskStatusFailed {
					break
				}
			}
			mu.Lock()
			defer mu.Unlock()
			evidence := f.sends(t)
			if f.row.Status != model.TaskStatusFailed || f.row.FailureReason == nil || *f.row.FailureReason != "attempts_exhausted" || f.row.RetryCount != maxAttempts-1 || len(sent) != maxAttempts || len(evidence) != maxAttempts {
				t.Fatalf("fallback did not obey its send budget: task=%#v sends=%v evidence=%#v", f.row, sent, evidence)
			}
			if len(sent[0]) != 2 || len(sent[1]) != 1 || len(sent[2]) != 1 || sent[1][0] != sent[2][0] || sent[1][0] != sent[0][0] && sent[1][0] != sent[0][1] {
				t.Fatalf("fallback expanded an exposed authorization: %v", sent)
			}
			if f.request.Status != storageremoval.RequestStatusReady || f.request.TaskID == nil || *f.request.TaskID != f.row.ID || f.request.AttentionCode == nil || *f.request.AttentionCode != "provider_unreachable" {
				t.Fatalf("exhausted fallback lost its owner or diagnostic: %#v", f.request)
			}
			if lookup == "all errors" {
				if f.row.LastError == nil || f.request.LastError == nil {
					t.Fatalf("exhausted fallback lost lookup diagnostics: task=%#v batch=%#v", f.row, f.request)
				}
				for _, diagnostic := range []string{*f.row.LastError, *f.request.LastError} {
					for _, detail := range []string{"503", "piece lookup unavailable", "connection refused", sent[len(sent)-1][0]} {
						if !strings.Contains(diagnostic, detail) {
							t.Fatalf("fallback diagnostic lost %q: %s", detail, diagnostic)
						}
					}
					for _, secret := range []string{"password", "token=private", "#fragment"} {
						if strings.Contains(diagnostic, secret) {
							t.Fatalf("fallback diagnostic exposed a URL secret: %s", diagnostic)
						}
					}
				}
			}
			for i, send := range evidence {
				if send.SendNo != i+1 || send.RefusedAt == nil || send.TaskID == nil || *send.TaskID != f.row.ID || len(send.Pieces) != len(sent[i]) {
					t.Fatalf("fallback lost a refused authorization: %#v", send)
				}
			}
			var copies []model.StorageCleanupCopy
			if err := f.runtime.db.NewSelect().Model(&copies).Where("id IN (?, ?)", f.members[0].ID, f.members[1].ID).OrderExpr("id ASC").Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			if len(copies) != 2 || copies[0].Status != model.StorageCleanupCopyStatusPending || copies[1].Status != model.StorageCleanupCopyStatusPending || copies[0].RemovalRequestID == nil || copies[1].RemovalRequestID != nil {
				t.Fatalf("unavailable piece lookup discarded a member: %#v", copies)
			}
		})
	}
}
