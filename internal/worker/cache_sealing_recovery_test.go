package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestSealIntentAndCachePressureSurviveRuntimeRestart(t *testing.T) {
	for _, source := range []string{"manual", "cache pressure"} {
		t.Run(source, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			fs, err := cache.NewFilesystem(dir, 384)
			if err != nil {
				t.Fatal(err)
			}
			policy := cache.EvictionPolicyNone
			if source == "cache pressure" {
				policy = cache.EvictionPolicyAfterUpload
			}
			f := newRegistrationFixture(t, 2, nil, 30*time.Minute, func(options *handlerRuntimeOptions) {
				options.cache, options.policy, options.maxBytes, options.maxWriteBytes = fs, policy, 384, 256
				options.commitSealOnCachePressure = source == "cache pressure"
				options.register = func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := h.RegisterCore(registry); err != nil {
						return err
					}
					return h.RegisterStorage(registry)
				}
			})
			bucket, err := f.runtime.repos.Buckets.GetByID(ctx, f.copies[0].BucketID)
			if err != nil {
				t.Fatal(err)
			}
			for _, copyRow := range f.copies {
				if _, err := fs.Put(ctx, bucket.Name, model.ContentCacheKey(copyRow.ContentID), bytes.NewReader(make([]byte, 128)), 128); err != nil {
					t.Fatal(err)
				}
				if err := f.runtime.repos.Objects.RecordContentCacheCommit(ctx, copyRow.ContentID, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			requestID, taskID := f.collect(t)
			var plannerID int64
			if source == "manual" {
				if _, err := f.runtime.repos.Contents.RequestCommitSeal(ctx, requestID); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := fs.Put(ctx, bucket.Name, "next", bytes.NewReader(make([]byte, 256)), 256); !errors.Is(err, cache.ErrCacheFull) {
					t.Fatal(err)
				}
				planner, _, err := f.runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity"})
				if err != nil {
					t.Fatal(err)
				}
				plannerID = planner.ID
			}
			var signingUnavailable atomic.Bool
			signingUnavailable.Store(true)
			sign := f.provider.target.PresignForCommitFunc
			f.provider.target.PresignForCommitFunc = func(ctx context.Context, pieces []storage.PieceInput) ([]byte, error) {
				if signingUnavailable.Load() {
					return nil, errors.New("signer temporarily unavailable")
				}
				return sign(ctx, pieces)
			}
			cancel, done := runHandlerEngine(t, f.runtime)
			waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusPending && task.RetryCount > 0 })
			stopHandlerEngine(t, cancel, done)
			if got := f.request(t, requestID); got.Status != storagecommit.RequestStatusCollecting || (source == "manual" && got.SealRequestedAt == nil) {
				t.Fatalf("retry lost intent: %#v", got)
			}
			// Reopen the directory and rebuild handlers: no refusal latch or pressure
			// signal from the first runtime remains.
			reopened, err := cache.NewFilesystem(dir, 384)
			if err != nil {
				t.Fatal(err)
			}
			if reopened.ConsumeRefusedWriteBytes() != 0 || reopened.UsedBytes() != 256 {
				t.Fatal("invalid restarted cache state")
			}
			handlers, err := worker.NewTaskHandlers(worker.TaskHandlerDependencies{
				Repositories: repository.NewRepositories(f.runtime.db), Cache: reopened, CacheGate: f.runtime.gate, CacheTracker: f.runtime.tracker,
				Storage: f.runtime.storage, CommitNonces: f.provider.nonces, EvictionPolicy: policy, MaxCacheBytes: 384, MaxWriteBytes: 256,
				CommitMaxWait: 30 * time.Minute, CommitSealOnCachePressure: source == "cache pressure", DefaultCopies: 1, MaxRetries: 5, Logger: slog.Default(),
			})
			if err != nil {
				t.Fatal(err)
			}
			registry := taskengine.NewRegistry()
			if err := handlers.RegisterCore(registry); err != nil {
				t.Fatal(err)
			}
			if err := handlers.RegisterStorage(registry); err != nil {
				t.Fatal(err)
			}
			service, err := taskengine.NewService(registry, f.runtime.repos, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			handlers.SetTaskService(service)
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration, Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2}, f.runtime.repos, registry, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			f.runtime.engine = engine
			signingUnavailable.Store(false)
			wakeTask(t, f.runtime, taskID)
			if plannerID != 0 {
				wakeTask(t, f.runtime, plannerID)
			}
			cancel, done = runHandlerEngine(t, f.runtime)
			defer stopHandlerEngine(t, cancel, done)
			waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			if got := f.request(t, requestID); got.Status != storagecommit.RequestStatusConfirmed || got.SealRequestedAt != nil {
				t.Fatalf("restart did not finish sealing: %#v", got)
			}
		})
	}
}

func TestPressureSealsLaterReplicasUntilMinimumDurability(t *testing.T) {
	ctx := t.Context()
	fs, err := cache.NewFilesystem(t.TempDir(), 192)
	if err != nil {
		t.Fatal(err)
	}
	f := newRegistrationFixture(t, 1, nil, 30*time.Minute, func(options *handlerRuntimeOptions) {
		options.cache, options.policy, options.maxBytes, options.maxWriteBytes = fs, cache.EvictionPolicyAfterUpload, 192, 128
		options.commitSealOnCachePressure = true
		options.register = func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
			if err := h.RegisterCore(registry); err != nil {
				return err
			}
			return h.RegisterStorage(registry)
		}
	})
	ingress := f.copies[0]
	bucket, err := f.runtime.repos.Buckets.GetByID(ctx, ingress.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: new(3), SetMinimumDurableCopies: true, MinimumDurableCopies: new(3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("requested_copies = 3").Where("id = ?", ingress.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	targets := map[string]*registrationProvider{f.dataSet.DataSetID.String(): f.provider}
	var later []registrationFixture
	for slot := 1; slot < 3; slot++ {
		providerID := testOnChainID(t, 500000+int64(slot)*100+ingress.ID)
		binding, err := f.runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: providerID, CopyIndex: slot, CreatedByContentID: ingress.ContentID})
		if err != nil {
			t.Fatal(err)
		}
		chainID, clientID := testOnChainID(t, 600000+binding.ID), testOnChainID(t, 700000+binding.ID)
		if err := f.runtime.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: chainID, ClientDataSetID: &clientID}); err != nil {
			t.Fatal(err)
		}
		binding.DataSetID, binding.ClientDataSetID = &chainID, &clientID
		if err := f.runtime.repos.Contents.CreateUploadCopiesForBindings(ctx, ingress.ContentID, []repository.UploadCopyBindingInput{{StorageDataSetID: binding.ID, CopyIndex: slot, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: providerID}}); err != nil {
			t.Fatal(err)
		}
		copyRow, err := f.runtime.repos.Contents.GetUploadCopy(ctx, ingress.ContentID, slot)
		if err != nil {
			t.Fatal(err)
		}
		provider := newRegistrationProvider(t, providerID.SDK(), chainID.SDK(), clientID.SDK(), f.provider.nonces)
		var nonce atomic.Uint64
		nonce.Store(uint64(slot) * 1000)
		provider.target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
			return testutil.CommitExtraData(nonce.Add(1)), nil
		}
		targets[chainID.String()] = provider
		later = append(later, registrationFixture{runtime: f.runtime, provider: provider, dataSet: binding, copies: []*model.StorageCopy{copyRow}})
	}
	f.runtime.storage.OpenDataSetTargetFunc = func(_ context.Context, id sdktypes.BigInt, _ storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		if provider := targets[id.String()]; provider != nil {
			return provider.target, nil
		}
		return nil, fmt.Errorf("unknown test data set %s", id.String())
	}
	if _, err := fs.Put(ctx, bucket.Name, model.ContentCacheKey(ingress.ContentID), bytes.NewReader(make([]byte, 128)), 128); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.repos.Objects.RecordContentCacheCommit(ctx, ingress.ContentID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put(ctx, bucket.Name, "next", bytes.NewReader(make([]byte, 128)), 128); !errors.Is(err, cache.ErrCacheFull) {
		t.Fatal(err)
	}
	f.collect(t)
	if _, _, err := f.runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: systemtask.CacheCapacityKey, Input: systemtask.Input{}, SubjectType: "system", SubjectKey: "cache-capacity"}); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForCommitted(t, f.runtime, f.copies)
	for i, replica := range later {
		if !fs.Exists(ctx, bucket.Name, model.ContentCacheKey(ingress.ContentID)) {
			t.Fatalf("cache removed with only %d confirmed replicas", i+1)
		}
		if bytes, err := f.runtime.repos.CacheEvictions.ActiveEvictionBytes(ctx); err != nil || bytes != 0 {
			t.Fatalf("premature safe cleanup = %d, %v", bytes, err)
		}
		copyRow := replica.copies[0]
		if err := f.runtime.repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex, PieceCID: f.pieceCID(t, ingress), RequireEligibleCopy: true}); err != nil {
			t.Fatal(err)
		}
		replica.collect(t)
		waitForCommitted(t, f.runtime, replica.copies)
	}
	deadline := time.Now().Add(5 * time.Second)
	for fs.UsedBytes() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fs.UsedBytes() != 0 {
		t.Fatalf("cache did not release after three confirmations: %d", fs.UsedBytes())
	}
	if _, err := fs.Put(ctx, bucket.Name, "next", bytes.NewReader(make([]byte, 128)), 128); err != nil {
		t.Fatal(err)
	}
}
