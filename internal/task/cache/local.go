package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

const (
	LocalReconcileInterval = time.Hour
	localBatchSize         = 200
)

// LocalFilesystem exposes cache files that need reconciliation with durable
// state. The boolean selects quarantined files.
type LocalFilesystem interface {
	ListUploadIDs(context.Context) ([]string, error)
	WalkContentFiles(context.Context, bool, func(bucket string, contentID int64) error) error
	Quarantine(context.Context, string, int64) error
	DeleteQuarantined(context.Context, string, int64) error
}

type LocalDependencies struct {
	Repositories   *repository.Repositories
	Cache          cache.Cache
	CacheGate      *cacheaccess.Gate
	CacheTracker   *cacheaccess.Tracker
	Scheduler      *taskengine.Scheduler
	EvictionPolicy cache.EvictionPolicy
	Logger         *slog.Logger
}

type LocalHandler struct {
	*taskengine.FuncHandler
	deps  LocalDependencies
	files LocalFilesystem
}

func NewLocalHandler(deps LocalDependencies) (*LocalHandler, error) {
	if deps.Repositories == nil || deps.Repositories.CacheLocal == nil || deps.Repositories.Tasks == nil ||
		deps.Cache == nil || deps.CacheGate == nil || deps.CacheTracker == nil || deps.Scheduler == nil {
		return nil, errors.New("local cache handler requires repositories, cache, gate, tracker, and scheduler")
	}
	files, ok := deps.Cache.(LocalFilesystem)
	if !ok {
		return nil, errors.New("local cache handler requires filesystem reconciliation support")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &LocalHandler{deps: deps, files: files}
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheLocalReconcile, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec: taskengine.CycleCodec, Identity: taskengine.CycleIdentity("cache-local"), MaxConcurrency: 1,
		Policy:     taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy(), InvocationTimeout: time.Hour},
		AllowRetry: true,
	}
	h.FuncHandler = taskengine.NewFuncHandler(definition, h.run, h.run)
	return h, nil
}

func (h *LocalHandler) run(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	if _, err := taskengine.DecodeInput[taskengine.CycleInput](execution); err != nil {
		return decodeFailure(string(model.TaskTypeCacheLocalReconcile), err)
	}
	if err := h.reconcileUploads(ctx); err != nil {
		return retryTask(err, "cache_local_scan_failed")
	}
	for _, quarantined := range []bool{false, true} {
		if err := h.reconcileFiles(ctx, execution, quarantined); err != nil {
			return retryTask(err, "cache_local_reconcile_failed")
		}
	}
	return taskengine.CompleteCycle("Local cache checked", nil)
}

func (h *LocalHandler) reconcileUploads(ctx context.Context) error {
	uploadIDs, err := h.files.ListUploadIDs(ctx)
	if err != nil {
		return err
	}
	for _, uploadID := range uploadIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		release, held := h.deps.CacheGate.TryHoldUpload(uploadID)
		if !held {
			continue
		}
		upload, err := h.deps.Repositories.Multiparts.GetByUploadID(ctx, uploadID)
		if err != nil {
			release()
			return err
		}
		if upload == nil || upload.Status == model.MultipartStatusCompleted || upload.Status == model.MultipartStatusAborted {
			if err := h.deps.Cache.DeleteUpload(ctx, uploadID); err != nil {
				h.deps.Logger.Warn("could not remove unused multipart files", "uploadID", uploadID, "error", err)
			}
		}
		release()
	}
	return nil
}

type localFile struct {
	bucket    string
	contentID int64
}

// localFileError lets a damaged filesystem entry be skipped without masking a
// database or lease failure that must retry the cycle.
type localFileError struct{ error }

func (h *LocalHandler) reconcileFiles(ctx context.Context, execution taskengine.Execution, quarantined bool) error {
	batch := make([]localFile, 0, localBatchSize)
	flush := func() error {
		ids := make([]int64, len(batch))
		for i, file := range batch {
			ids[i] = file.contentID
		}
		states, err := h.deps.Repositories.CacheLocal.ListStates(ctx, ids)
		if err != nil {
			return err
		}
		for _, file := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			state, exists := states[file.contentID]
			if exists && state.BucketName != file.bucket {
				continue
			}
			if quarantined {
				err = h.reconcileQuarantined(ctx, execution, file)
			} else {
				err = h.reconcileContent(ctx, execution, file, state, exists)
			}
			if err != nil {
				if fileErr, ok := errors.AsType[localFileError](err); ok && ctx.Err() == nil {
					h.deps.Logger.Warn("could not reconcile cached file", "bucket", file.bucket, "contentID", file.contentID, "error", fileErr.error)
					continue
				}
				return err
			}
		}
		batch = batch[:0]
		return nil
	}
	if err := h.files.WalkContentFiles(ctx, quarantined, func(bucket string, contentID int64) error {
		batch = append(batch, localFile{bucket: bucket, contentID: contentID})
		if len(batch) == localBatchSize {
			return flush()
		}
		return ctx.Err()
	}); err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	return flush()
}

func (h *LocalHandler) reconcileContent(ctx context.Context, execution taskengine.Execution, file localFile, state repository.CacheLocalState, exists bool) error {
	key := model.ContentCacheKey(file.contentID)
	if !exists {
		release, held := h.deps.CacheGate.TryHoldDeletion(key)
		if !held {
			return nil
		}
		defer release()
		return h.releaseOrphan(ctx, execution, file)
	}
	if state.BucketName != file.bucket || state.InCache || !state.Referenced {
		return nil
	}
	release, held := h.deps.CacheGate.TryHoldRead(key)
	if !held {
		return nil
	}
	matches, err := h.matches(ctx, file, state)
	if err != nil {
		release()
		if os.IsNotExist(err) {
			return nil
		}
		return localFileError{err}
	}
	if matches {
		defer release()
		return h.withClaim(ctx, execution, func(tx *repository.Repositories) error {
			restored, err := tx.CacheLocal.RestorePresence(ctx, state)
			if err != nil || !restored || h.deps.EvictionPolicy != cache.EvictionPolicyAfterUpload {
				return err
			}
			_, err = enqueueEviction(ctx, tx, h.deps.Scheduler, file.contentID, nil)
			return err
		})
	}
	release()
	release, held = h.deps.CacheGate.TryHoldDeletion(key)
	if !held {
		return nil
	}
	defer release()
	states, err := h.deps.Repositories.CacheLocal.ListStates(ctx, []int64{file.contentID})
	if err != nil {
		return err
	}
	current, exists := states[file.contentID]
	if !exists {
		return h.releaseOrphan(ctx, execution, file)
	}
	if current.BucketName != file.bucket {
		return nil
	}
	matches, err = h.matches(ctx, file, current)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return localFileError{err}
	}
	if matches {
		return nil
	}
	if err := h.files.Quarantine(ctx, file.bucket, file.contentID); err != nil {
		return localFileError{err}
	}
	return h.clearPresence(ctx, execution, file.contentID)
}

func (h *LocalHandler) matches(ctx context.Context, file localFile, state repository.CacheLocalState) (bool, error) {
	body, _, err := h.deps.Cache.Get(ctx, file.bucket, model.ContentCacheKey(file.contentID))
	if err != nil {
		return false, err
	}
	if body == nil {
		return false, errors.New("cache returned an empty read handle")
	}
	hasher := sha256.New()
	var size int64
	var readErr error
	buffer := make([]byte, 64*1024)
	for {
		if readErr = ctx.Err(); readErr != nil {
			break
		}
		n, err := body.Read(buffer)
		_, _ = hasher.Write(buffer[:n])
		size += int64(n)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	if err := errors.Join(readErr, body.Close()); err != nil {
		return false, err
	}
	return size == state.ContentSize && hex.EncodeToString(hasher.Sum(nil)) == state.Checksum, nil
}

func (h *LocalHandler) clearPresence(ctx context.Context, execution taskengine.Execution, contentID int64) error {
	if err := h.withClaim(ctx, execution, func(tx *repository.Repositories) error {
		return tx.Objects.ClearContentCachePresence(ctx, contentID)
	}); err != nil {
		return err
	}
	h.deps.CacheTracker.Forget(contentID)
	return nil
}

func (h *LocalHandler) releaseOrphan(ctx context.Context, execution taskengine.Execution, file localFile) error {
	var released bool
	err := h.withClaim(ctx, execution, func(tx *repository.Repositories) error {
		var err error
		released, err = tx.Objects.ReleaseContentCacheIfUnreferenced(ctx, file.contentID, func() error {
			if err := h.deps.Cache.Delete(ctx, file.bucket, model.ContentCacheKey(file.contentID)); err != nil {
				return localFileError{err}
			}
			return nil
		})
		return err
	})
	if err == nil && released {
		h.deps.CacheTracker.Forget(file.contentID)
	}
	return err
}

func (h *LocalHandler) reconcileQuarantined(ctx context.Context, execution taskengine.Execution, file localFile) error {
	release, held := h.deps.CacheGate.TryHoldDeletion(model.ContentCacheKey(file.contentID))
	if !held {
		return nil
	}
	defer release()
	body, _, err := h.deps.Cache.Get(ctx, file.bucket, model.ContentCacheKey(file.contentID))
	if os.IsNotExist(err) {
		if err := h.clearPresence(ctx, execution, file.contentID); err != nil {
			return err
		}
	} else if err != nil {
		return localFileError{err}
	} else if body == nil {
		return localFileError{errors.New("cache returned an empty read handle")}
	} else if err := body.Close(); err != nil {
		return localFileError{err}
	}
	return h.withClaim(ctx, execution, func(tx *repository.Repositories) error {
		_, err := tx.CacheLocal.DeleteQuarantinedIfSafe(ctx, file.contentID, func() error {
			if err := h.files.DeleteQuarantined(ctx, file.bucket, file.contentID); err != nil {
				return localFileError{err}
			}
			return nil
		})
		return err
	})
}

func (h *LocalHandler) withClaim(ctx context.Context, execution taskengine.Execution, fn func(*repository.Repositories) error) error {
	return h.deps.Repositories.WithTx(ctx, func(tx *repository.Repositories) error {
		if err := tx.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration()); err != nil {
			return fmt.Errorf("validating local cache task: %w", err)
		}
		return fn(tx)
	})
}
