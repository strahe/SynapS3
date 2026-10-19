package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
)

var (
	errStoreIdentity = errors.New("cached bytes differ from the expected storage identity")
	errStoreResult   = errors.New("storage provider returned a mismatched piece identity or size")
	errStoreReady    = errors.New("upload is already published")
	errStoreBusy     = errors.New("upload session is still processing")
)

type storeBody struct {
	*storeContextReader
	release func()
}

func (b *storeBody) Close() error {
	err := b.storeContextReader.Close()
	b.release()
	return err
}

type storeContextReader struct {
	ctx       context.Context
	body      io.ReadCloser
	once      sync.Once
	closeOnce sync.Once
	readMu    sync.Mutex
	reads     sync.WaitGroup
	closed    bool
	closeErr  error
	stop      func() bool
	done      chan struct{}
}

func newStoreContextReader(ctx context.Context, body io.ReadCloser) *storeContextReader {
	r := &storeContextReader{ctx: ctx, body: body, done: make(chan struct{})}
	r.stop = context.AfterFunc(ctx, func() { r.closeBody(); close(r.done) })
	return r
}

func (r *storeContextReader) Read(data []byte) (int, error) {
	r.readMu.Lock()
	if r.closed {
		r.readMu.Unlock()
		return 0, io.ErrClosedPipe
	}
	r.reads.Add(1)
	r.readMu.Unlock()
	defer r.reads.Done()
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.body.Read(data)
}

func (r *storeContextReader) closeBody() {
	r.once.Do(func() { r.closeErr = r.body.Close() })
}

func (r *storeContextReader) Close() error {
	r.closeOnce.Do(func() {
		r.readMu.Lock()
		r.closed = true
		r.readMu.Unlock()
		if !r.stop() {
			<-r.done
		}
		r.closeBody()
		r.reads.Wait()
	})
	return r.closeErr
}

func (h *StoreHandler) openStoreBody(ctx context.Context, execution taskengine.Execution, copyRow *model.StorageCopy, content *model.StorageContent, bucket *model.Bucket, expected string) (*storeBody, cid.Cid, error) {
	if h.deps.Cache == nil || h.deps.CacheGate == nil {
		return nil, cid.Undef, errors.New("cache reader is unavailable")
	}
	key := model.ContentCacheKey(content.ID)
	release, err := h.deps.CacheGate.HoldRead(ctx, key)
	if err != nil {
		return nil, cid.Undef, err
	}
	keep := false
	defer func() {
		if !keep {
			release()
		}
	}()
	if err := h.CopyCoordinator.deps.Repositories.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration()); err != nil {
		return nil, cid.Undef, err
	}
	raw, _, err := h.deps.Cache.Get(ctx, bucket.Name, key)
	if err != nil {
		return nil, cid.Undef, err
	}
	reader := newStoreContextReader(ctx, raw)
	checksum := sha256.New()
	info, calculateErr := piece.Calculate(io.TeeReader(reader, checksum))
	closeErr := reader.Close()
	if err := ctx.Err(); err != nil {
		return nil, cid.Undef, err
	}
	if calculateErr != nil {
		return nil, cid.Undef, calculateErr
	}
	if closeErr != nil {
		return nil, cid.Undef, closeErr
	}
	if !info.CIDv2.Defined() || content.ContentSize < 0 || info.RawSize != uint64(content.ContentSize) ||
		hex.EncodeToString(checksum.Sum(nil)) != content.Checksum ||
		(content.PieceCID != nil && info.CIDv2.String() != *content.PieceCID) ||
		(expected != "" && info.CIDv2.String() != expected) {
		return nil, cid.Undef, errStoreIdentity
	}
	raw, _, err = h.deps.Cache.Get(ctx, bucket.Name, key)
	if err != nil {
		return nil, cid.Undef, err
	}
	keep = true
	return &storeBody{storeContextReader: newStoreContextReader(ctx, raw), release: release}, info.CIDv2, nil
}

func (h *StoreHandler) storeBodyFailure(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy, hasCheckpoint bool, err error) taskengine.Result {
	if errors.Is(err, errStoreIdentity) {
		if hasCheckpoint || copyRow.IngressStoreAttempt > 0 {
			return taskengine.Fail(err, "store_identity_mismatch", nil)
		}
		return h.failCopyTask(execution, input, copyRow, err, "store_identity_mismatch")
	}
	if os.IsNotExist(err) {
		if result, stop := h.storeMissingCache(ctx, execution, input, copyRow, hasCheckpoint); stop {
			return result
		}
		return taskengine.Wait(model.TaskResumeModeExecute, "source", "Waiting for retained cache data", nil)
	}
	return h.storeCacheFailure(execution, hasCheckpoint, err)
}

func (h *StoreHandler) executeStoreSession(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy, target synapse.DataSetTarget, content *model.StorageContent, bucket *model.Bucket, checkpoint storeCheckpoint, hasCheckpoint bool, prepared *storeBody) taskengine.Result {
	pieceCID, err := cid.Parse(checkpoint.IntendedPieceCID)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	var progress *uploadProgressReporter
	defer func() { progress.Close() }()
	previousObservationStartedAt := checkpoint.ObservationStartedAt
	candidate := checkpoint
	candidate.ObservationStartedAt = time.Time{}
	requestStarted := false
	attempted, err := execution.WithCheckpointedEffect(ctx, "store:"+checkpoint.IntendedPieceCID, candidate, nil, func(ctx context.Context) error {
		checkpoint = candidate
		if checkpoint.Stage == storeStageExpired {
			checkpoint.SessionID, checkpoint.Stage = "", storeStageCreate
			checkpoint.ProviderServiceURL = target.ServiceURL()
		}
		if checkpoint.SessionID == "" {
			checkpoint.AttemptedAt = time.Now().UTC()
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return err
			}
			requestStarted = true
			id, err := target.CreateStoreSession(ctx)
			if err != nil {
				return err
			}
			checkpoint.SessionID, checkpoint.Stage = id, storeStageUpload
			checkpoint.UploadsIncomplete = true
			if err := validateStoreCheckpoint(checkpoint, content.ContentSize); err != nil {
				return err
			}
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return err
			}
		} else {
			checkpoint.Stage = storeStageFinalize
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return err
			}
			requestStarted = true
			err := target.FinalizeStoreSession(ctx, checkpoint.ProviderServiceURL, checkpoint.SessionID, pieceCID)
			if err == nil {
				return h.storePublished(ctx, execution, &checkpoint)
			}
			if errors.Is(err, synapse.ErrStoreSessionMissing) {
				return h.storeSessionMissing(ctx, execution, target, &checkpoint, pieceCID, synapse.ErrStoreSessionMissing)
			}
			if !errors.Is(err, synapse.ErrStoreSessionIncomplete) {
				return err
			}
		}
		body := prepared
		if body == nil {
			var err error
			body, _, err = h.openStoreBody(ctx, execution, copyRow, content, bucket, checkpoint.IntendedPieceCID)
			if err != nil {
				return err
			}
			defer func() { _ = body.Close() }()
		}
		claimable := checkpoint.UploadsIncomplete
		uploadCheckpoint := checkpoint
		uploadCheckpoint.Stage = storeStageUpload
		uploadCheckpoint.UploadsIncomplete = false
		var progressSettlement taskengine.Settlement
		if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress || copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
			uploadCheckpoint.IngressAttempt = copyRow.IngressStoreAttempt + 1
			progressSettlement = func(ctx context.Context, repos *repository.Repositories) error {
				_, err := repos.Contents.BeginIngressStoreProgress(ctx, repository.BeginIngressStoreProgressInput{CopyID: copyRow.ID, Generation: input.Generation, TaskID: execution.ID(), Attempt: uploadCheckpoint.IngressAttempt})
				return err
			}
		}
		if err := execution.WriteCheckpointWith(ctx, uploadCheckpoint, progressSettlement); err != nil {
			return err
		}
		checkpoint = uploadCheckpoint
		options := &storage.StoreOptions{PieceCID: pieceCID}
		if progressSettlement != nil {
			progress = h.newIngressProgressReporter(ctx, execution.ID(), input.Generation, copyRow.ID, checkpoint.IngressAttempt, content, bucket)
			options.OnProgress = progress.OnProgress
		}
		requestStarted = true
		stored, err := target.UploadStoreSession(ctx, checkpoint.ProviderServiceURL, checkpoint.SessionID, body.storeContextReader, options)
		if errors.Is(err, synapse.ErrStoreSessionMissing) {
			return h.storeSessionMissing(ctx, execution, target, &checkpoint, pieceCID, synapse.ErrStoreSessionMissing)
		}
		if errors.Is(err, synapse.ErrStoreSessionClaimed) {
			checkpoint.UploadsIncomplete = claimable
			if claimable {
				return h.storeSessionMissing(ctx, execution, target, &checkpoint, pieceCID, err)
			}
			return err
		}
		if err != nil {
			checkpoint.UploadsIncomplete = claimable && errors.Is(err, synapse.ErrStoreUploadTruncated)
			return err
		}
		if stored == nil || !stored.PieceCID.Equals(pieceCID) || stored.Size != content.ContentSize {
			return errStoreResult
		}
		progress.Flush(content.ContentSize, true)
		checkpoint.Stage = storeStageFinalize
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return err
		}
		requestStarted = true
		err = target.FinalizeStoreSession(ctx, checkpoint.ProviderServiceURL, checkpoint.SessionID, pieceCID)
		if errors.Is(err, synapse.ErrStoreSessionMissing) {
			return h.storeSessionMissing(ctx, execution, target, &checkpoint, pieceCID, synapse.ErrStoreSessionMissing)
		}
		if errors.Is(err, synapse.ErrStoreSessionIncomplete) {
			return errStoreBusy
		}
		if err != nil {
			return err
		}
		return h.storePublished(ctx, execution, &checkpoint)
	})
	if !attempted {
		if errors.Is(err, taskengine.ErrEffectAlreadyAdmitted) {
			return taskengine.RetryInMode(err, "store_resend_required", model.TaskResumeModeRecover, nil)
		}
		if errors.Is(err, repository.ErrTaskOperationUnresolved) || errors.Is(err, taskengine.ErrEffectForbidden) {
			return taskengine.Fail(err, "store_checkpoint_conflict", nil)
		}
		if !hasCheckpoint && copyRow.IngressStoreAttempt > 0 {
			return taskengine.Fail(err, "store_checkpoint_write_failed", nil)
		}
		return retryTask(err, "store_checkpoint_write_failed")
	}
	if !requestStarted {
		checkpoint.ObservationStartedAt = previousObservationStartedAt
	}
	if errors.Is(err, errStoreReady) {
		return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceCID), pieceCID, "")
	}
	if err == nil {
		return h.recoverStore(ctx, execution, input, copyRow, target, checkpoint)
	}
	if errors.Is(err, synapse.ErrStoreSessionClaimed) && checkpoint.Stage != storeStageExpired {
		return retryStoreCheckpoint(execution, checkpoint, err, "store_resend_required")
	}
	if errors.Is(err, errStoreBusy) {
		return h.waitForStoreStatus(ctx, execution, checkpoint, synapse.ParkedPieceProcessing, nil)
	}
	settlement, encodeErr := storeCheckpointSettlement(execution, checkpoint)
	if encodeErr != nil {
		return taskengine.Fail(encodeErr, "invalid_checkpoint", nil)
	}
	if errors.Is(err, errStoreIdentity) {
		return taskengine.Fail(err, "store_identity_mismatch", settlement)
	}
	if errors.Is(err, errStoreResult) {
		return taskengine.Fail(err, "store_result_invalid", settlement)
	}
	if os.IsNotExist(err) {
		return h.storeBodyFailure(ctx, execution, input, copyRow, true, err)
	}
	var httpErr *pdp.HTTPError
	if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusBadRequest || httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden || httpErr.StatusCode == http.StatusRequestEntityTooLarge || httpErr.StatusCode == http.StatusUnprocessableEntity) {
		return taskengine.Fail(synapse.SummarizedError(err), "store_request_rejected", settlement)
	}
	if !requestStarted {
		return retryStoreCheckpoint(execution, checkpoint, err, "store_checkpoint_write_failed")
	}
	return retryStoreCheckpoint(execution, checkpoint, err, "provider_confirmation")
}

func storeCheckpointSettlement(execution taskengine.Execution, checkpoint storeCheckpoint) (taskengine.Settlement, error) {
	encoded, encodeErr := json.Marshal(checkpoint)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), encoded)
	}, nil
}

func retryStoreCheckpoint(execution taskengine.Execution, checkpoint storeCheckpoint, err error, reason string) taskengine.Result {
	settlement, encodeErr := storeCheckpointSettlement(execution, checkpoint)
	if encodeErr != nil {
		return taskengine.Fail(encodeErr, "invalid_checkpoint", nil)
	}
	if errors.Is(err, repository.ErrTaskIdentityContended) || errors.Is(err, repository.ErrRepositoryContended) {
		return taskengine.Wait(model.TaskResumeModeRecover, "database_contention", "Waiting to continue", settlement)
	}
	return taskengine.RetryInMode(synapse.SummarizedError(err), reason, model.TaskResumeModeRecover, settlement)
}

func (h *StoreHandler) storePublished(ctx context.Context, execution taskengine.Execution, checkpoint *storeCheckpoint) error {
	checkpoint.Stage = storeStageObserve
	return execution.WriteCheckpoint(ctx, *checkpoint)
}

func (h *StoreHandler) storeSessionMissing(ctx context.Context, execution taskengine.Execution, target synapse.DataSetTarget, checkpoint *storeCheckpoint, pieceCID cid.Cid, cause error) error {
	state, err := h.storePieceState(ctx, target, *checkpoint, pieceCID)
	if err != nil {
		return err
	}
	switch state {
	case synapse.ParkedPieceReady:
		return errStoreReady
	case synapse.ParkedPieceMissing:
		checkpoint.Stage = storeStageExpired
		if err := execution.WriteCheckpoint(ctx, *checkpoint); err != nil {
			return err
		}
		return cause
	case synapse.ParkedPieceProcessing:
		return errStoreBusy
	default:
		return fmt.Errorf("unexpected storage provider state %q", state)
	}
}
