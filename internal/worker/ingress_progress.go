package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

const (
	uploadProgressFlushInterval = time.Second
	uploadProgressWriteTimeout  = 2 * time.Second
)

type uploadProgressReporter struct {
	ctx       context.Context
	repos     *repository.Repositories
	publisher EventPublisher
	logger    *slog.Logger

	contentID  int64
	copyID     int64
	generation int64
	taskID     int64
	versionID  string
	bucketName string
	objectKey  string
	attempt    int

	mu           sync.Mutex
	lastFlush    time.Time
	pendingBytes int64
	pending      bool
	pendingTimer *time.Timer
	closed       bool
	writes       sync.WaitGroup
}

func (h *TaskHandlers) newIngressProgressReporter(
	ctx context.Context,
	taskID int64,
	generation int64,
	copyID int64,
	attempt int,
	content *model.StorageContent,
	bucket *model.Bucket,
) *uploadProgressReporter {
	if content == nil || copyID < 1 || generation < 1 || taskID < 1 || attempt < 1 {
		return nil
	}
	reporter := &uploadProgressReporter{
		ctx: ctx, repos: h.deps.Repositories, publisher: h.deps.Events, logger: h.deps.Logger,
		contentID: content.ID, copyID: copyID, generation: generation, taskID: taskID,
		attempt: attempt,
	}
	// The transfer belongs to the content; naming a version is only there to
	// give the progress event something a reader recognises, so a content with
	// no live version still reports progress.
	if version, err := h.deps.Repositories.Contents.GetLiveVersionForUpload(ctx, content.ID); err != nil {
		h.deps.Logger.Warn("failed to label ingress upload progress", "content_id", content.ID, "error", err)
	} else if version != nil {
		reporter.versionID = version.VersionID
		reporter.objectKey = version.Key
	}
	if bucket != nil {
		reporter.bucketName = bucket.Name
	}
	reporter.scheduleRecord(0, false)
	return reporter
}

func (r *uploadProgressReporter) OnProgress(bytesUploaded int64) {
	if r == nil || r.attempt <= 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	if r.lastFlush.IsZero() || now.Sub(r.lastFlush) >= uploadProgressFlushInterval {
		r.cancelPendingLocked()
		r.lastFlush = now
		r.scheduleRecordLocked(bytesUploaded, false)
		r.mu.Unlock()
		return
	}
	r.pendingBytes = bytesUploaded
	r.pending = true
	if r.pendingTimer == nil {
		r.pendingTimer = time.AfterFunc(uploadProgressFlushInterval-now.Sub(r.lastFlush), r.flushPending)
	}
	r.mu.Unlock()
}

func (r *uploadProgressReporter) Flush(bytesUploaded int64, done bool) {
	if r == nil || r.attempt <= 0 {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.cancelPendingLocked()
	r.writes.Add(1)
	r.mu.Unlock()
	r.record(bytesUploaded, done)
	r.writes.Done()
}

func (r *uploadProgressReporter) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.writes.Wait()
		return
	}
	r.closed = true
	r.cancelPendingLocked()
	r.mu.Unlock()
	r.writes.Wait()
}

func (r *uploadProgressReporter) flushPending() {
	r.mu.Lock()
	if r.closed || !r.pending {
		r.pendingTimer = nil
		r.mu.Unlock()
		return
	}
	bytesUploaded := r.pendingBytes
	r.pending = false
	r.pendingTimer = nil
	r.lastFlush = time.Now()
	r.scheduleRecordLocked(bytesUploaded, false)
	r.mu.Unlock()
}

func (r *uploadProgressReporter) cancelPendingLocked() {
	r.pending = false
	if r.pendingTimer != nil {
		r.pendingTimer.Stop()
		r.pendingTimer = nil
	}
}

func (r *uploadProgressReporter) scheduleRecord(bytesUploaded int64, done bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.scheduleRecordLocked(bytesUploaded, done)
}

func (r *uploadProgressReporter) scheduleRecordLocked(bytesUploaded int64, done bool) {
	r.writes.Go(func() {
		r.record(bytesUploaded, done)
	})
}

func (r *uploadProgressReporter) record(bytesUploaded int64, done bool) {
	ctx, cancel := context.WithTimeout(r.ctx, uploadProgressWriteTimeout)
	defer cancel()
	upload, err := r.repos.Contents.RecordIngressStoreProgress(ctx, repository.RecordIngressStoreProgressInput{
		CopyID: r.copyID, Generation: r.generation, TaskID: r.taskID,
		Attempt: r.attempt, BytesUploaded: bytesUploaded,
	})
	if err != nil {
		if errors.Is(err, repository.ErrConflict) {
			return
		}
		r.logger.Warn("failed to record ingress upload progress", "content_id", r.contentID, "attempt", r.attempt, "error", err)
		return
	}
	if r.publisher == nil || upload.ProgressUpdatedAt == nil {
		return
	}
	r.publisher.Publish("upload_progress_updated", map[string]any{
		"content_id": r.contentID, "task_id": r.taskID, "version_id": r.versionID,
		"bucket_name": r.bucketName, "object_key": r.objectKey,
		"progress": uploadProgressEventPayload(upload, done),
	})
}

func uploadProgressEventPayload(upload *model.StorageCopy, done bool) map[string]any {
	uploaded := max(upload.IngressBytesTransferred, 0)
	total := max(upload.ContentSize, 0)
	uploaded = min(uploaded, total)
	payload := map[string]any{
		"scope": "ingress_store", "attempt": upload.IngressStoreAttempt,
		"uploaded_bytes": uploaded, "total_bytes": total,
		"done":       done || (total > 0 && uploaded >= total),
		"updated_at": upload.ProgressUpdatedAt.Format(time.RFC3339),
	}
	if upload.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
		payload["scope"] = "cache_restore_store"
	}
	if percent := model.UploadProgressPercent(uploaded, total); percent != nil {
		payload["percent"] = *percent
	}
	return payload
}
