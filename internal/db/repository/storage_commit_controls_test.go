package repository_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
)

func testManualSealIsDurableAndIdempotent(t *testing.T, f commitFixture) {
	ctx := t.Context()
	copyRow := f.transferredCopy(t, "manual")
	taskID := f.collecting(t, "manual", copyRow)
	future := time.Now().Add(time.Hour)
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", future).Where("id = ?", taskID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := f.repos.Contents.RequestCommitSeal(ctx, "manual")
	if err != nil || first.SealRequestedAt == nil {
		t.Fatalf("request = %#v, %v", first, err)
	}
	stored := f.request(t, "manual").SealRequestedAt
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { _, err := f.repos.Contents.RequestCommitSeal(ctx, "manual"); errs <- err })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := f.request(t, "manual").SealRequestedAt; got == nil || !got.Equal(*stored) {
		t.Fatalf("repeated request changed intent: %v -> %v", stored, got)
	}
	task, err := f.repos.Tasks.GetByID(ctx, taskID)
	if err != nil || !task.AvailableAt.Before(future) {
		t.Fatalf("task wasn't woken: %#v, %v", task, err)
	}
	// A fresh repository observes the intent without any process-local state.
	if got, err := repository.NewRepositories(f.db).Contents.GetCommitRequest(ctx, "manual"); err != nil || got.SealRequestedAt == nil {
		t.Fatalf("reload = %#v, %v", got, err)
	}
	f.seal(t, "manual", taskID, copyRow)
	if got, err := f.repos.Contents.RequestCommitSeal(ctx, "manual"); err != nil || got.Status != storagecommit.RequestStatusReady || got.SealRequestedAt != nil {
		t.Fatalf("sealed replay = %#v, %v", got, err)
	}
	if _, err := f.repos.Contents.RequestCommitSeal(ctx, "absent"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("absent = %v", err)
	}

	emptyMember := f.transferredCopy(t, "empty")
	f.collecting(t, "empty", emptyMember)
	if _, err := f.repos.Contents.RequestCommitSeal(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{StorageCopyID: emptyMember.ID, ContentID: emptyMember.ContentID, CopyIndex: 0, LastError: "removed"}); err != nil {
		t.Fatal(err)
	}
	if got := f.request(t, "empty"); got.Status != storagecommit.RequestStatusAbandoned || got.SealRequestedAt != nil {
		t.Fatalf("empty request retained intent: %#v", got)
	}
	if _, err := f.repos.Contents.RequestCommitSeal(ctx, "empty"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("abandoned = %v", err)
	}

	stoppedMember := f.transferredCopy(t, "stopped-manual")
	stopped := f.collecting(t, "stopped-manual", stoppedMember)
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Set("failure_reason = ?", "handler_panic").Set("last_error = ?", "stopped").Where("id = ?", stopped).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repos.Contents.RequestCommitSeal(ctx, "stopped-manual"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stopped task = %v", err)
	}
	if f.request(t, "stopped-manual").SealRequestedAt != nil {
		t.Fatal("stopped task accepted manual intent")
	}
	queued := f.transferredCopy(t, "accepted-before-stop")
	queuedTask := f.collecting(t, "accepted-before-stop", queued)
	if _, err := f.repos.Contents.RequestCommitSeal(ctx, "accepted-before-stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Set("failure_reason = ?", "handler_panic").Set("last_error = ?", "stopped").Where("id = ?", queuedTask).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repos.Contents.RequestCommitSeal(ctx, "accepted-before-stop"); err != nil {
		t.Fatalf("repeated accepted intent = %v", err)
	}
	if task, err := f.repos.Tasks.GetByID(ctx, queuedTask); err != nil || task.Status != model.TaskStatusFailed {
		t.Fatalf("request revived stopped task: %#v, %v", task, err)
	}
	if _, err := f.repos.Contents.AbandonCommitRequest(ctx, repository.AbandonCommitRequestInput{RequestID: "accepted-before-stop", TaskID: queuedTask, Reason: "given up"}); err != nil {
		t.Fatal(err)
	}
	if got := f.request(t, "accepted-before-stop"); got.SealRequestedAt != nil || got.Status != storagecommit.RequestStatusAbandoned {
		t.Fatalf("abandon retained intent: %#v", got)
	}
}

func testCacheDependentCommitsAndSafeCleanupAccounting(t *testing.T, f commitFixture) {
	ctx := t.Context()
	if _, err := f.repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{Name: f.bucket.Name, SetMinimumDurableCopies: true, MinimumDurableCopies: new(1)}); err != nil {
		t.Fatal(err)
	}
	member := f.transferredCopy(t, "cached")
	taskID := f.collecting(t, "cached", member)
	if _, err := f.db.NewUpdate().Model((*model.ObjectCache)(nil)).Set("in_cache = ?", false).Where("content_id = ?", member.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if dependent, err := f.repos.Contents.HasCacheDependentCommitMembers(ctx, "cached"); err != nil || dependent {
		t.Fatalf("remote-only member = %v, %v", dependent, err)
	}
	if err := f.repos.Objects.RecordContentCacheCommit(ctx, member.ContentID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if dependent, err := f.repos.Contents.HasCacheDependentCommitMembers(ctx, "cached"); err != nil || !dependent {
		t.Fatalf("cached member = %v, %v", dependent, err)
	}
	future := time.Now().Add(time.Hour)
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", future).Where("id = ?", taskID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Contents.WakeCacheDependentCommitTasks(ctx); err != nil {
		t.Fatal(err)
	}
	task, err := f.repos.Tasks.GetByID(ctx, taskID)
	if err != nil || !task.AvailableAt.Before(future) {
		t.Fatalf("dependent task wasn't woken: %#v, %v", task, err)
	}
	if bytes, err := f.repos.CacheEvictions.ReclaimableLRUBytes(ctx); err != nil || bytes != 0 {
		t.Fatalf("unconfirmed bytes = %d, %v", bytes, err)
	}
	f.seal(t, "cached", taskID, member)
	if _, err := f.repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{RequestID: "cached", TaskID: taskID, FirstPieceID: onChainID(t, "101"), RetrievalURLs: []string{"https://provider.example/piece/cached"}}); err != nil {
		t.Fatal(err)
	}
	if bytes, err := f.repos.CacheEvictions.ReclaimableLRUBytes(ctx); err != nil || bytes != 10 {
		t.Fatalf("confirmed bytes = %d, %v", bytes, err)
	}
	entry, err := f.repos.CacheEvictions.GetCacheEntry(ctx, member.ContentID)
	if err != nil || entry == nil || entry.CacheAccessedAt == nil {
		t.Fatalf("cache entry = %#v, %v", entry, err)
	}
	reservation, err := f.repos.CacheEvictions.PrepareEviction(ctx, member.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(cacheeviction.EvictInput{ContentID: member.ContentID, Generation: reservation.Generation, AccessedAt: entry.CacheAccessedAt})
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := f.repos.Tasks.Enqueue(ctx, &model.Task{Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(member.ContentID, reservation.Generation), InputVersion: 1, InputHash: "cache-cleanup", Input: input, SubjectType: new(model.TaskSubjectStorageContent), SubjectKey: new(strconv.FormatInt(member.ContentID, 10))})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.CacheEvictions.BindEvictionTask(ctx, member.ContentID, reservation.Generation, owner.ID); err != nil {
		t.Fatal(err)
	}
	if bytes, err := f.repos.CacheEvictions.ActiveEvictionBytes(ctx); err != nil || bytes != 10 {
		t.Fatalf("safe scheduled bytes = %d, %v", bytes, err)
	}
	if bytes, err := f.repos.CacheEvictions.ReclaimableLRUBytes(ctx); err != nil || bytes != 0 {
		t.Fatalf("scheduled bytes counted twice = %d, %v", bytes, err)
	}
	if err := f.repos.Objects.RecordContentCacheAccess(ctx, member.ContentID, entry.CacheAccessedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if bytes, err := f.repos.CacheEvictions.ActiveEvictionBytes(ctx); err != nil || bytes != 0 {
		t.Fatalf("revoked scheduled bytes = %d, %v", bytes, err)
	}
}

func testBatchListUsesStablePagingAndSignedMembership(t *testing.T, f commitFixture) {
	ctx := t.Context()
	created := time.Now().UTC().Truncate(time.Second)
	a, b := f.transferredCopy(t, "page-a"), f.transferredCopy(t, "page-b")
	taskID := f.collectingAt(t, "a", created, a, b)
	before, err := f.repos.Contents.GetCommitBatch(ctx, "a")
	if err != nil || before.MemberCount != 2 || before.TotalBytes == nil || *before.TotalBytes != 20 || before.OldestReadyAt == nil {
		t.Fatalf("collecting batch = %#v, %v", before, err)
	}
	members, err := f.repos.Contents.ListCommitBatchMembers(ctx, "a")
	if err != nil || len(members) != 2 || members[0].ContentID != a.ContentID || members[0].PieceCID != "piece-page-a" || members[0].Size == nil || *members[0].Size != 10 {
		t.Fatalf("collecting members = %#v, %v", members, err)
	}
	f.seal(t, "a", taskID, a)
	f.collectingAt(t, "b", created, b)
	members, err = f.repos.Contents.ListCommitBatchMembers(ctx, "a")
	signedCID := "piece-" + strconv.FormatInt(a.ContentID, 10)
	if err != nil || len(members) != 1 || members[0].ContentID != a.ContentID || members[0].PieceCID != signedCID {
		t.Fatalf("signed members included spilled copies or live CID: %#v, %v", members, err)
	}
	first, err := f.repos.Contents.ListCommitBatches(ctx, repository.CommitBatchFilter{Limit: 1})
	if err != nil || len(first) != 1 || first[0].RequestID != "b" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := f.repos.Contents.ListCommitBatches(ctx, repository.CommitBatchFilter{Limit: 1, BeforeCreatedAt: first[0].CreatedAt, BeforeRequestID: first[0].RequestID})
	if err != nil || len(second) != 1 || second[0].RequestID != "a" || second[0].MemberCount != 1 || second[0].TotalBytes == nil || *second[0].TotalBytes != 10 {
		t.Fatalf("next page = %#v, %v", second, err)
	}
	filtered, err := f.repos.Contents.ListCommitBatches(ctx, repository.CommitBatchFilter{Limit: 20, Status: storagecommit.RequestStatusCollecting})
	if err != nil || len(filtered) != 1 || filtered[0].RequestID != "b" {
		t.Fatalf("filtered page = %#v, %v", filtered, err)
	}
	// Historical piece identities survive deletion of their content rows.
	if _, err := f.repos.Contents.AbandonCommitRequest(ctx, repository.AbandonCommitRequestInput{RequestID: "a", TaskID: taskID, Reason: "retired"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Object)(nil)).Set("current_version_id = NULL").Where("current_version_id IN (?)", f.db.NewSelect().Model((*model.ObjectVersion)(nil)).Column("version_id").Where("content_id = ?", a.ContentID)).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewDelete().Model((*model.ObjectVersion)(nil)).Where("content_id = ?", a.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewDelete().Model((*model.StorageCopy)(nil)).Where("content_id = ?", a.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewDelete().Table("object_cache").Where("content_id = ?", a.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.NewDelete().Model((*model.StorageContent)(nil)).Where("id = ?", a.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	history, err := f.repos.Contents.GetCommitBatch(ctx, "a")
	if err != nil || history.MemberCount != 1 || history.TotalBytes != nil {
		t.Fatalf("historical batch = %#v, %v", history, err)
	}
	members, err = f.repos.Contents.ListCommitBatchMembers(ctx, "a")
	if err != nil || len(members) != 1 || members[0].PieceCID != signedCID || members[0].Size != nil {
		t.Fatalf("historical member identity or missing size lost: %#v, %v", members, err)
	}
}
