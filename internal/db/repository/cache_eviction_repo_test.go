package repository_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestPrepareEvictionReusesOnlyMatchingLiveOwner(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "cache-eviction-reservation")
	ctx := t.Context()
	seedCachedContent := func(checksum string) int64 {
		t.Helper()
		contentID := seedContent(t, repos, bucket.ID, checksum, 10)
		if err := repos.Objects.RecordContentCacheCommit(ctx, contentID, time.Now()); err != nil {
			t.Fatalf("record cache presence: %v", err)
		}
		return contentID
	}

	enqueue := func(contentID, generation int64, taskType model.TaskType, subjectType, subjectKey string) *model.Task {
		t.Helper()
		input, err := json.Marshal(cacheeviction.EvictInput{ContentID: contentID, Generation: generation})
		if err != nil {
			t.Fatalf("marshal task input: %v", err)
		}
		row, created, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{
			Type: taskType, IdempotencyKey: cacheeviction.EvictTaskKey(contentID, generation),
			InputVersion: 1, InputHash: fmt.Sprintf("hash-%d-%d", contentID, generation),
			Input: input, SubjectType: &subjectType, SubjectKey: &subjectKey,
		}))
		if err != nil || !created {
			t.Fatalf("enqueue task = %#v created=%v err=%v", row, created, err)
		}
		return row
	}

	matchingContentID := seedCachedContent("matching-cache-owner")
	matching, err := repos.CacheEvictions.PrepareEviction(ctx, matchingContentID)
	if err != nil || matching.Generation != 1 || matching.ActiveTaskID != nil {
		t.Fatalf("initial matching reservation = %#v, err=%v", matching, err)
	}
	matchingTask := enqueue(
		matchingContentID,
		matching.Generation,
		model.TaskTypeCacheEvict,
		"storage_content",
		strconv.FormatInt(matchingContentID, 10),
	)
	if err := repos.CacheEvictions.BindEvictionTask(ctx, matchingContentID, matching.Generation, matchingTask.ID); err != nil {
		t.Fatalf("bind matching owner: %v", err)
	}
	reused, err := repos.CacheEvictions.PrepareEviction(ctx, matchingContentID)
	if err != nil || reused.Generation != matching.Generation || reused.ActiveTaskID == nil || *reused.ActiveTaskID != matchingTask.ID {
		t.Fatalf("reused reservation = %#v, err=%v", reused, err)
	}

	terminalContentID := seedCachedContent("terminal-cache-owner")
	terminal, err := repos.CacheEvictions.PrepareEviction(ctx, terminalContentID)
	if err != nil {
		t.Fatalf("prepare terminal owner: %v", err)
	}
	terminalTask := enqueue(
		terminalContentID,
		terminal.Generation,
		model.TaskTypeCacheEvict,
		"storage_content",
		strconv.FormatInt(terminalContentID, 10),
	)
	if err := repos.CacheEvictions.BindEvictionTask(ctx, terminalContentID, terminal.Generation, terminalTask.ID); err != nil {
		t.Fatalf("bind terminal owner: %v", err)
	}
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("failure_reason = ?", "test_terminal_owner").
		Set("finished_at = ?", time.Now()).
		Where("id = ?", terminalTask.ID).
		Exec(ctx); err != nil {
		t.Fatalf("mark owner terminal: %v", err)
	}
	next, err := repos.CacheEvictions.PrepareEviction(ctx, terminalContentID)
	if err != nil || next.Generation != terminal.Generation || next.ActiveTaskID == nil || *next.ActiveTaskID != terminalTask.ID {
		t.Fatalf("next reservation after terminal owner = %#v, err=%v", next, err)
	}
	entry, err := repos.CacheEvictions.GetCacheEntry(ctx, terminalContentID)
	if err != nil || entry == nil || entry.CacheActiveTaskID == nil || *entry.CacheActiveTaskID != terminalTask.ID {
		t.Fatalf("cache entry after terminal owner = %#v, err=%v", entry, err)
	}

	conflictContentID := seedCachedContent("conflicting-cache-owner")
	conflicting, err := repos.CacheEvictions.PrepareEviction(ctx, conflictContentID)
	if err != nil {
		t.Fatalf("prepare conflicting owner: %v", err)
	}
	conflictingTask := enqueue(
		conflictContentID,
		conflicting.Generation,
		model.TaskTypeCacheEvict,
		"object_version",
		"wrong-subject",
	)
	if err := repos.CacheEvictions.BindEvictionTask(ctx, conflictContentID, conflicting.Generation, conflictingTask.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("bind conflicting owner: %v, want ErrConflict", err)
	}
	// Existing invalid references must also be rejected during recovery.
	if _, err := db.NewUpdate().Model((*model.ObjectCache)(nil)).
		Set("cache_operation_generation = ?", conflicting.Generation).
		Set("cache_active_task_id = ?", conflictingTask.ID).
		Where("content_id = ?", conflictContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.CacheEvictions.PrepareEviction(ctx, conflictContentID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("prepare with mismatched live owner = %v, want ErrConflict", err)
	}

	generationConflictContentID := seedCachedContent("generation-conflicting-cache-owner")
	generationConflict, err := repos.CacheEvictions.PrepareEviction(ctx, generationConflictContentID)
	if err != nil {
		t.Fatalf("prepare generation-conflicting owner: %v", err)
	}
	generationConflictTask := enqueue(
		generationConflictContentID,
		generationConflict.Generation,
		model.TaskTypeCacheEvict,
		"storage_content",
		strconv.FormatInt(generationConflictContentID, 10),
	)
	badInput, err := json.Marshal(cacheeviction.EvictInput{
		ContentID:  generationConflictContentID,
		Generation: generationConflict.Generation + 1,
	})
	if err != nil {
		t.Fatalf("marshal mismatched generation input: %v", err)
	}
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).Set("input_json = ?", badInput).Where("id = ?", generationConflictTask.ID).Exec(ctx); err != nil {
		t.Fatalf("corrupt generation input: %v", err)
	}
	if err := repos.CacheEvictions.BindEvictionTask(
		ctx,
		generationConflictContentID,
		generationConflict.Generation,
		generationConflictTask.ID,
	); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("bind generation-conflicting owner: %v, want ErrConflict", err)
	}
	if _, err := db.NewUpdate().Model((*model.ObjectCache)(nil)).
		Set("cache_operation_generation = ?", generationConflict.Generation).
		Set("cache_active_task_id = ?", generationConflictTask.ID).
		Where("content_id = ?", generationConflictContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.CacheEvictions.PrepareEviction(ctx, generationConflictContentID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("prepare with mismatched task generation = %v, want ErrConflict", err)
	}
}

func TestLRUCandidateCursorUsesAccessTimeFallbackAndContentTieBreak(t *testing.T) {
	assertLRUCandidateCursorUsesAccessTimeFallbackAndContentTieBreak(t, testDB(t))
}

func assertLRUCandidateCursorUsesAccessTimeFallbackAndContentTieBreak(t *testing.T, db *bun.DB) {
	t.Helper()
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "lru-candidate-cursor")
	if _, err := db.NewUpdate().Model(bucket).Set("minimum_durable_copies = 1").Where("id = ?", bucket.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	var ids []int64
	for i := range 4 {
		contentID := seedContent(t, repos, bucket.ID, fmt.Sprintf("lru-cursor-%d", i), 10)
		providerID, dataSetID, pieceID := onChainID(t, "100"), onChainID(t, "200"), onChainID(t, fmt.Sprint(i))
		url := "https://provider.example/piece"
		seedCommittedUploadCopies(t, db, repos, bucket.ID, contentID, "bafk2bzaceclrupaging", []storageUploadCopySeed{{ProviderID: &providerID, DataSetID: &dataSetID, PieceID: &pieceID, RetrievalURL: &url}})
		if err := repos.Objects.RecordContentCacheCommit(t.Context(), contentID, base); err != nil {
			t.Fatal(err)
		}
		accessedAt := base.Add(time.Duration(max(i-1, 0)) * time.Minute)
		query := db.NewUpdate().Model((*model.ObjectCache)(nil)).Set("created_at = ?", accessedAt).Where("content_id = ?", contentID)
		if i%2 == 0 {
			query = query.Set("cache_accessed_at = NULL")
		} else {
			query = query.Set("cache_accessed_at = ?", accessedAt)
		}
		if _, err := query.Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, contentID)
	}
	if reclaimable, err := repos.CacheEvictions.ReclaimableLRUBytes(t.Context()); err != nil || reclaimable != 40 {
		t.Fatalf("reclaimable LRU bytes = %d, err=%v, want 40", reclaimable, err)
	}
	first, err := repos.CacheEvictions.ListLRUCandidates(t.Context(), 2)
	if err != nil || len(first) != 2 || first[0].ContentID != ids[0] || first[1].ContentID != ids[1] || !first[0].AccessedAt.Equal(base) {
		t.Fatalf("first LRU page = %#v, err=%v", first, err)
	}
	second, err := repos.CacheEvictions.ListLRUCandidates(t.Context(), 2, repository.CacheLRUCursor{AccessedAt: first[1].AccessedAt, ContentID: first[1].ContentID})
	if err != nil || len(second) != 2 || second[0].ContentID != ids[2] || second[1].ContentID != ids[3] {
		t.Fatalf("second LRU page = %#v, err=%v", second, err)
	}
	last := second[len(second)-1]
	if tail, err := repos.CacheEvictions.ListLRUCandidates(t.Context(), 2, repository.CacheLRUCursor{AccessedAt: last.AccessedAt, ContentID: last.ContentID}); err != nil || len(tail) != 0 {
		t.Fatalf("LRU tail = %#v, err=%v", tail, err)
	}
}
