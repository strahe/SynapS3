package repository_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestPostgresLRUPlanUsesEffectiveAccessTimeAndCursor(t *testing.T) {
	db := testDB(t)
	capture := new(postgresQueryCapture)
	repos := repository.NewRepositories(db.WithQueryHook(capture))
	bucket := seedBucket(t, db, "lru-plan")
	ctx := t.Context()
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	if _, err := db.NewUpdate().Model(bucket).Set("minimum_durable_copies = 1").Where("id = ?", bucket.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, CopyIndex: 0, ProviderID: onChainID(t, "101"),
	})
	if err != nil {
		t.Fatal(err)
	}
	markSourceReady(t, repos, source.ID)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO storage_contents (id, bucket_id, checksum, content_size, piece_cid, requested_copies, created_at, updated_at)
		 SELECT value, ?, lpad(to_hex(value), 64, '0'), 10, 'lru-plan-piece', 1, ?, ?
		 FROM generate_series(1, 4096) AS series(value)`, []any{bucket.ID, base, base}},
		{`INSERT INTO object_cache (content_id, in_cache, cache_accessed_at, created_at, updated_at)
		 SELECT id, TRUE,
		   CASE WHEN id % 2 = 0 THEN NULL ELSE ?::timestamptz + ((id - 1) / 2) * interval '1 second' END,
		   ?::timestamptz + ((id - 1) / 2) * interval '1 second', ?
		 FROM storage_contents`, []any{base, base, base}},
		{`INSERT INTO storage_commit_requests (request_id, storage_data_set_id, status, piece_count,
		 extra_data_hex, sealed_at, first_sent_at, sends, refusals, first_piece_id, confirmed_at, created_at, updated_at)
		 SELECT 'lru-plan-' || ((id - 1) / 80), ?, 'confirmed', count(*),
		 'abcd', ?, ?, 1, 0, min(id)::text, ?, ?, ?
		 FROM storage_contents GROUP BY (id - 1) / 80`, []any{source.ID, base, base, base, base, base}},
		{`INSERT INTO storage_commit_request_pieces (request_id, position, content_id, storage_data_set_id, piece_cid, created_at)
		 SELECT 'lru-plan-' || ((id - 1) / 80), (id - 1) % 80, id, ?, piece_cid, ? FROM storage_contents`, []any{source.ID, base}},
		{`INSERT INTO storage_copies (content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id,
		 piece_id, transfer_method, status, retrieval_url, commit_request_id, commit_position, commit_request_status, created_at, updated_at)
		 SELECT id, bucket_id, content_size, ?, 0, '101', id::text, 'peer_pull', 'committed',
		 'https://provider.example/piece', 'lru-plan-' || ((id - 1) / 80), (id - 1) % 80, 'confirmed', ?, ?
		 FROM storage_contents`, []any{source.ID, base, base}},
	} {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM storage_copies WHERE content_id = 1"); err != nil {
		t.Fatal(err)
	}
	storeTarget, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, CopyIndex: 1, ProviderID: onChainID(t, "202"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO storage_copies
		(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, status, created_at, updated_at)
		VALUES (2, ?, 10, ?, 1, '202', 'ingress', 'pending', ?, ?)`, bucket.ID, storeTarget.ID, base, base); err != nil {
		t.Fatal(err)
	}
	storeVersion := newObjectVersion(bucket.ID, "unfinished-store", "lru-plan-store-version", 10)
	storeVersion.ContentID = new(int64(2))
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, storeVersion); err != nil {
		t.Fatal(err)
	}
	var replacementTarget int64
	if err := db.NewRaw(`INSERT INTO storage_data_sets
		(bucket_id, provider_id, copy_index, generation, is_current, data_set_id, status, created_at, updated_at)
		VALUES (?, '303', 0, 2, FALSE, '503', 'ready', ?, ?) RETURNING id`, bucket.ID, base, base).Scan(ctx, &replacementTarget); err != nil {
		t.Fatal(err)
	}
	var replacementID int64
	if err := db.NewRaw(`INSERT INTO storage_replacements
		(bucket_id, copy_index, source_data_set_id, target_data_set_id, selection_mode, client_request_id,
		price_list_fingerprint, status, created_at, updated_at)
		VALUES (?, 0, ?, ?, 'manual', 'lru-plan', 'lru-plan', 'migrating', ?, ?) RETURNING id`,
		bucket.ID, source.ID, replacementTarget, base, base).Scan(ctx, &replacementID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO storage_replacement_items
		(replacement_id, content_id, target_data_set_id, status, created_at, updated_at)
		VALUES (?, 3, ?, 'pending', ?, ?)`, replacementID, replacementTarget, base, base); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []struct {
		contentID int64
		taskType  model.TaskType
		status    model.TaskStatus
	}{
		{4, model.TaskTypeCacheEvict, model.TaskStatusPending},
		{5, model.TaskTypeCacheEvict, model.TaskStatusFailed},
		{6, model.TaskTypeStorageStore, model.TaskStatusFailed},
	} {
		task := repositoryTestTask(&model.Task{
			Type: owner.taskType, SubjectType: "storage_content", SubjectKey: strconv.FormatInt(owner.contentID, 10),
			InputVersion: 1, InputHash: "lru-plan", Input: []byte(`{}`), Status: owner.status,
			ResumeMode: model.TaskResumeModeExecute, AvailableAt: base, CreatedAt: base, UpdatedAt: base,
		})
		if owner.status == model.TaskStatusFailed {
			task.FinishedAt, task.LastError, task.FailureReason = &base, new("failed"), new("handler_panic")
		}
		if _, err := db.NewInsert().Model(task).Exec(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE object_cache SET cache_active_task_id = ? WHERE content_id = ?", task.ID, owner.contentID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "ANALYZE object_cache, storage_contents, storage_copies, storage_data_sets, storage_replacement_items, storage_replacements, object_versions, tasks, buckets"); err != nil {
		t.Fatal(err)
	}
	for _, cursorID := range []int64{0, 3680} {
		t.Run(fmt.Sprintf("after-%d", cursorID), func(t *testing.T) {
			capture.reset()
			var cursors []repository.CacheLRUCursor
			if cursorID > 0 {
				cursors = append(cursors, repository.CacheLRUCursor{ContentID: cursorID, AccessedAt: base.Add(time.Duration((cursorID-1)/2) * time.Second)})
			}
			candidates, err := repos.CacheEvictions.ListLRUCandidates(ctx, 32, cursors...)
			if err != nil || len(candidates) != 32 {
				t.Fatalf("LRU page = %#v, err=%v", candidates, err)
			}
			wantID := cursorID + 1
			for _, candidate := range candidates {
				for wantID <= 4 || wantID == 6 {
					wantID++
				}
				if candidate.ContentID != wantID || !candidate.AccessedAt.Equal(base.Add(time.Duration((wantID-1)/2)*time.Second)) {
					t.Fatalf("LRU candidate = %#v, want content %d with effective access time", candidate, wantID)
				}
				wantID++
			}
			plan := explainPostgresPlan(t, db, capture.last(t))
			conditions := planIndexConditions(plan, "idx_object_cache_lru")
			if len(conditions) == 0 {
				t.Fatal("LRU scan does not use the effective access-time index")
			}
			if cursorID > 0 {
				condition := strings.Join(conditions, " ")
				if !strings.Contains(condition, "ROW(") || !strings.Contains(condition, "COALESCE") || !strings.Contains(condition, "content_id") || !strings.Contains(condition, " > ") {
					t.Fatalf("LRU cursor is not an index condition: %v", conditions)
				}
			}
			assertLRUPlanHasNoAccessTimeSort(t, plan)
		})
	}
}

func assertLRUPlanHasNoAccessTimeSort(t *testing.T, plan postgresExecutionPlan) {
	t.Helper()
	for _, key := range plan.SortKeys {
		if strings.Contains(key, "COALESCE") && strings.Contains(key, "cache_accessed_at") {
			t.Fatalf("LRU plan sorts effective access times: %v", plan.SortKeys)
		}
	}
	for _, child := range plan.Children {
		assertLRUPlanHasNoAccessTimeSort(t, child)
	}
}
