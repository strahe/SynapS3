package repository_test

import (
	"slices"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestPostgresCommitQueueAndMembersPlans(t *testing.T) {
	db := testDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	stopped, _ := f.sealedRequest(t, "stopped")
	_, _ = f.sealedRequest(t, "backoff")
	_, mismatched := f.sealedRequest(t, "incomplete")
	first, second := f.transferredCopy(t, "first"), f.transferredCopy(t, "second")
	taskID := f.collecting(t, "eligible", first, second)
	f.seal(t, "eligible", taskID, second, first)
	for index, id := range []string{"stopped", "backoff", "incomplete", "eligible"} {
		if _, err := db.NewUpdate().Model((*storagecommit.Request)(nil)).
			Set("sealed_at = ?", now.Add(time.Duration(index-4)*time.Minute)).
			Where("request_id = ?", id).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusFailed).
		Set("finished_at = ?", now).Set("failure_reason = ?", "handler_panic").Where("id = ?", stopped).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*storagecommit.Request)(nil)).Set("retry_at = ?", now.Add(time.Hour)).
		Where("request_id = ?", "backoff").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("status = ?", model.StorageCopyStatusPending).
		Set("commit_ready_at = NULL").Where("id = ?", mismatched.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO storage_commit_requests
		(request_id, storage_data_set_id, status, piece_count, sends, refusals, last_error, created_at, updated_at)
		SELECT 'history-' || n, ?, 'abandoned', 0, 0, 0, 'stopped', ?, ?
		FROM generate_series(1, 4096) AS n`, f.dataSetID, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `WITH contents AS (
		INSERT INTO storage_contents (bucket_id, checksum, content_size, requested_copies, created_at, updated_at)
		SELECT ?, md5('commit-plan-' || n) || md5('commit-plan-second-' || n), 10, 1, ?, ? FROM generate_series(1, 4096) AS n
		RETURNING id, bucket_id, content_size
	)
	INSERT INTO storage_copies
		(content_id, bucket_id, content_size, storage_data_set_id, copy_index, provider_id, transfer_method, status, created_at, updated_at)
	SELECT id, bucket_id, content_size, ?, 0, '701', 'ingress', 'pending', ?, ? FROM contents`,
		f.bucket.ID, now, now, f.dataSetID, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE storage_commit_requests, storage_copies, storage_contents, tasks"); err != nil {
		t.Fatal(err)
	}
	capture := new(postgresQueryCapture)
	testutil.AddQueryHook(db, capture)
	repos := repository.NewRepositories(db)
	state, err := repos.Contents.CommitQueueState(ctx, f.dataSetID, now)
	if err != nil || state.Submitted != 0 || state.ReadyHead != "eligible" {
		t.Fatalf("queue = %#v, err=%v, want eligible after all three inadmissible requests", state, err)
	}
	queue := explainPostgresPlan(t, db, capture.last(t))
	if len(planIndexConditions(queue, "idx_storage_commit_requests_data_set_status")) == 0 &&
		len(planIndexConditions(queue, "idx_storage_commit_requests_status_created")) == 0 {
		t.Fatalf("ready queue did not use a supporting request index: %#v", queue)
	}
	capture.reset()
	members, err := repos.Contents.ListCommitRequestMembers(ctx, "eligible")
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, len(members))
	for index, member := range members {
		ids[index] = member.ID
	}
	if !slices.Equal(ids, []int64{second.ID, first.ID}) {
		t.Fatalf("member order = %v, want signed position order %v", ids, []int64{second.ID, first.ID})
	}
	memberPlan := explainPostgresPlan(t, db, capture.last(t))
	if len(planIndexConditions(memberPlan, "idx_storage_copies_commit_request")) == 0 {
		t.Fatalf("members did not use the request index: %#v", memberPlan)
	}
}
