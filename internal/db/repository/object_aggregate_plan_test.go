package repository_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func TestObjectAggregatesCountReadableReplicaSlotsAcrossGenerations(t *testing.T) {
	db := testDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	if _, err := db.NewUpdate().Model(f.bucket).Set("default_copies = 2").Set("minimum_durable_copies = 1").
		Where("id = ?", f.bucket.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	sourceCopy := f.transferredCopy(t, "slot-generations")
	testutil.CommitStorageCopy(t, db, f.repos, testutil.CommitCopyInput{
		StorageCopyID: sourceCopy.ID, PieceCID: "piece-slot-generations",
	})
	replacement, _ := f.activateReplacement(t)
	target, err := f.repos.Contents.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("target data set = %#v, %v", target, err)
	}
	if err := f.repos.Contents.CreateUploadCopiesForBindings(ctx, sourceCopy.ContentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID, CopyIndex: 0, ProviderID: target.ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull,
	}}); err != nil {
		t.Fatal(err)
	}
	targetCopy, err := f.repos.Contents.GetUploadCopyForDataSet(ctx, sourceCopy.ContentID, target.ID)
	if err != nil || targetCopy == nil {
		t.Fatalf("target copy = %#v, %v", targetCopy, err)
	}
	testutil.CommitStorageCopy(t, db, f.repos, testutil.CommitCopyInput{
		StorageCopyID: targetCopy.ID, PieceCID: "piece-slot-generations",
	})
	readable, err := f.repos.Contents.ListReadableCommittedCopies(ctx, sourceCopy.ContentID)
	if err != nil || len(readable) != 2 {
		t.Fatalf("readable generations = %#v, %v, want two physical copies", readable, err)
	}
	version, err := f.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, f.bucket.ID, "slot-generations")
	if err != nil || version == nil || version.State != model.ObjectStateReplicating {
		t.Fatalf("current object = %#v, %v, want one of two replica slots", version, err)
	}
	snapshot, err := f.repos.Objects.OverviewSnapshot(ctx)
	if err != nil || len(snapshot.States) != 1 || snapshot.States[0].State != string(version.State) ||
		snapshot.States[0].Count != 1 || snapshot.States[0].TotalSize != version.Size ||
		snapshot.Attention.NeedsAttention != 0 || snapshot.Attention.Unavailable != 0 {
		t.Fatalf("overview with two generations in one slot = %#v, %v", snapshot, err)
	}
	counts, err := f.repos.Objects.CountByState(ctx)
	if err != nil || len(counts) != 1 || counts[0].State != string(version.State) || counts[0].Count != 1 {
		t.Fatalf("state counts with two generations in one slot = %#v, %v", counts, err)
	}
}

func TestPostgresOverviewAggregatesCopyFactsOncePerSharedContent(t *testing.T) {
	db := testDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	const contentCount, objectCount, revisions = 4, 96, 12
	contentIDs := make([]int64, 0, contentCount)
	for index := range contentCount {
		contentID := seedContent(t, f.repos, f.bucket.ID, fmt.Sprintf("overview-plan-%d", index), 10)
		contentIDs = append(contentIDs, contentID)
		if err := f.repos.Contents.CreateUploadCopiesForBindings(ctx, contentID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: f.dataSetID, CopyIndex: 0, ProviderID: onChainID(t, "701"), TransferMethod: model.StorageCopyTransferMethodIngress,
		}}); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{
			`INSERT INTO objects (bucket_id, key, created_at, updated_at)
		 SELECT ?, 'shared-' || value, ?, ? FROM generate_series(1, ?) AS series(value)`,
			[]any{f.bucket.ID, base, base, objectCount},
		},
		{
			`INSERT INTO object_versions
		 (version_id, object_id, bucket_id, key, content_id, size, e_tag, content_type, created_at, updated_at)
		 SELECT object_row.key || '-v-' || revision, object_row.id, object_row.bucket_id, object_row.key,
		 selected.content_id, 10, 'shared-etag', 'application/octet-stream',
		 ?::timestamptz + revision * interval '1 second', ?
		 FROM objects AS object_row
		 JOIN unnest(?::bigint[]) WITH ORDINALITY AS selected(content_id, position)
		   ON selected.position = 1 + (split_part(object_row.key, '-', 2)::int - 1) % ?
		 CROSS JOIN generate_series(1, ?) AS history(revision)
		 WHERE object_row.bucket_id = ?`,
			[]any{base, base, pgdialect.Array(contentIDs), contentCount, revisions, f.bucket.ID},
		},
		{
			`UPDATE objects SET current_version_id = key || '-v-' || ? WHERE bucket_id = ?`,
			[]any{revisions, f.bucket.ID},
		},
		{`ANALYZE objects, object_versions, storage_contents, storage_copies, storage_data_sets, object_cache`, nil},
	} {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	capture := new(postgresQueryCapture)
	testutil.AddQueryHook(db, capture)
	snapshot, err := f.repos.Objects.OverviewSnapshot(ctx)
	if err != nil || len(snapshot.States) != 1 || snapshot.States[0].State != string(model.ObjectStateUploading) ||
		snapshot.States[0].Count != objectCount || snapshot.States[0].TotalSize != objectCount*10 {
		t.Fatalf("shared content overview = %#v, %v", snapshot, err)
	}
	plan := explainPostgresPlan(t, db, capture.last(t))
	copyScans := 0
	var inspect func(postgresExecutionPlan, float64)
	inspect = func(node postgresExecutionPlan, factLoops float64) {
		if strings.Contains(node.NodeType, "Aggregate") {
			factLoops = node.ActualLoops
		}
		if node.RelationName == "storage_copies" {
			copyScans++
			if factLoops < 1 || factLoops > contentCount {
				t.Errorf("copy facts repeated %.0f times for %d contents shared by %d objects", factLoops, contentCount, objectCount)
			}
		}
		for _, child := range node.Children {
			inspect(child, factLoops)
		}
	}
	inspect(plan, 0)
	if copyScans == 0 {
		t.Fatal("captured overview plan did not read copy facts")
	}
}
