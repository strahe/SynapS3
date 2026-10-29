package backend_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/s3response"
)

func waitForBackendPostgresLockWait(t *testing.T, db *bun.DB, table string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := db.NewRaw(`SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query ILIKE ?`,
			"%"+table+"%").Scan(t.Context(), &waiting); err != nil {
			t.Fatalf("reading lock waits: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PUT did not wait for a lock on %s", table)
}

func TestIdenticalPutReadsPipelineStateAfterConcurrentUploadPlanSettlement(t *testing.T) {
	tb := newTestBackend(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	bucket := seedActiveBucket(t, tb, "put-settlement-race")
	body := validTestObjectBody("identical bytes")
	first := putValidTestObjectOutput(t, tb, bucket.Name, "first", body)
	version, err := tb.repos.Objects.GetVersionByID(ctx, first.VersionID)
	if err != nil || version == nil || version.ContentID == nil {
		t.Fatalf("first version = %#v, %v", version, err)
	}
	claimed, err := tb.repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{})
	if err != nil || claimed == nil || claimed.Type != model.TaskTypeUploadPlan {
		t.Fatalf("claimed task = %#v, %v; want upload plan", claimed, err)
	}
	providerID := onChainID(t, "101")
	binding, err := tb.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: *version.ContentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tb.repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	defer release()
	settled := make(chan error, 1)
	go func() {
		settled <- tb.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			repos := repository.NewRepositories(tx)
			if _, err := repos.Contents.LockForCopyCreation(ctx, *version.ContentID); err != nil {
				return err
			}
			if err := repos.Contents.CreateUploadCopiesForBindings(ctx, *version.ContentID, []repository.UploadCopyBindingInput{{
				StorageDataSetID: binding.ID, ProviderID: providerID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress,
			}}); err != nil {
				return err
			}
			if err := repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
				Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover,
			}); err != nil {
				return err
			}
			close(ready)
			select {
			case <-proceed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-ready:
	case err := <-settled:
		t.Fatalf("preparing upload settlement: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	written := make(chan error, 1)
	go func() {
		_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
			Bucket: &bucket.Name, Key: aws.String("second"), Body: strings.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		written <- err
	}()
	waitForBackendPostgresLockWait(t, tb.db, "storage_contents")
	release()
	select {
	case err := <-settled:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("identical PUT after settlement: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	current, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "second")
	if err != nil || current == nil || current.ContentID == nil || *current.ContentID != *version.ContentID {
		t.Fatalf("second version = %#v, %v; want the same content", current, err)
	}
	count, err := tb.db.NewSelect().Model((*model.Task)(nil)).
		Where("type = ?", model.TaskTypeUploadPlan).
		Where("subject_key = ?", strconv.FormatInt(*version.ContentID, 10)).Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("upload plans for reused content = %d, %v; want one", count, err)
	}
}
