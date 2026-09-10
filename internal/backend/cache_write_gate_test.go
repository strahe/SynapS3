package backend_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/s3response"
)

type blockingObjectVersionInsertHook struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (h *blockingObjectVersionInsertHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	query := strings.ToLower(event.Query)
	if strings.Contains(query, `insert into "object_versions"`) || strings.Contains(query, "insert into object_versions") {
		h.once.Do(func() {
			close(h.entered)
			select {
			case <-h.release:
			case <-ctx.Done():
			}
		})
	}
	return ctx
}

func (*blockingObjectVersionInsertHook) AfterQuery(context.Context, *bun.QueryEvent) {}

func seedExpectedWriteContent(t *testing.T, tb *testBackend, bucketName, body string) int64 {
	t.Helper()
	bucket, err := tb.repos.Buckets.GetByName(t.Context(), bucketName)
	if err != nil || bucket == nil {
		t.Fatalf("load bucket %q = %#v, err=%v", bucketName, bucket, err)
	}
	content, err := tb.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: int64(len(body)), Checksum: testutil.StorageChecksum(testSHA256Hex(body)),
		RequestedCopies: bucket.DefaultCopies,
	})
	if err != nil {
		t.Fatalf("seed expected content: %v", err)
	}
	return content.ID
}

func assertWriteHoldsContentGateThroughVersionInsert(
	t *testing.T,
	tb *testBackend,
	contentID int64,
	write func() error,
) {
	t.Helper()
	hook := &blockingObjectVersionInsertHook{entered: make(chan struct{}), release: make(chan struct{})}
	tb.db.AddQueryHook(hook)
	writeDone := make(chan error, 1)
	go func() { writeDone <- write() }()

	select {
	case <-hook.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not reach the object version transaction")
	}
	deletionStarted := make(chan struct{})
	deletionEntered := make(chan struct{})
	go func() {
		close(deletionStarted)
		tb.gate.GuardDeletion(model.ContentCacheKey(contentID), func() { close(deletionEntered) })
	}()
	<-deletionStarted
	select {
	case <-deletionEntered:
		close(hook.release)
		t.Fatal("deletion entered while the cache write transaction was still blocked")
	case <-time.After(50 * time.Millisecond):
	}
	close(hook.release)
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("cache write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cache write did not finish after releasing the transaction hook")
	}
	select {
	case <-deletionEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("deletion did not enter after the cache write transaction finished")
	}
}

func TestPutObjectHoldsContentGateThroughVersionTransaction(t *testing.T) {
	tb := newTestBackend(t)
	bucketName := "put-cache-gate"
	seedActiveBucket(t, tb, bucketName)
	body := validTestObjectBody("put cache gate")
	contentID := seedExpectedWriteContent(t, tb, bucketName, body)
	assertWriteHoldsContentGateThroughVersionInsert(t, tb, contentID, func() error {
		_, err := tb.backend.PutObject(t.Context(), s3response.PutObjectInput{
			Bucket: &bucketName, Key: aws.String("object.bin"), Body: strings.NewReader(body),
		})
		return err
	})
}

func TestCopyObjectHoldsContentGateThroughVersionTransaction(t *testing.T) {
	tb := newTestBackend(t)
	bucketName := "copy-cache-gate"
	bucket := seedActiveBucket(t, tb, bucketName)
	putValidTestObject(t, tb, bucketName, "source.bin", "copy cache gate")
	source, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(t.Context(), bucket.ID, "source.bin")
	if err != nil || source == nil || source.ContentID == nil {
		t.Fatalf("load copy source = %#v, err=%v", source, err)
	}
	assertWriteHoldsContentGateThroughVersionInsert(t, tb, *source.ContentID, func() error {
		copySource := bucketName + "/source.bin"
		_, err := tb.backend.CopyObject(t.Context(), s3response.CopyObjectInput{
			Bucket: &bucketName, Key: aws.String("destination.bin"), CopySource: &copySource,
		})
		return err
	})
}

func TestCompleteMultipartHoldsContentGateThroughVersionTransaction(t *testing.T) {
	tb := newTestBackend(t)
	bucketName := "multipart-cache-gate"
	seedActiveBucket(t, tb, bucketName)
	key := "assembled.bin"
	created, err := tb.backend.CreateMultipartUpload(t.Context(), s3response.CreateMultipartUploadInput{
		Bucket: &bucketName, Key: &key,
	})
	if err != nil {
		t.Fatalf("create multipart upload: %v", err)
	}
	body := validTestObjectBody("multipart cache gate")
	partNumber := int32(1)
	part, err := tb.backend.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket: &bucketName, Key: &key, UploadId: &created.UploadId,
		PartNumber: &partNumber, Body: strings.NewReader(body),
	})
	if err != nil {
		t.Fatalf("upload multipart part: %v", err)
	}
	contentID := seedExpectedWriteContent(t, tb, bucketName, body)
	assertWriteHoldsContentGateThroughVersionInsert(t, tb, contentID, func() error {
		_, _, err := tb.backend.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{
			Bucket: &bucketName, Key: &key, UploadId: &created.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{
				PartNumber: &partNumber, ETag: part.ETag,
			}}},
		})
		return err
	})
}
