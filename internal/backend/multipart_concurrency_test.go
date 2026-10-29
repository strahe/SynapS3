package backend_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

type pausedPartPublicationCache struct {
	cache.Cache
	staged  chan struct{}
	proceed chan struct{}
}

func (c *pausedPartPublicationCache) PutPartStaged(ctx context.Context, uploadID string, partNumber int, body io.Reader, size int64) (*cache.StagedObject, error) {
	staged, err := c.Cache.PutPartStaged(ctx, uploadID, partNumber, body, size)
	if err != nil {
		return nil, err
	}
	close(c.staged)
	select {
	case <-c.proceed:
		return staged, nil
	case <-ctx.Done():
		_ = staged.Rollback()
		return nil, ctx.Err()
	}
}

func TestAbortPreventsPublicationOfAnAlreadyStagedPart(t *testing.T) {
	filesystem := newTestCache(t, 1<<30)
	localCache := &pausedPartPublicationCache{Cache: filesystem, staged: make(chan struct{}), proceed: make(chan struct{})}
	tb := newTestBackendWithCache(t, localCache)
	bucket := seedActiveBucket(t, tb, "abort-staged-part")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	upload, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket.Name, Key: aws.String("object"),
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline := filesystem.CapacitySnapshot()
	var once sync.Once
	resume := func() { once.Do(func() { close(localCache.proceed) }) }
	defer resume()
	finished := make(chan error, 1)
	go func() {
		_, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: &bucket.Name, Key: aws.String("object"), UploadId: &upload.UploadId,
			PartNumber: aws.Int32(1), ContentLength: aws.Int64(4), Body: strings.NewReader("part"),
		})
		finished <- err
	}()
	select {
	case <-localCache.staged:
	case <-ctx.Done():
		t.Fatal("part did not reach its staged publication boundary")
	}
	if err := tb.backend.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: &bucket.Name, Key: aws.String("object"), UploadId: &upload.UploadId,
	}); err != nil {
		t.Fatal(err)
	}
	resume()
	select {
	case err := <-finished:
		requireAPIErrorCode(t, err, s3err.GetAPIError(s3err.ErrNoSuchUpload))
	case <-ctx.Done():
		t.Fatal("staged part did not finish after abort")
	}
	parts, err := tb.repos.Multiparts.GetParts(ctx, upload.UploadId, 0, 10)
	if err != nil || len(parts) != 0 {
		t.Fatalf("parts after aborted publication = %#v, %v; want none", parts, err)
	}
	if got := filesystem.CapacitySnapshot(); got != baseline {
		t.Fatalf("capacity after aborted publication = %#v, want %#v", got, baseline)
	}
}

func TestCompleteRejectsPartBytesThatDifferFromTheRecordedETag(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "changed-part-bytes")
	upload, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket.Name, Key: aws.String("object"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := validTestObjectBody("original part")
	part, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: &bucket.Name, Key: aws.String("object"), UploadId: &upload.UploadId,
		PartNumber: aws.Int32(1), ContentLength: aws.Int64(int64(len(body))), Body: strings.NewReader(body),
	})
	if err != nil {
		t.Fatal(err)
	}
	filesystem := tb.cache.(*cache.Filesystem)
	baseline := filesystem.CapacitySnapshot()
	partPath := filepath.Join(filesystem.RootDir(), ".multipart", upload.UploadId, "1")
	if err := os.WriteFile(partPath, []byte(strings.Repeat("x", len(body))), 0o600); err != nil {
		t.Fatal(err)
	}
	_, versionID, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: &bucket.Name, Key: aws.String("object"), UploadId: &upload.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}},
	})
	requireAPIErrorCode(t, err, s3err.GetAPIError(s3err.ErrInvalidPart))
	if versionID != "" {
		t.Fatalf("rejected completion returned version %q", versionID)
	}
	version, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "object")
	if err != nil || version != nil {
		t.Fatalf("version after rejected completion = %#v, %v; want none", version, err)
	}
	current, err := tb.repos.Multiparts.GetByUploadID(ctx, upload.UploadId)
	if err != nil || current == nil || current.Status != model.MultipartStatusInitiated {
		t.Fatalf("upload after rejected completion = %#v, %v; want initiated", current, err)
	}
	if got := filesystem.CapacitySnapshot(); got != baseline {
		t.Fatalf("capacity after rejected completion = %#v, want %#v", got, baseline)
	}
}
