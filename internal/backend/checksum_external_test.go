package backend_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func TestPutObjectBadChecksumPreservesCurrentVersionAndCapacity(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "bad-object-checksum")
	first := putValidTestObjectOutput(t, tb, bucket.Name, "object", "original object")
	baseline := tb.cache.CapacitySnapshot()
	body := validTestObjectBody("replacement object")
	wrong := sha256.Sum256([]byte("different bytes"))
	_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket.Name, Key: aws.String("object"), Body: strings.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(wrong[:])),
	})
	requireAPIErrorCode(t, err, s3err.GetAPIError(s3err.ErrBadDigest))
	current, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "object")
	if err != nil || current == nil || current.VersionID != first.VersionID {
		t.Fatalf("current version after BadDigest = %#v, %v; want %s", current, err, first.VersionID)
	}
	if got := tb.cache.CapacitySnapshot(); got != baseline {
		t.Fatalf("capacity after BadDigest = %#v, want %#v", got, baseline)
	}
}

func TestUploadPartBadChecksumLeavesNoPartOrCapacityReservation(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "bad-part-checksum")
	upload, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket.Name, Key: aws.String("object"),
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline := tb.cache.CapacitySnapshot()
	wrong := make([]byte, 4)
	binary.BigEndian.PutUint32(wrong, crc32.ChecksumIEEE([]byte("different bytes")))
	_, err = tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: &bucket.Name, Key: aws.String("object"), UploadId: &upload.UploadId,
		PartNumber: aws.Int32(1), ContentLength: aws.Int64(4), Body: strings.NewReader("part"),
		ChecksumCRC32: aws.String(base64.StdEncoding.EncodeToString(wrong)),
	})
	requireAPIErrorCode(t, err, s3err.GetAPIError(s3err.ErrBadDigest))
	parts, err := tb.repos.Multiparts.GetParts(ctx, upload.UploadId, 0, 10)
	if err != nil || len(parts) != 0 {
		t.Fatalf("parts after BadDigest = %#v, %v; want none", parts, err)
	}
	if got := tb.cache.CapacitySnapshot(); got != baseline {
		t.Fatalf("capacity after BadDigest = %#v, want %#v", got, baseline)
	}
	entries, err := os.ReadDir(filepath.Join(tb.cache.(*cache.Filesystem).RootDir(), ".multipart", upload.UploadId))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("BadDigest left %d multipart files", len(entries))
	}
}
