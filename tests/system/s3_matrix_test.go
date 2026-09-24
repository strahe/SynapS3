//go:build systemtest && s3compat

package system_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

// Each subtest names an operation promised by the public S3 compatibility matrix.
// The SDK sends signed HTTP requests to the real gateway over the harness socket.
func TestS3CompatibilityMatrix(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	harness, err := systemtest.NewHarness(t.Context(), logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client := e2e.NewUnixSocketS3Client(harness.S3SocketPath(), systemtest.OwnerAccess, systemtest.OwnerSecret)
	ctx := t.Context()
	bucket := aws.String("s3-matrix")
	key := aws.String("item.bin")
	body := bytes.Repeat([]byte("matrix object\n"), 20)
	var firstVersion string
	var firstCopyMarker string
	var batchCopyMarker string

	t.Run("CreateBucket", func(t *testing.T) {
		if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("HeadBucket", func(t *testing.T) {
		if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ListBuckets", func(t *testing.T) {
		out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil || !slices.ContainsFunc(out.Buckets, func(b types.Bucket) bool { return aws.ToString(b.Name) == *bucket }) {
			t.Fatalf("bucket absent: %#v, %v", out, err)
		}
	})
	t.Run("GetBucketVersioning", func(t *testing.T) {
		out, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: bucket})
		if err != nil || out.Status != types.BucketVersioningStatusEnabled {
			t.Fatalf("versioning = %#v, %v", out, err)
		}
	})
	t.Run("PutBucketVersioning", func(t *testing.T) {
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: bucket, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
		})
		if err != nil {
			t.Fatalf("Enabled: %v", err)
		}
		_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: bucket, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
		})
		requireS3ErrorCode(t, err, "InvalidBucketState")
	})
	t.Run("GetBucketAcl", func(t *testing.T) {
		out, err := client.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: bucket})
		if err != nil || out.Owner == nil {
			t.Fatalf("ACL = %#v, %v", out, err)
		}
	})
	t.Run("PutBucketAcl", func(t *testing.T) {
		e2e.Eventually(t, ctx, 10*time.Second, "bucket provisioning for ACL write", func(ctx context.Context) (*s3.PutBucketAclOutput, bool, error) {
			out, err := client.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: bucket, ACL: types.BucketCannedACLPublicRead})
			return out, err == nil, err
		})
		out, err := client.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: bucket})
		if err != nil || !slices.ContainsFunc(out.Grants, func(grant types.Grant) bool {
			return grant.Grantee != nil && grant.Grantee.Type == types.TypeGroup && grant.Permission == types.PermissionRead
		}) {
			t.Fatalf("public-read ACL was not persisted: grants=%+v, %v", out.Grants, err)
		}
	})
	t.Run("GetBucketOwnershipControls", func(t *testing.T) {
		out, err := client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: bucket})
		if err != nil || out.OwnershipControls == nil || len(out.OwnershipControls.Rules) != 1 ||
			out.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerPreferred {
			t.Fatalf("ownership = %#v, %v", out, err)
		}
	})
	t.Run("PutBucketOwnershipControls", func(t *testing.T) {
		_, err := client.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket:            bucket,
			OwnershipControls: &types.OwnershipControls{Rules: []types.OwnershipControlsRule{{ObjectOwnership: types.ObjectOwnershipBucketOwnerPreferred}}},
		})
		if err != nil {
			t.Fatalf("BucketOwnerPreferred: %v", err)
		}
		_, err = client.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket:            bucket,
			OwnershipControls: &types.OwnershipControls{Rules: []types.OwnershipControlsRule{{ObjectOwnership: types.ObjectOwnershipBucketOwnerEnforced}}},
		})
		requireS3ErrorCode(t, err, "InvalidArgument")
	})
	t.Run("DeleteBucketOwnershipControls", func(t *testing.T) {
		if _, err := client.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		out, err := client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: bucket})
		if err != nil || out.OwnershipControls == nil || len(out.OwnershipControls.Rules) != 1 ||
			out.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerPreferred {
			t.Fatalf("ACL-compatible ownership lost: %#v, %v", out, err)
		}
	})
	t.Run("PutObject", func(t *testing.T) {
		out := e2e.Eventually(t, ctx, 10*time.Second, "bucket provisioning", func(ctx context.Context) (*s3.PutObjectOutput, bool, error) {
			got, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: bytes.NewReader(body), Metadata: map[string]string{"custom": "first"}})
			return got, err == nil, err
		})
		firstVersion = aws.ToString(out.VersionId)
		if firstVersion == "" {
			t.Fatal("missing version ID")
		}
		second := e2e.Eventually(t, ctx, 10*time.Second, "second object version", func(ctx context.Context) (*s3.PutObjectOutput, bool, error) {
			got, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: bytes.NewReader(body), Metadata: map[string]string{"custom": "second"}})
			return got, err == nil, err
		})
		if aws.ToString(second.VersionId) == firstVersion {
			t.Fatal("overwrite reused the first version ID")
		}
	})
	t.Run("GetObject", func(t *testing.T) {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key, VersionId: &firstVersion, Range: aws.String("bytes=2-5")})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = out.Body.Close() }()
		got, err := io.ReadAll(out.Body)
		if err != nil || !bytes.Equal(got, body[2:6]) || out.ContentRange == nil || out.LastModified == nil || out.Metadata["custom"] != "first" {
			t.Fatalf("range GET = %q, %#v, %v", got, out, err)
		}
		_, err = client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key, Range: aws.String("bytes=999999-")})
		requireS3ErrorCode(t, err, "InvalidRange")
	})
	t.Run("HeadObject", func(t *testing.T) {
		out, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key, VersionId: &firstVersion})
		if err != nil || out.LastModified == nil || out.Metadata["custom"] != "first" {
			t.Fatalf("HEAD = %#v, %v", out, err)
		}
	})
	t.Run("CopyObject", func(t *testing.T) {
		out, err := client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: bucket, Key: aws.String("z-copy.bin"), CopySource: aws.String(*bucket + "/" + *key)})
		if err != nil || out.CopyObjectResult == nil {
			t.Fatalf("copy = %#v, %v", out, err)
		}
	})
	for _, operation := range []string{"ListObjects", "ListObjectsV2"} {
		t.Run(operation, func(t *testing.T) {
			switch operation {
			case "ListObjects":
				out, err := client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: bucket, MaxKeys: aws.Int32(1)})
				if err != nil || len(out.Contents) != 1 || !aws.ToBool(out.IsTruncated) {
					t.Fatalf("ListObjects = %#v, %v", out, err)
				}
				page, err := client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: bucket, Marker: out.Contents[0].Key})
				if err != nil || len(page.Contents) != 1 || aws.ToString(page.Contents[0].Key) != "z-copy.bin" {
					t.Fatalf("ListObjects marker page = %#v, %v", page, err)
				}
			case "ListObjectsV2":
				out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket, MaxKeys: aws.Int32(1)})
				if err != nil || len(out.Contents) != 1 || out.NextContinuationToken == nil {
					t.Fatalf("ListObjectsV2 = %#v, %v", out, err)
				}
				page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket, ContinuationToken: out.NextContinuationToken})
				if err != nil || len(page.Contents) != 1 || aws.ToString(page.Contents[0].Key) != "z-copy.bin" {
					t.Fatalf("ListObjectsV2 continuation page = %#v, %v", page, err)
				}
			}
		})
	}
	t.Run("DeleteObject", func(t *testing.T) {
		out, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: aws.String("z-copy.bin")})
		if err != nil || !aws.ToBool(out.DeleteMarker) {
			t.Fatalf("delete = %#v, %v", out, err)
		}
		firstCopyMarker = aws.ToString(out.VersionId)
		if firstCopyMarker == "" {
			t.Fatal("delete marker has no version ID")
		}
	})
	t.Run("ListObjectVersions", func(t *testing.T) {
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: bucket})
		if err != nil || len(out.Versions) < 3 || len(out.DeleteMarkers) == 0 || !slices.ContainsFunc(out.Versions, func(v types.ObjectVersion) bool {
			return aws.ToString(v.VersionId) == firstVersion
		}) {
			t.Fatalf("versions and delete markers = %#v, %v", out, err)
		}
	})
	t.Run("DeleteObjects", func(t *testing.T) {
		out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: key}, {Key: aws.String("z-copy.bin")}}}})
		if err != nil || len(out.Deleted) != 2 || len(out.Errors) != 0 {
			t.Fatalf("batch delete = %#v, %v", out, err)
		}
		for _, deleted := range out.Deleted {
			if aws.ToString(deleted.Key) == "z-copy.bin" {
				batchCopyMarker = aws.ToString(deleted.DeleteMarkerVersionId)
			}
		}
		if batchCopyMarker == "" {
			t.Fatal("batch delete did not return the copy delete marker version")
		}
		partial, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("z-copy.bin"), VersionId: &firstCopyMarker},
			{Key: key, VersionId: aws.String("missing-version")},
		}}})
		if err != nil || len(partial.Deleted) != 1 || len(partial.Errors) != 1 {
			t.Fatalf("batch version deletion and entry error = %#v, %v", partial, err)
		}
	})
	t.Run("DeleteObject/versionId", func(t *testing.T) {
		out, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: aws.String("z-copy.bin"), VersionId: &batchCopyMarker})
		if err != nil || !aws.ToBool(out.DeleteMarker) {
			t.Fatalf("delete marker version = %#v, %v", out, err)
		}
	})

	partKey := aws.String("multipart.bin")
	var uploadID string
	var partETags []types.CompletedPart
	t.Run("CreateMultipartUpload", func(t *testing.T) {
		out, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: partKey, Metadata: map[string]string{"source": "multipart"}})
		if err != nil {
			t.Fatal(err)
		}
		uploadID = aws.ToString(out.UploadId)
	})
	t.Run("UploadPart", func(t *testing.T) {
		for partNumber, data := range [][]byte{bytes.Repeat([]byte("a"), 5<<20), bytes.Repeat([]byte("b"), 128)} {
			out, err := client.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: partKey, UploadId: &uploadID, PartNumber: aws.Int32(int32(partNumber + 1)), Body: bytes.NewReader(data)})
			if err != nil {
				t.Fatalf("part %d: %v", partNumber+1, err)
			}
			partETags = append(partETags, types.CompletedPart{PartNumber: aws.Int32(int32(partNumber + 1)), ETag: out.ETag})
		}
	})
	t.Run("ListMultipartUploads", func(t *testing.T) {
		out, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: bucket})
		if err != nil || len(out.Uploads) == 0 {
			t.Fatalf("uploads = %#v, %v", out, err)
		}
	})
	t.Run("ListParts", func(t *testing.T) {
		out, err := client.ListParts(ctx, &s3.ListPartsInput{Bucket: bucket, Key: partKey, UploadId: &uploadID})
		if err != nil || len(out.Parts) != 2 {
			t.Fatalf("parts = %#v, %v", out, err)
		}
	})
	t.Run("CompleteMultipartUpload", func(t *testing.T) {
		_, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: bucket, Key: partKey, UploadId: &uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: partETags},
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("GetObjectAttributes", func(t *testing.T) {
		out, err := client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: bucket, Key: partKey,
			ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag, types.ObjectAttributesObjectParts, types.ObjectAttributesObjectSize},
		})
		if err != nil || out.ObjectParts == nil || len(out.ObjectParts.Parts) != 2 || out.ObjectParts.TotalPartsCount != nil ||
			aws.ToInt64(out.ObjectSize) != (5<<20)+128 || out.ETag == nil {
			t.Fatalf("attributes = %#v, %v", out, err)
		}
	})
	t.Run("UploadPartCopy", func(t *testing.T) {
		out, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("copy-part.bin")})
		if err != nil {
			t.Fatal(err)
		}
		copyUploadID := aws.ToString(out.UploadId)
		copySource := *bucket + "/" + *partKey
		copied, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: bucket, Key: aws.String("copy-part.bin"), UploadId: &copyUploadID,
			PartNumber: aws.Int32(1), CopySource: &copySource,
		})
		if err != nil || copied.CopyPartResult == nil {
			t.Fatalf("whole copy = %#v, %v", copied, err)
		}
		_, err = client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: bucket, Key: aws.String("copy-part.bin"), UploadId: &copyUploadID,
			PartNumber: aws.Int32(2), CopySource: &copySource, CopySourceRange: aws.String("bytes=0-127"),
		})
		requireS3ErrorCode(t, err, "NotImplemented")
		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: bucket, Key: aws.String("copy-part.bin"), UploadId: &copyUploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: copied.CopyPartResult.ETag}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		get, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("copy-part.bin")})
		if err != nil {
			t.Fatal(err)
		}
		copiedBody, readErr := io.ReadAll(get.Body)
		_ = get.Body.Close()
		want := append(bytes.Repeat([]byte("a"), 5<<20), bytes.Repeat([]byte("b"), 128)...)
		if readErr != nil || !bytes.Equal(copiedBody, want) {
			t.Fatalf("whole-object part copy returned %d bytes, read error %v", len(copiedBody), readErr)
		}
	})
	t.Run("AbortMultipartUpload", func(t *testing.T) {
		out, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("abort.bin")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: bucket, Key: aws.String("abort.bin"), UploadId: out.UploadId}); err != nil {
			t.Fatal(err)
		}
	})
}

func requireS3ErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != want {
		t.Fatalf("S3 error = %v; want %s", err, want)
	}
}
