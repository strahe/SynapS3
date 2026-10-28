package backend_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type countingListingRepository struct {
	repository.ObjectRepository
	rowsRead int
}

func (r *countingListingRepository) ListCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix, afterKey string, maxKeys int) ([]model.ObjectVersion, error) {
	rows, err := r.ObjectRepository.ListCurrentVersionsByBucket(ctx, bucketID, prefix, afterKey, maxKeys)
	r.rowsRead += len(rows)
	return rows, err
}

func (r *countingListingRepository) ListCurrentVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix, fromKey string, maxKeys int) ([]model.ObjectVersion, error) {
	rows, err := r.ObjectRepository.ListCurrentVersionsByBucketAtOrAfter(ctx, bucketID, prefix, fromKey, maxKeys)
	r.rowsRead += len(rows)
	return rows, err
}

func (r *countingListingRepository) ListVersionsByBucket(ctx context.Context, bucketID int64, prefix, keyMarker, versionMarker string, maxKeys int) ([]repository.ObjectVersionListItem, error) {
	rows, err := r.ObjectRepository.ListVersionsByBucket(ctx, bucketID, prefix, keyMarker, versionMarker, maxKeys)
	r.rowsRead += len(rows)
	return rows, err
}

func (r *countingListingRepository) ListVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix, fromKey string, maxKeys int) ([]repository.ObjectVersionListItem, error) {
	rows, err := r.ObjectRepository.ListVersionsByBucketAtOrAfter(ctx, bucketID, prefix, fromKey, maxKeys)
	r.rowsRead += len(rows)
	return rows, err
}

func TestDelimiterListingsBoundReadsAcrossCollapsedPrefix(t *testing.T) {
	const batchSize = 1000
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "listing-scan-budget")
	seed := putTestObjectOutput(t, tb, bucket.Name, "a/00000", "shared content")
	source, err := tb.repos.Objects.GetVersionByID(ctx, seed.VersionID)
	if err != nil || source == nil {
		t.Fatalf("loading seed version: version=%v err=%v", source, err)
	}
	create := func(key string) {
		t.Helper()
		_, err := tb.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
			VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: key,
			ContentID: source.ContentID, Size: source.Size, ETag: source.ETag,
			ContentType: source.ContentType,
		})
		if err != nil {
			t.Fatalf("seeding %s: %v", key, err)
		}
	}
	for i := 1; i < 2*batchSize+1; i++ {
		create(fmt.Sprintf("a/%05d", i))
	}
	create("b")
	counted := &countingListingRepository{ObjectRepository: tb.repos.Objects}
	tb.repos.Objects = counted

	type page struct {
		items                    []string
		keyMarker, versionMarker string
		truncated                bool
	}
	for _, tt := range []struct {
		name string
		list func(string, string) (page, error)
	}{
		{name: "current", list: func(marker, _ string) (page, error) {
			out, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{
				Bucket: aws.String(bucket.Name), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1), Marker: aws.String(marker),
			})
			if err != nil {
				return page{}, err
			}
			result := page{keyMarker: aws.ToString(out.NextMarker), truncated: aws.ToBool(out.IsTruncated)}
			for _, prefix := range out.CommonPrefixes {
				result.items = append(result.items, aws.ToString(prefix.Prefix))
			}
			for _, object := range out.Contents {
				result.items = append(result.items, aws.ToString(object.Key))
			}
			return result, nil
		}},
		{name: "versions", list: func(marker, versionMarker string) (page, error) {
			out, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
				Bucket: aws.String(bucket.Name), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1),
				KeyMarker: aws.String(marker), VersionIdMarker: aws.String(versionMarker),
			})
			if err != nil {
				return page{}, err
			}
			result := page{keyMarker: aws.ToString(out.NextKeyMarker), versionMarker: aws.ToString(out.NextVersionIdMarker), truncated: aws.ToBool(out.IsTruncated)}
			for _, prefix := range out.CommonPrefixes {
				result.items = append(result.items, aws.ToString(prefix.Prefix))
			}
			for _, version := range out.Versions {
				result.items = append(result.items, aws.ToString(version.Key))
			}
			return result, nil
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var marker, versionMarker string
			for index, want := range []string{"a/", "b"} {
				counted.rowsRead = 0
				got, err := tt.list(marker, versionMarker)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got.items, []string{want}) || got.truncated != (index == 0) {
					t.Fatalf("page %d: items=%v truncated=%t", index+1, got.items, got.truncated)
				}
				if counted.rowsRead > batchSize+2 {
					t.Fatalf("page %d read %d rows, budget %d", index+1, counted.rowsRead, batchSize+2)
				}
				marker, versionMarker = got.keyMarker, got.versionMarker
			}
		})
	}
}
