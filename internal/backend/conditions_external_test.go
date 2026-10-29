package backend_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ipfs/go-cid"
	"github.com/strahe/synapse-go/storage"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
)

func TestReadConditionsRejectBeforeUnavailableProviderDownload(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "conditional-cold-read")
	key := "object"
	put := putValidTestObjectOutput(t, tb, bucket.Name, key, "conditional object")
	version, err := tb.repos.Objects.GetVersionByID(ctx, put.VersionID)
	if err != nil || version == nil {
		t.Fatalf("object version = %#v, %v", version, err)
	}
	for _, tc := range []struct {
		name        string
		ifMatch     *string
		ifNoneMatch *string
		status      int
	}{
		{"matching list", aws.String(utils.TrimQuotes(`"other", "` + version.ETag + `"`)), nil, 0},
		{"weak none match", nil, aws.String(`W/"other", W/"` + version.ETag + `"`), http.StatusNotModified},
		{"weak match", aws.String(`W/"` + version.ETag + `"`), nil, http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			get, getErr := tb.backend.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucket.Name, Key: &key, IfMatch: tc.ifMatch, IfNoneMatch: tc.ifNoneMatch,
			})
			if get != nil && get.Body != nil {
				if err := get.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			_, headErr := tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: &bucket.Name, Key: &key, IfMatch: tc.ifMatch, IfNoneMatch: tc.ifNoneMatch,
			})
			for _, err := range []error{getErr, headErr} {
				if tc.status == 0 {
					if err != nil {
						t.Fatal(err)
					}
				} else if apiErr, ok := errors.AsType[s3err.S3Error](err); !ok || apiErr.BaseError().HTTPStatusCode != tc.status {
					t.Fatalf("conditional read error = %v; want HTTP %d", err, tc.status)
				}
			}
		})
	}
	acceptBackendVersionUpload(t, tb.db, tb.repos, version.VersionID, "bafkqaaa", "https://provider.example/piece")
	if err := tb.cache.Delete(ctx, bucket.Name, version.CacheKey()); err != nil {
		t.Fatal(err)
	}
	downloads := 0
	tb.storage.DownloadFunc = func(context.Context, cid.Cid, *storage.DownloadOptions) (io.ReadCloser, error) {
		downloads++
		return nil, errors.New("provider unavailable")
	}
	_, err = tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket.Name, Key: &key, IfNoneMatch: aws.String(`"` + version.ETag + `"`),
	})
	if apiErr, ok := errors.AsType[s3err.S3Error](err); !ok || apiErr.BaseError().HTTPStatusCode != http.StatusNotModified {
		t.Fatalf("conditional GET error = %v; want 304", err)
	}
	_, err = tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket.Name, Key: &key, IfMatch: aws.String(`"wrong-etag"`),
	})
	if apiErr, ok := errors.AsType[s3err.S3Error](err); !ok || apiErr.BaseError().HTTPStatusCode != http.StatusPreconditionFailed {
		t.Fatalf("conditional HEAD error = %v; want 412", err)
	}
	if downloads != 0 {
		t.Fatalf("conditional read downloaded object data %d times", downloads)
	}
}
