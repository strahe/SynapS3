package objectreader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
)

type metadataCheckBucketRepository struct{ repository.BucketRepository }

func (metadataCheckBucketRepository) GetByName(context.Context, string) (*model.Bucket, error) {
	return &model.Bucket{ID: 1, Status: model.BucketStatusActive}, nil
}

type metadataCheckObjectRepository struct {
	repository.ObjectRepository
	version     *model.ObjectVersion
	replacement *model.ObjectVersion
}

func (r *metadataCheckObjectRepository) GetCurrentVersionByBucketAndKey(context.Context, int64, string) (*model.ObjectVersion, error) {
	return r.version, nil
}

func (r *metadataCheckObjectRepository) GetVersionByBucketKeyAndID(context.Context, int64, string, string) (*model.ObjectVersion, error) {
	return r.version, nil
}

func (r *metadataCheckObjectRepository) GetCurrentVersionByObjectID(context.Context, int64) (*model.ObjectVersion, error) {
	if r.replacement != nil {
		r.version = r.replacement
	}
	return r.version, nil
}

func TestMetadataCheckRejectsBeforeOpeningObjectData(t *testing.T) {
	for _, explicitVersion := range []bool{false, true} {
		name := "current version"
		if explicitVersion {
			name = "explicit version"
		}
		t.Run(name, func(t *testing.T) {
			version := &model.ObjectVersion{VersionID: "version", ETag: "etag"}
			repos := &repository.Repositories{
				Buckets: metadataCheckBucketRepository{},
				Objects: &metadataCheckObjectRepository{version: version},
			}
			localCache := &testutil.MockCache{
				GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
					t.Fatal("rejected metadata check opened cached data")
					return nil, nil, os.ErrNotExist
				},
			}
			storageClient := &testutil.MockStorageClient{
				DownloadFunc: func(context.Context, cid.Cid, *storage.DownloadOptions) (io.ReadCloser, error) {
					t.Fatal("rejected metadata check downloaded object data")
					return nil, errors.New("provider unavailable")
				},
			}
			reader := newTestReader(repos, localCache, storageClient, slog.Default())
			rejected := errors.New("metadata condition failed")
			check := func(got *model.ObjectVersion) error {
				if got != version {
					t.Fatalf("checked version = %#v, want resolved version", got)
				}
				return rejected
			}
			var result *Result
			var err error
			if explicitVersion {
				result, err = reader.OpenVersion(t.Context(), "bucket", "key", version.VersionID, S3Visibility, check)
			} else {
				result, err = reader.Open(t.Context(), "bucket", "key", S3Visibility, check)
			}
			if result != nil || err != rejected {
				t.Fatalf("Open = %#v, %v; want nil and the original condition error", result, err)
			}
		})
	}
}

type metadataCheckContentRepository struct {
	repository.StorageContentRepository
	pieceCID string
}

func (r metadataCheckContentRepository) ListReadableCommittedCopies(context.Context, int64) ([]repository.ReadableStorageCopy, error) {
	return []repository.ReadableStorageCopy{{PieceCID: r.pieceCID, RetrievalURL: "https://provider.example/piece"}}, nil
}

type metadataCheckReadCloser struct {
	io.Reader
	closed bool
}

func (r *metadataCheckReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestMetadataCheckRunsAgainAfterCurrentVersionChanges(t *testing.T) {
	contentID := int64(1)
	old := &model.ObjectVersion{VersionID: "old", ObjectID: 1, ContentID: &contentID, ETag: "old-etag"}
	replacement := &model.ObjectVersion{VersionID: "replacement", ObjectID: 1, ContentID: &contentID, ETag: "new-etag"}
	repos := &repository.Repositories{
		Buckets:  metadataCheckBucketRepository{},
		Objects:  &metadataCheckObjectRepository{version: old, replacement: replacement},
		Contents: metadataCheckContentRepository{pieceCID: buildTestCID(t)},
	}
	cacheOpens := 0
	localCache := &testutil.MockCache{
		GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
			cacheOpens++
			return nil, nil, os.ErrNotExist
		},
	}
	body := &metadataCheckReadCloser{Reader: bytes.NewReader([]byte("object"))}
	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(context.Context, cid.Cid, *storage.DownloadOptions) (io.ReadCloser, error) {
			return body, nil
		},
	}
	reader := newTestReader(repos, localCache, storageClient, slog.Default())
	rejected := errors.New("ETag changed")
	checked := make([]string, 0, 2)
	result, err := reader.Open(t.Context(), "bucket", "key", S3Visibility, func(version *model.ObjectVersion) error {
		checked = append(checked, version.VersionID)
		if version.ETag != old.ETag {
			return rejected
		}
		return nil
	})
	if result != nil || err != rejected {
		t.Fatalf("Open = %#v, %v; want rejection of the replacement version", result, err)
	}
	if len(checked) != 2 || checked[0] != old.VersionID || checked[1] != replacement.VersionID {
		t.Fatalf("checked versions = %v; want old then replacement", checked)
	}
	if !body.closed || cacheOpens != 1 {
		t.Fatalf("old body closed=%v, cache opens=%d; replacement data must remain unopened", body.closed, cacheOpens)
	}
}
