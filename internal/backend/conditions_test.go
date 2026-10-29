package backend

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	versitybackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
)

func TestReadConditionsHTTPDateAndCopyStatus(t *testing.T) {
	modified := time.Date(2026, 10, 27, 1, 2, 3, 999, time.UTC)
	sameSecond := modified.Truncate(time.Second)
	older := modified.Add(-time.Hour)
	etag := `"object-etag"`
	for _, tc := range []struct {
		name       string
		conditions versitybackend.PreConditions
		copySource bool
		status     int
		condition  string
	}{
		{"unmodified equality", versitybackend.PreConditions{IfUnmodeSince: &sameSecond}, false, 0, ""},
		{"modified equality", versitybackend.PreConditions{IfModSince: &sameSecond}, false, http.StatusNotModified, ""},
		{"etag overrides unmodified", versitybackend.PreConditions{IfMatch: &etag, IfUnmodeSince: &older}, false, 0, ""},
		{"none match overrides modified", versitybackend.PreConditions{IfNoneMatch: &etag, IfModSince: &older}, false, http.StatusNotModified, ""},
		{"unmodified before none match", versitybackend.PreConditions{IfNoneMatch: &etag, IfUnmodeSince: &older}, false, http.StatusPreconditionFailed, "If-Unmodified-Since"},
		{"copy none match", versitybackend.PreConditions{IfNoneMatch: &etag}, true, http.StatusPreconditionFailed, "If-None-Match"},
		{"copy modified equality", versitybackend.PreConditions{IfModSince: &sameSecond}, true, http.StatusPreconditionFailed, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := evaluateReadConditions("object-etag", modified, tc.conditions, tc.copySource)
			if tc.status == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var apiErr s3err.S3Error
			if !errors.As(err, &apiErr) || apiErr.BaseError().HTTPStatusCode != tc.status {
				t.Fatalf("condition error = %v; want HTTP %d", err, tc.status)
			}
			var response struct{ Condition string }
			if err := xml.Unmarshal(apiErr.XMLBody("", ""), &response); err != nil || response.Condition != tc.condition {
				t.Fatalf("condition response=%q err=%v; want %q", response.Condition, err, tc.condition)
			}
		})
	}
}

func TestReadConditionsETagLists(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		ifMatch bool
		status  int
	}{
		{"match list", `"other", "object-etag"`, true, 0},
		{"match mixed list", `W/"object-etag", "object-etag"`, true, 0},
		{"weak match", `W/"object-etag"`, true, http.StatusPreconditionFailed},
		{"none match weak last", `"other", W/"object-etag"`, false, http.StatusNotModified},
		{"none match weak first", `W/"object-etag", "other"`, false, http.StatusNotModified},
		{"optional whitespace", " \t, \"other\" ,\t\"object-etag\" , ", true, 0},
		{"quoted comma", `"other,object-etag", "last"`, true, http.StatusPreconditionFailed},
		{"match wildcard", " \t*\t ", true, 0},
		{"none match wildcard", "*", false, http.StatusNotModified},
		{"wildcard in list", `*, "object-etag"`, true, http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, value := range []string{tc.value, utils.TrimQuotes(tc.value)} {
				conditions := versitybackend.PreConditions{}
				if tc.ifMatch {
					conditions.IfMatch = &value
				} else {
					conditions.IfNoneMatch = &value
				}
				err := evaluateReadConditions(`"object-etag"`, time.Time{}, conditions, false)
				if tc.status == 0 {
					if err != nil {
						t.Fatalf("header %q: %v", value, err)
					}
				} else if apiErr, ok := errors.AsType[s3err.S3Error](err); !ok || apiErr.BaseError().HTTPStatusCode != tc.status {
					t.Fatalf("header %q: error = %v; want HTTP %d", value, err, tc.status)
				}
			}
		})
	}
}

func TestWriteConditionsTreatDeleteMarkerAsMissing(t *testing.T) {
	if writeObjectCondition(nil, nil) != nil {
		t.Fatal("unconditional write has a condition")
	}
	star := "*"
	condition := writeObjectCondition(nil, &star)
	if err := condition(nil); err != nil {
		t.Fatal(err)
	}
	if err := condition(&model.ObjectVersion{IsDeleteMarker: true}); err != nil {
		t.Fatal(err)
	}
	if err := condition(&model.ObjectVersion{ETag: "existing"}); err == nil {
		t.Fatal("existing object passed If-None-Match")
	}
}

func TestDeleteConditionsDistinguishMissingObjectsAndDeleteMarkers(t *testing.T) {
	if deleteObjectCondition(versitybackend.ObjectDeletePreconditions{}) != nil {
		t.Fatal("unconditional delete has a condition")
	}
	star, wrong := "*", "wrong-etag"
	for _, tc := range []struct {
		name    string
		version *model.ObjectVersion
		ifMatch *string
		status  int
	}{
		{"missing", nil, &star, http.StatusNotFound},
		{"delete marker", &model.ObjectVersion{IsDeleteMarker: true}, &star, http.StatusPreconditionFailed},
		{"live object", &model.ObjectVersion{ETag: "live-etag"}, &star, 0},
		{"mismatched ETag", &model.ObjectVersion{ETag: "live-etag"}, &wrong, http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := deleteObjectCondition(versitybackend.ObjectDeletePreconditions{IfMatch: tc.ifMatch})(tc.version)
			if tc.status == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var apiErr s3err.S3Error
			if !errors.As(err, &apiErr) || apiErr.BaseError().HTTPStatusCode != tc.status {
				t.Fatalf("condition error = %v; want HTTP %d", err, tc.status)
			}
		})
	}
}

type conditionalDeleteBucketRepository struct{ repository.BucketRepository }

func (conditionalDeleteBucketRepository) GetByName(context.Context, string) (*model.Bucket, error) {
	return &model.Bucket{ID: 1, Status: model.BucketStatusActive}, nil
}

type conditionalDeleteObjectRepository struct {
	repository.ObjectRepository
	version *model.ObjectVersion
}

func (r conditionalDeleteObjectRepository) CreateDeleteMarkerAndSetCurrent(_ context.Context, _ int64, _, _ string, conditions ...repository.ObjectCondition) (*model.ObjectVersion, error) {
	for _, condition := range conditions {
		if err := condition(r.version); err != nil {
			return nil, fmt.Errorf("creating marker: %w", err)
		}
	}
	return &model.ObjectVersion{VersionID: "marker", IsDeleteMarker: true}, nil
}

func TestDeleteObjectReturnsConditionalErrorDirectlyToGateway(t *testing.T) {
	star, wrong := "*", "wrong-etag"
	for _, tc := range []struct {
		name    string
		version *model.ObjectVersion
		ifMatch *string
		status  int
		code    string
	}{
		{"missing", nil, &star, http.StatusNotFound, "NoSuchKey"},
		{"delete marker", &model.ObjectVersion{IsDeleteMarker: true}, &star, http.StatusPreconditionFailed, "PreconditionFailed"},
		{"live object", &model.ObjectVersion{ETag: "live-etag"}, &star, 0, ""},
		{"mismatched ETag", &model.ObjectVersion{ETag: "live-etag"}, &wrong, http.StatusPreconditionFailed, "PreconditionFailed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &SynapseBackend{repos: &repository.Repositories{
				Buckets: conditionalDeleteBucketRepository{},
				Objects: conditionalDeleteObjectRepository{version: tc.version},
			}}
			bucket, key := "bucket", "key"
			out, err := b.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key, IfMatch: tc.ifMatch})
			if tc.status == 0 {
				if err != nil || out == nil || out.DeleteMarker == nil || !*out.DeleteMarker {
					t.Fatalf("DeleteObject = %#v, %v; want a successful delete marker", out, err)
				}
			} else {
				apiErr, ok := err.(s3err.S3Error)
				if !ok || apiErr.BaseError().HTTPStatusCode != tc.status || apiErr.BaseError().Code != tc.code {
					t.Fatalf("gateway error = %T %v; want direct S3 %d %s", err, err, tc.status, tc.code)
				}
			}
			batch, err := b.DeleteObjects(t.Context(), &s3.DeleteObjectsInput{
				Bucket: &bucket,
				Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: &key, ETag: tc.ifMatch}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if tc.status == 0 {
				if len(batch.Error) != 0 || len(batch.Deleted) != 1 {
					t.Fatalf("DeleteObjects = %#v; want one successful deletion", batch)
				}
			} else if len(batch.Error) != 1 || batch.Error[0].Code == nil || *batch.Error[0].Code != tc.code || len(batch.Deleted) != 0 {
				t.Fatalf("DeleteObjects = %#v; want one %s error", batch, tc.code)
			}
		})
	}
}
