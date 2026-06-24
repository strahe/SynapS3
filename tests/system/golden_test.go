//go:build systemtest

package system_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

func TestSystemGoldenPath(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	harness, err := systemtest.NewHarness(t.Context(), logger)
	if err != nil {
		t.Fatalf("NewHarness: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if closed {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.Close(closeCtx); err != nil {
			t.Errorf("Close harness: %v", err)
		}
	})

	admin := e2e.NewAdminClient(t, harness.AdminURL)
	admin.Login(t, t.Context(), systemtest.AdminUsername, systemtest.AdminPassword)
	credentials := admin.CreateS3User(t, t.Context())

	s3Client := e2e.NewUnixSocketS3Client(harness.S3SocketPath(), credentials.AccessKey, credentials.SecretKey)
	bucket, key := "system-golden", "objects/golden.bin"
	if _, err := s3Client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	content := bytes.Repeat([]byte("synaps3-system-golden\n"), 6000)
	checksum := sha256.Sum256(content)
	if _, err := s3Client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(content), ContentType: aws.String("application/octet-stream"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, key, content, checksum)

	object := e2e.Eventually(t, t.Context(), 10*time.Second, "object to complete three-copy upload", func(ctx context.Context) (struct {
		VersionID string
		Snapshot  string
	}, bool, error,
	) {
		var list e2e.ObjectListResponse
		raw, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list)
		if err != nil {
			return struct {
				VersionID string
				Snapshot  string
			}{Snapshot: raw}, false, err
		}
		if len(list.Objects) != 1 {
			return struct {
				VersionID string
				Snapshot  string
			}{Snapshot: raw}, false, nil
		}
		item := list.Objects[0]
		return struct {
			VersionID string
			Snapshot  string
		}{VersionID: item.CurrentVersionID, Snapshot: raw}, item.UploadStatus == "complete" && item.Location.Filecoin, nil
	})

	provenancePath := "/api/v1/buckets/" + bucket + "/objects/provenance?version_id=" + url.QueryEscape(object.VersionID)
	provenance := e2e.Eventually(t, t.Context(), 5*time.Second, "three readable committed copies", func(ctx context.Context) (e2e.ProvenanceResponse, bool, error) {
		var value e2e.ProvenanceResponse
		_, err := admin.GetJSON(ctx, provenancePath, &value)
		if err != nil {
			return value, false, err
		}
		if value.SuccessCopies != 3 || value.RequestedCopies != 3 || len(value.Copies) != 3 {
			return value, false, nil
		}
		for _, copy := range value.Copies {
			if copy.Status != "committed" || copy.ProviderID == "" || copy.DataSetID == "" || copy.PieceID == "" || copy.RetrievalURL == "" {
				return value, false, nil
			}
		}
		return value, true, nil
	})
	if provenance.UploadStatus != "complete" {
		t.Fatalf("provenance upload status = %q, want complete", provenance.UploadStatus)
	}

	e2e.Eventually(t, t.Context(), 5*time.Second, "completed upload tasks", func(ctx context.Context) (string, bool, error) {
		var tasks e2e.TaskListResponse
		raw, err := admin.GetJSON(ctx, "/api/v1/tasks?type=upload&limit=100", &tasks)
		if err != nil {
			return raw, false, err
		}
		completed, failed := 0, 0
		for _, task := range tasks.Tasks {
			if task.RefVersionID != object.VersionID {
				continue
			}
			switch task.Status {
			case "completed":
				completed++
			case "failed":
				failed++
			}
		}
		return raw, completed > 0 && failed == 0, nil
	})

	e2e.Eventually(t, t.Context(), 5*time.Second, "automatic cache eviction", func(ctx context.Context) (string, bool, error) {
		var list e2e.ObjectListResponse
		raw, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list)
		if err != nil {
			return raw, false, err
		}
		ready := len(list.Objects) == 1 && !list.Objects[0].Location.Cache && list.Objects[0].Location.Filecoin
		return raw, ready, nil
	})
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, key, content, checksum)

	e2e.Eventually(t, t.Context(), 5*time.Second, "provider and dataset observability snapshots", func(ctx context.Context) (string, bool, error) {
		var providers e2e.ProviderObservationPage
		providerRaw, err := admin.GetJSON(ctx, "/api/v1/observability/providers?limit=20", &providers)
		if err != nil {
			return providerRaw, false, err
		}
		var dataSets e2e.DataSetObservationPage
		dataSetRaw, err := admin.GetJSON(ctx, "/api/v1/observability/data-sets?limit=20", &dataSets)
		if err != nil {
			return providerRaw + "\n" + dataSetRaw, false, err
		}
		return providerRaw + "\n" + dataSetRaw, providers.Summary.Available == 3 && dataSets.Summary.Available == 3, nil
	})

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := harness.Close(closeCtx); err != nil {
		t.Fatalf("Close harness: %v", err)
	}
	closed = true
}
