//go:build systemtest

package system_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

type replacementDataSet struct {
	ID          int64  `json:"id"`
	CopyIndex   int    `json:"copy_index"`
	Generation  int    `json:"generation"`
	IsCurrent   bool   `json:"is_current"`
	Replaceable bool   `json:"replaceable"`
	ProviderID  string `json:"provider_id"`
	Status      string `json:"status"`
}

type replacementSummary struct {
	ID          int64  `json:"id"`
	Status      string `json:"status"`
	WaitReason  string `json:"wait_reason"`
	ItemsTotal  int    `json:"items_total"`
	ItemsCopied int    `json:"items_copied"`
	Source      struct {
		ProviderID string `json:"provider_id"`
		Status     string `json:"status"`
	} `json:"source"`
	Target struct {
		ProviderID string `json:"provider_id"`
		IsCurrent  bool   `json:"is_current"`
	} `json:"target"`
}

type bucketReplacementView struct {
	DataSets     []replacementDataSet `json:"data_sets"`
	Replacements []replacementSummary `json:"replacements"`
}

// A whole approved replacement, driven only through the public Admin and S3
// surfaces: confirm, migrate, retire, with reads working throughout.
func TestSystemProviderReplacement(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	harness, err := systemtest.NewHarness(t.Context(), logger)
	if err != nil {
		t.Fatalf("NewHarness: %v", err)
	}
	t.Cleanup(func() {
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

	bucket, key := "system-replacement", "objects/replaceable.bin"
	if _, err := s3Client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	content := bytes.Repeat([]byte("synaps3-provider-replacement\n"), 2000)
	checksum := sha256.Sum256(content)
	if _, err := s3Client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(content),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, key, content, checksum)

	// Wait until the object is durably stored on its replicas.
	view := e2e.Eventually(t, t.Context(), 30*time.Second, "bucket replicas to become ready",
		func(ctx context.Context) (bucketReplacementView, bool, error) {
			var got bucketReplacementView
			if _, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket, &got); err != nil {
				return got, false, err
			}
			ready := 0
			for _, set := range got.DataSets {
				if set.IsCurrent && set.Status == "ready" {
					ready++
				}
			}
			return got, ready > 0, nil
		})

	var source replacementDataSet
	for _, set := range view.DataSets {
		if set.Replaceable {
			source = set
			break
		}
	}
	if source.ID == 0 {
		t.Fatalf("no replaceable data set in %+v", view.DataSets)
	}
	if source.Generation != 1 {
		t.Fatalf("initial generation = %d, want 1", source.Generation)
	}

	// One confirmation authorizes the new service, the switch, the migration,
	// and retirement of the old service.
	var confirmed replacementSummary
	admin.PostJSON(t, t.Context(),
		fmt.Sprintf("/api/v1/buckets/%s/data-sets/%d/replacement", bucket, source.ID),
		map[string]string{
			"mode":              "automatic",
			"client_request_id": "system-provider-replacement",
		}, &confirmed)
	if confirmed.Status != "preparing_target" {
		t.Fatalf("confirmed status = %s, want preparing_target", confirmed.Status)
	}
	if confirmed.Target.ProviderID == source.ProviderID {
		t.Fatalf("automatic selection reused the retiring provider %s", source.ProviderID)
	}

	// Reads must keep working while the replacement runs.
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, key, content, checksum)

	// A coordinator that hits any dependency wait parks for uploadDependencyWaitDelay,
	// which is a minute. The budget has to clear one of those plus the work either
	// side of it, or a single transient wait fails the test rather than delaying it.
	final := e2e.Eventually(t, t.Context(), 150*time.Second, "provider replacement to finish",
		func(ctx context.Context) (bucketReplacementView, bool, error) {
			var got bucketReplacementView
			if _, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket, &got); err != nil {
				return got, false, err
			}
			completed := false
			for _, row := range got.Replacements {
				if row.ID == confirmed.ID {
					completed = row.Status == "completed"
					break
				}
			}
			if !completed {
				return got, false, nil
			}
			sourceRetired, targetCurrent := false, false
			for _, set := range got.DataSets {
				sourceRetired = sourceRetired || set.ID == source.ID && set.Status == "retired"
				targetCurrent = targetCurrent || set.CopyIndex == source.CopyIndex && set.IsCurrent && set.ProviderID == confirmed.Target.ProviderID
			}
			return got, sourceRetired && targetCurrent, nil
		})

	var completed replacementSummary
	for _, row := range final.Replacements {
		if row.ID == confirmed.ID {
			completed = row
		}
	}
	if completed.ItemsTotal == 0 || completed.ItemsCopied != completed.ItemsTotal {
		t.Fatalf("migration progress = %d/%d, want everything copied", completed.ItemsCopied, completed.ItemsTotal)
	}
	if completed.Source.Status != "retired" {
		t.Fatalf("source status = %s, want retired", completed.Source.Status)
	}
	if !completed.Target.IsCurrent {
		t.Fatal("target did not take over the replica slot")
	}

	// The slot now has two generations: the retired original and its successor.
	var current, retired *replacementDataSet
	for i := range final.DataSets {
		set := &final.DataSets[i]
		if set.CopyIndex != source.CopyIndex {
			continue
		}
		if set.IsCurrent {
			current = set
		} else if set.Status == "retired" {
			retired = set
		}
	}
	if current == nil || retired == nil {
		t.Fatalf("slot generations = %+v, want one current and one retired", final.DataSets)
	}
	if current.Generation <= retired.Generation {
		t.Fatalf("generations = current:%d retired:%d, want the successor to be newer", current.Generation, retired.Generation)
	}
	if retired.Replaceable {
		t.Fatal("a retired generation is still offered for replacement")
	}

	// The object is still readable from the new provider, and new writes land
	// there too.
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, key, content, checksum)
	newKey := "objects/after-replacement.bin"
	newContent := bytes.Repeat([]byte("after-replacement\n"), 500)
	newChecksum := sha256.Sum256(newContent)
	if _, err := s3Client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(newKey), Body: bytes.NewReader(newContent),
	}); err != nil {
		t.Fatalf("PutObject after replacement: %v", err)
	}
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, newKey, newContent, newChecksum)
}
