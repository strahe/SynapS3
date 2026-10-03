//go:build systemtest

package system_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

func TestSystemSmallObjectsRegisterTogether(t *testing.T) {
	const objects = 12

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
	bucket := "system-registration"
	if _, err := s3Client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	group, ctx := errgroup.WithContext(t.Context())
	for i := range objects {
		group.Go(func() error {
			content := bytes.Repeat([]byte(fmt.Sprintf("synaps3-registration-%02d\n", i)), 2000)
			_, err := s3Client.PutObject(ctx, &awss3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(fmt.Sprintf("objects/%02d.bin", i)), Body: bytes.NewReader(content),
			})
			return err
		})
	}
	if err := group.Wait(); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	e2e.Eventually(t, t.Context(), 60*time.Second, "every object stored on Filecoin", func(ctx context.Context) (string, bool, error) {
		var list e2e.ObjectListResponse
		raw, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket+"/objects?limit=100", &list)
		if err != nil {
			return raw, false, err
		}
		stored := 0
		for _, item := range list.Objects {
			if item.State == "stored" && item.Location.Filecoin {
				stored++
			}
		}
		return raw, stored == objects, nil
	})

	copies := 0
	for i := range objects {
		var list e2e.ObjectListResponse
		key := fmt.Sprintf("objects/%02d.bin", i)
		if _, err := admin.GetJSON(t.Context(), "/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list); err != nil || len(list.Objects) != 1 {
			t.Fatalf("list %s: %v", key, err)
		}
		var provenance e2e.ProvenanceResponse
		path := "/api/v1/buckets/" + bucket + "/objects/provenance?version_id=" + url.QueryEscape(list.Objects[0].CurrentVersionID)
		if _, err := admin.GetJSON(t.Context(), path, &provenance); err != nil {
			t.Fatalf("provenance %s: %v", key, err)
		}
		for _, copy := range provenance.Copies {
			if copy.Status != "committed" || copy.PieceID == "" {
				t.Fatalf("%s copy = %#v, want committed", key, copy)
			}
			copies++
		}
	}
	if copies != objects*config.DefaultFilecoinCopies {
		t.Fatalf("committed copies = %d, want %d", copies, objects*config.DefaultFilecoinCopies)
	}

	// Uploads and Pull replicas share the same batching policy.
	registrations := 0
	path := "/api/v1/tasks?type=storage_commit&limit=100"
	for {
		var tasks e2e.TaskListResponse
		raw, err := admin.GetJSON(t.Context(), path, &tasks)
		if err != nil {
			t.Fatalf("list registration tasks: %v\n%s", err, raw)
		}
		for _, task := range tasks.Tasks {
			if task.Status != "completed" {
				t.Fatalf("registration task %d status = %q: %s", task.ID, task.Status, raw)
			}
			registrations++
		}
		if tasks.NextCursor == nil {
			break
		}
		path = "/api/v1/tasks?type=storage_commit&limit=100&cursor=" + strconv.FormatInt(*tasks.NextCursor, 10)
	}
	if registrations < config.DefaultFilecoinCopies || registrations > copies/2 {
		t.Fatalf("registrations = %d for %d copies, want at least two copies per registration on average",
			registrations, copies)
	}
}
