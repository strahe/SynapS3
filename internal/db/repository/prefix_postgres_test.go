package repository_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	appdb "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/uptrace/bun"
)

func TestPostgresPrefixPlan(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}

	ctx := context.Background()
	db := newPostgresPrefixTestDB(t, ctx, dsn)
	capture := new(postgresQueryCapture)
	repos := repository.NewRepositories(db.WithQueryHook(capture))
	bucket := seedBucket(t, db, "pg-prefix-plan-bucket")

	seedPostgresPrefixObjects(t, ctx, repos, bucket.ID)
	seedPostgresPrefixMultiparts(t, ctx, repos, bucket.ID)
	seedPostgresPrefixPlanRows(t, ctx, db, bucket.ID)
	riskVersionID, staleBefore := seedPostgresStorageRisk(t, ctx, repos, bucket)
	if _, err := db.ExecContext(ctx, "ANALYZE object_versions, multipart_uploads"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}

	capture.reset()
	current, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "prefix/00010000", "", 10)
	if err != nil {
		t.Fatalf("ListCurrentVersionsByBucket: %v", err)
	}
	requireObjectVersionKeys(t, current, []string{"prefix/00010000.txt"})
	assertPostgresPlanUsesIndex(t, db, "idx_object_versions_current_bucket_delete_key_c", capture.last(t))

	capture.reset()
	versions, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "under", "underX/literal.txt", "01J000000000000000PG000003", 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket: %v", err)
	}
	if len(versions) == 0 || versions[0].Key != "under_/literal.txt" {
		t.Fatalf("version marker page = %#v, want under_/literal.txt first", versions)
	}
	assertPostgresPlanUsesIndex(t, db, "idx_object_versions_bucket_key_created_c", capture.last(t))

	capture.reset()
	uploads, err := repos.Multiparts.ListByBucket(ctx, bucket.ID, `back\slash/`, `back\slash/literal.txt`, "pg-prefix-upload-000002", 10)
	if err != nil {
		t.Fatalf("ListByBucket multipart: %v", err)
	}
	if len(uploads) != 1 || uploads[0].UploadID != "pg-prefix-upload-000002-next" {
		t.Fatalf("multipart page = %#v, want next upload for the marker key", uploads)
	}
	assertPostgresPlanUsesIndex(t, db, "idx_multipart_uploads_bucket_status_key_upload_c", capture.last(t))

	capture.reset()
	riskPage, err := repos.Uploads.ListBucketStorageHealthAffectedVersions(ctx, repository.BucketStorageHealthAffectedVersionsInput{
		BucketID:    bucket.ID,
		Prefix:      "prefix/00010000",
		StaleBefore: staleBefore,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListBucketStorageHealthAffectedVersions: %v", err)
	}
	if len(riskPage.Versions) != 1 || riskPage.Versions[0].Version.VersionID != riskVersionID {
		t.Fatalf("storage risk versions = %#v, want %s", riskPage.Versions, riskVersionID)
	}
	assertPostgresPlanUsesIndex(t, db, "idx_object_versions_bucket_key_created_c", capture.match(t, `ORDER BY object_version.key COLLATE "C" ASC`))
}

func newPostgresPrefixTestDB(t *testing.T, ctx context.Context, dsn string) *bun.DB {
	t.Helper()
	db, err := appdb.New(config.DatabaseConfig{
		Driver:       "postgres",
		DSN:          dsn,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("opening postgres test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	schema := fmt.Sprintf("synaps3_prefix_verify_%d", time.Now().UnixNano())
	quotedSchema := quotePostgresIdentifier(schema)
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("creating schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE")
	})
	if _, err := db.ExecContext(ctx, "SET search_path TO "+quotedSchema); err != nil {
		t.Fatalf("setting search_path: %v", err)
	}
	if err := appdb.RunMigrations(ctx, db); err != nil {
		t.Fatalf("running postgres migrations: %v", err)
	}
	return db
}

func seedPostgresPrefixObjects(t *testing.T, ctx context.Context, repos *repository.Repositories, bucketID int64) {
	t.Helper()
	keys := []string{
		"wild%/literal.txt",
		"wildX/literal.txt",
		"under_/literal.txt",
		"underX/literal.txt",
	}
	for i, key := range keys {
		versionID := fmt.Sprintf("01J000000000000000PG%06d", i)
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucketID, key, versionID, 10)); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", key, err)
		}
	}
}

func seedPostgresPrefixMultiparts(t *testing.T, ctx context.Context, repos *repository.Repositories, bucketID int64) {
	t.Helper()
	for _, uploadID := range []string{"pg-prefix-upload-000002", "pg-prefix-upload-000002-next"} {
		if err := repos.Multiparts.Create(ctx, &model.MultipartUpload{
			BucketID:    bucketID,
			Key:         `back\slash/literal.txt`,
			UploadID:    uploadID,
			ContentType: "application/octet-stream",
			Status:      model.MultipartStatusInitiated,
		}); err != nil {
			t.Fatalf("Create multipart(%s): %v", uploadID, err)
		}
	}
}

func seedPostgresPrefixPlanRows(t *testing.T, ctx context.Context, db *bun.DB, bucketID int64) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO objects (bucket_id, key)
		SELECT ?, 'prefix/' || lpad(value::text, 8, '0') || '.txt'
		FROM generate_series(0, 19999) AS series(value)`, bucketID); err != nil {
		t.Fatalf("seeding plan objects: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO object_versions (
			version_id, object_id, bucket_id, key, size, e_tag, checksum,
			content_type, cache_key, in_cache, is_current, is_delete_marker, state
		)
		SELECT
			'pg-prefix-version-' || object_row.id,
			object_row.id,
			object_row.bucket_id,
			object_row.key,
			10,
			'etag',
			'checksum',
			'text/plain',
			'.versions/' || object_row.id,
			TRUE,
			TRUE,
			FALSE,
			'cached'
		FROM objects AS object_row
		WHERE object_row.bucket_id = ? AND object_row.key LIKE 'prefix/%'`, bucketID); err != nil {
		t.Fatalf("seeding plan object versions: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO multipart_uploads (bucket_id, key, upload_id, content_type, status)
		SELECT
			?,
			'prefix/' || lpad(value::text, 8, '0') || '.txt',
			'pg-prefix-plan-upload-' || value,
			'application/octet-stream',
			'initiated'
		FROM generate_series(0, 19999) AS series(value)`, bucketID); err != nil {
		t.Fatalf("seeding plan multipart uploads: %v", err)
	}
}

func seedPostgresStorageRisk(t *testing.T, ctx context.Context, repos *repository.Repositories, bucket *model.Bucket) (string, time.Time) {
	t.Helper()
	version, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "prefix/00010000.txt")
	if err != nil {
		t.Fatalf("GetCurrentVersionByBucketAndKey: %v", err)
	}
	if version == nil {
		t.Fatal("storage risk version is missing")
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
	risk := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "501", "5501", "6501", "https://provider.example/prefix-risk")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)
	checkedAt := time.Now().UTC()
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{{
		LocalDataSetID: risk.ID,
		BucketID:       bucket.ID,
		BucketName:     bucket.Name,
		CopyIndex:      risk.CopyIndex,
		ProviderID:     risk.ProviderID,
		ChainDataSetID: risk.DataSetID,
		Status:         observability.StatusUnavailable,
		LastCheckedAt:  checkedAt,
		Evidence:       map[string]any{},
	}}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}
	return version.VersionID, checkedAt.Add(-time.Hour)
}

type postgresQueryCapture struct {
	queries []string
}

func (h *postgresQueryCapture) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *postgresQueryCapture) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	h.queries = append(h.queries, event.Query)
}

func (h *postgresQueryCapture) reset() {
	h.queries = h.queries[:0]
}

func (h *postgresQueryCapture) last(t *testing.T) string {
	t.Helper()
	if len(h.queries) == 0 {
		t.Fatal("repository did not execute a query")
	}
	return h.queries[len(h.queries)-1]
}

func (h *postgresQueryCapture) match(t *testing.T, fragment string) string {
	t.Helper()
	for _, query := range h.queries {
		if strings.Contains(query, fragment) {
			return query
		}
	}
	t.Fatalf("repository did not execute a query containing %q", fragment)
	return ""
}

func assertPostgresPlanUsesIndex(t *testing.T, db *bun.DB, indexName string, query string) {
	t.Helper()
	var plan []string
	if err := db.NewRaw("EXPLAIN (ANALYZE, BUFFERS) "+query).Scan(context.Background(), &plan); err != nil {
		t.Fatalf("EXPLAIN %s: %v", indexName, err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, indexName) {
		t.Fatalf("plan does not use %s:\n%s", indexName, joined)
	}
	t.Logf("plan for %s:\n%s", indexName, joined)
}

func quotePostgresIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
