package testutil

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestNewTestDBCreatesIsolatedDatabases(t *testing.T) {
	ctx := context.Background()
	first := NewTestDB(t)
	second := NewTestDB(t)

	bucket := &model.Bucket{Name: "isolated-db-bucket", Status: model.BucketStatusActive}
	if _, err := first.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("insert bucket into first db: %v", err)
	}

	count, err := second.NewSelect().Model((*model.Bucket)(nil)).Where("name = ?", bucket.Name).Count(ctx)
	if err != nil {
		t.Fatalf("count bucket in second db: %v", err)
	}
	if count != 0 {
		t.Fatalf("second test db bucket count = %d, want 0", count)
	}
}
