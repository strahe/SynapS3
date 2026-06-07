package testutil

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestNewTestDBCreatesIsolatedDatabases(t *testing.T) {
	for _, tt := range []struct {
		name  string
		newDB func(*testing.T) *bun.DB
	}{
		{name: "Memory", newDB: NewTestDB},
		{name: "File", newDB: NewTestFileDB},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertTestDBsAreIsolated(t, tt.newDB)
		})
	}
}

func assertTestDBsAreIsolated(t *testing.T, newDB func(*testing.T) *bun.DB) {
	t.Helper()
	ctx := context.Background()
	first := newDB(t)
	second := newDB(t)

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
