package repository_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"

	_ "modernc.org/sqlite"
)

// testDB creates a fresh in-memory SQLite DB with all migrations applied.
func testDB(t *testing.T) *bun.DB {
	t.Helper()
	return testutil.NewTestDB(t)
}

func onChainID(t *testing.T, value string) types.OnChainID {
	t.Helper()
	id, err := types.ParseOnChainID("test id", value)
	if err != nil {
		t.Fatalf("parse on-chain id %q: %v", value, err)
	}
	return id
}

func onChainIDPtr(t *testing.T, value string) *types.OnChainID {
	t.Helper()
	id := onChainID(t, value)
	return &id
}

// seedBucket inserts a bucket and returns it.
func seedBucket(t *testing.T, db *bun.DB, name string) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	_, err := db.NewInsert().Model(bucket).Exec(context.Background())
	if err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	testutil.OpenBucketReplicaSlots(t, db, bucket.ID, bucket.DefaultCopies)
	return bucket
}

// markSourceReady finishes the creation of a freshly bound generation, the
// only state a replacement accepts as its source.
func markSourceReady(t *testing.T, repos *repository.Repositories, dataSetID int64) {
	t.Helper()
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: dataSetID, DataSetID: onChainID(t, strconv.FormatInt(900000+dataSetID, 10)),
	}); err != nil {
		t.Fatalf("MarkDataSetReady(%d): %v", dataSetID, err)
	}
}
