package repository_test

import (
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestCacheLocalPresenceCannotOverrideANewerCacheGeneration(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "local-presence")
	version := newObjectVersion(bucket.ID, "object", model.NewVersionID(), 10)
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	contentID := *version.ContentID
	if err := repos.Objects.ClearContentCachePresence(ctx, contentID); err != nil {
		t.Fatal(err)
	}
	states, err := repos.CacheLocal.ListStates(ctx, []int64{contentID})
	if err != nil {
		t.Fatal(err)
	}
	observed := states[contentID]
	if observed.InCache || !observed.Referenced || observed.BucketName != bucket.Name {
		t.Fatalf("observed state = %#v", observed)
	}
	if err := repos.Objects.ClearContentCachePresence(ctx, contentID); err != nil {
		t.Fatal(err)
	}
	if restored, err := repos.CacheLocal.RestorePresence(ctx, observed); err != nil || restored {
		t.Fatalf("restore from stale observation = %t, err=%v", restored, err)
	}
	states, err = repos.CacheLocal.ListStates(ctx, []int64{contentID})
	if err != nil {
		t.Fatal(err)
	}
	if restored, err := repos.CacheLocal.RestorePresence(ctx, states[contentID]); err != nil || !restored {
		t.Fatalf("restore from current observation = %t, err=%v", restored, err)
	}
	current, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || current == nil || !current.InCache {
		t.Fatalf("restored version = %#v, err=%v", current, err)
	}
	if restored, err := repos.CacheLocal.RestorePresence(ctx, states[contentID]); err != nil || restored {
		t.Fatalf("repeat restoration = %t, err=%v", restored, err)
	}
}

func TestCacheLocalQuarantineRetainsUndurableContentAndDeletesOrphans(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "local-quarantine")
	contentID := seedContent(t, repos, bucket.ID, "quarantined", 10)
	called := false
	deleted, err := repos.CacheLocal.DeleteQuarantinedIfSafe(t.Context(), contentID, func() error {
		called = true
		return nil
	})
	if err != nil || deleted || called {
		t.Fatalf("undurable quarantine deletion = %t, called=%t, err=%v", deleted, called, err)
	}
	deleted, err = repos.CacheLocal.DeleteQuarantinedIfSafe(t.Context(), contentID+1000000, func() error {
		called = true
		return nil
	})
	if err != nil || !deleted || !called {
		t.Fatalf("orphan quarantine deletion = %t, called=%t, err=%v", deleted, called, err)
	}
}
