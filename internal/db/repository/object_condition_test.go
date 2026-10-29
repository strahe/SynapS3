package repository_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestObjectRepo_SkipsCurrentVersionLookupWithoutConditions(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "optional-object-condition")
	version := withContent(t, repos, newObjectVersion(bucket.ID, "key", model.NewVersionID(), 10))
	capture := &postgresQueryCapture{}
	testutil.AddQueryHook(db, capture)
	countLookups := func() int {
		count := 0
		for _, query := range capture.queries {
			if strings.Contains(query, "current_object.current_version_id = object_version.version_id") {
				count++
			}
		}
		return count
	}
	rejected := errors.New("condition rejected the missing object")
	called := false
	condition := func(current *model.ObjectVersion) error {
		called = true
		if current != nil {
			t.Fatalf("current version = %#v, want no object", current)
		}
		return rejected
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version, nil, condition, nil); !errors.Is(err, rejected) {
		t.Fatalf("conditional write error = %v, want %v", err, rejected)
	}
	if !called || countLookups() != 1 {
		t.Fatalf("condition called = %t, current version lookups = %d, want true and 1", called, countLookups())
	}
	capture.reset()
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version, nil); err != nil {
		t.Fatal(err)
	}
	if countLookups() != 0 {
		t.Fatalf("unconditional write made %d current version lookups", countLookups())
	}
}
