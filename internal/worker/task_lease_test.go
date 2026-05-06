package worker

import (
	"context"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestTaskLeaseRenewalExtendsLeaseUntilStopped(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J0000000000000000000LEASE",
		IdempotencyKey: "upload:lease-renewal",
		Status:         model.TaskStatusPending,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil || claimed.LeaseUntil == nil {
		t.Fatal("expected claimed task with lease")
	}
	oldLeaseUntil := *claimed.LeaseUntil

	stop := startTaskLeaseRenewal(nil, repos, claimed.ID, 30*time.Millisecond)
	defer stop()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, err := repos.Tasks.GetByID(ctx, claimed.ID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if got.LeaseUntil != nil && got.LeaseUntil.After(oldLeaseUntil) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lease_until was not renewed")
}
