package worker

import (
	"context"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

type renewLeaseNotifyRepo struct {
	repository.TaskRepository
	renewed chan struct{}
}

func (r *renewLeaseNotifyRepo) RenewLease(ctx context.Context, task *model.Task, leaseDuration time.Duration) error {
	err := r.TaskRepository.RenewLease(ctx, task, leaseDuration)
	if err == nil {
		select {
		case r.renewed <- struct{}{}:
		default:
		}
	}
	return err
}

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
		Status:         model.TaskStatusQueued,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil || claimed.LeaseUntil == nil {
		t.Fatal("expected claimed task with lease")
	}
	oldLeaseUntil := *claimed.LeaseUntil

	notify := &renewLeaseNotifyRepo{
		TaskRepository: repos.Tasks,
		renewed:        make(chan struct{}, 1),
	}
	repos.Tasks = notify

	stop := startTaskLeaseRenewal(nil, repos, claimed, 30*time.Millisecond)
	defer stop()

	select {
	case <-notify.renewed:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("lease renewal was not observed")
	}

	got, err := repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.LeaseUntil == nil || !got.LeaseUntil.After(oldLeaseUntil) {
		t.Fatal("lease_until was not renewed")
	}
}
