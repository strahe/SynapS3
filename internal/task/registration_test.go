package task_test

import (
	"slices"
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestProductionRegistrationClosesAllTaskDependencies(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	want := []model.TaskType{
		model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull,
		model.TaskTypeCacheCapacityReconcile, model.TaskTypeCacheEvict, model.TaskTypeCacheReconcileDurability,
		model.TaskTypeObservabilityRefresh, model.TaskTypeApprovedProviderRefresh, model.TaskTypeEndorsedProviderRefresh,
		model.TaskTypeProviderUploadSpeedTest, model.TaskTypeWalletOperation,
		model.TaskTypeStorageDataSetEnsure, model.TaskTypeStorageDataSetRetire, model.TaskTypeStorageCommit,
		model.TaskTypeBucketProvision, model.TaskTypeUploadPlan, model.TaskTypeProviderReplacementCoordinate,
		model.TaskTypeStorageCleanup,
	}
	got := runtime.registry.Types()
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("registered task types = %v, want %v", got, want)
	}
}

func TestRetirementRegistrationDoesNotRequireCoordinatorMessages(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{retireOnly: true})
	if got := runtime.registry.Types(); !slices.Equal(got, []model.TaskType{model.TaskTypeStorageDataSetRetire}) {
		t.Fatalf("retirement task types = %v", got)
	}
}
