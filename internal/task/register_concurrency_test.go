package task_test

import (
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestRegisteredTaskConcurrencyDefaults(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	limits := map[model.TaskType]int{
		model.TaskTypeStorageStore:            4,
		model.TaskTypeStoragePull:             4,
		model.TaskTypeStorageCommit:           4,
		model.TaskTypeStorageDataSetEnsure:    2,
		model.TaskTypeStorageCleanup:          2,
		model.TaskTypeStorageDataSetRetire:    1,
		model.TaskTypeWalletOperation:         1,
		model.TaskTypeProviderUploadSpeedTest: 1,
	}
	for _, taskType := range runtime.registry.Types() {
		definition, ok := runtime.registry.Definition(taskType)
		if !ok || definition.MaxConcurrency != limits[taskType] {
			t.Errorf("%s concurrency = %d, want %d", taskType, definition.MaxConcurrency, limits[taskType])
		}
	}
}
