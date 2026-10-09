package repository_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
)

func enqueueCacheEvictionTask(t *testing.T, repos *repository.Repositories, contentID, generation int64, key string) *model.Task {
	t.Helper()
	input, err := json.Marshal(cacheeviction.EvictInput{ContentID: contentID, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	row, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
		Type: model.TaskTypeCacheEvict, IdempotencyKey: key, InputVersion: 1, InputHash: key, Input: input,
		SubjectType: new(model.TaskSubjectStorageContent), SubjectKey: new(strconv.FormatInt(contentID, 10)),
	}))
	if err != nil || !created {
		t.Fatalf("enqueue eviction: created=%v, err=%v", created, err)
	}
	return row
}

func enqueueProviderSpeedTask(t *testing.T, repos *repository.Repositories, providerID, hash, key string) *model.Task {
	t.Helper()
	input, err := json.Marshal(providerbenchmark.Input{ProviderID: providerID, ServiceURLHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	row, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
		Type: model.TaskTypeProviderUploadSpeedTest, IdempotencyKey: key, InputVersion: 1, InputHash: key, Input: input,
		SubjectType: new("provider"), SubjectKey: new(providerID),
	}))
	if err != nil || !created {
		t.Fatalf("enqueue upload speed: created=%v, err=%v", created, err)
	}
	return row
}
