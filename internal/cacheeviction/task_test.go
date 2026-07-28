package cacheeviction_test

import (
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
)

func TestLRUTaskPayloadRoundTripUsesDatabasePrecision(t *testing.T) {
	accessedAt := time.Date(2026, time.July, 28, 8, 9, 10, 123456789, time.FixedZone("test", 8*60*60))
	task := cacheeviction.NewLRUTask(cacheeviction.Candidate{
		ObjectID:   12,
		VersionID:  "01J0000000000000000000LRU1",
		Size:       42,
		AccessedAt: accessedAt,
	}, 3, time.Now())

	payload, err := cacheeviction.ParseLRUTaskPayload(task)
	if err != nil {
		t.Fatalf("ParseLRUTaskPayload: %v", err)
	}
	want := accessedAt.UTC().Truncate(time.Microsecond)
	if !payload.AccessedAt.Equal(want) {
		t.Fatalf("accessed at = %v, want %v", payload.AccessedAt, want)
	}
	if task.Stage == nil || *task.Stage != cacheeviction.StageLRU {
		t.Fatalf("stage = %v, want %s", task.Stage, cacheeviction.StageLRU)
	}
	if task.IdempotencyKey != "evict_cache:lru:"+task.RefVersionID {
		t.Fatalf("idempotency key = %q", task.IdempotencyKey)
	}
}

func TestParseLRUTaskPayloadRejectsMalformedSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		payload map[string]any
	}{
		{name: "missing", payload: nil},
		{name: "wrong type", payload: map[string]any{"cache_accessed_at": float64(1)}},
		{name: "invalid timestamp", payload: map[string]any{"cache_accessed_at": "not-a-time"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cacheeviction.ParseLRUTaskPayload(&model.Task{Payload: tt.payload})
			if err == nil {
				t.Fatal("ParseLRUTaskPayload returned nil error")
			}
		})
	}
}

func TestAfterUploadTaskUsesIndependentStableKey(t *testing.T) {
	task := cacheeviction.NewAfterUploadTask(12, "01J0000000000000000000POST", 5, time.Now())
	if task.Stage == nil || *task.Stage != cacheeviction.StageAfterUpload {
		t.Fatalf("stage = %v, want %s", task.Stage, cacheeviction.StageAfterUpload)
	}
	if task.IdempotencyKey != "evict_cache:"+task.RefVersionID {
		t.Fatalf("idempotency key = %q", task.IdempotencyKey)
	}
}
