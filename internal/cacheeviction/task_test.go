package cacheeviction_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
)

func TestEvictInputRoundTripNormalizesAccessSnapshot(t *testing.T) {
	accessedAt := time.Date(2026, time.July, 28, 8, 9, 10, 123456789, time.FixedZone("test", 8*60*60))
	raw, err := json.Marshal(cacheeviction.EvictInput{
		ContentID: 41, Generation: 3, AccessedAt: &accessedAt,
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := cacheeviction.ParseEvictInput(&model.Task{Input: raw})
	if err != nil {
		t.Fatalf("ParseEvictInput: %v", err)
	}
	if want := accessedAt.UTC().Truncate(time.Microsecond); got.AccessedAt == nil || !got.AccessedAt.Equal(want) {
		t.Fatalf("accessed at = %v, want %v", got.AccessedAt, want)
	}
	if got.ContentID != 41 || got.Generation != 3 {
		t.Fatalf("input = %#v", got)
	}
}

func TestParseEvictInputRejectsIncompleteIdentity(t *testing.T) {
	for name, task := range map[string]*model.Task{
		"nil task":        nil,
		"missing input":   {},
		"missing content": {Input: json.RawMessage(`{"generation":1}`)},
		"zero generation": {Input: json.RawMessage(`{"content_id":41}`)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := cacheeviction.ParseEvictInput(task); err == nil {
				t.Fatal("ParseEvictInput succeeded")
			}
		})
	}
}

func TestCacheTaskKeysIncludeGeneration(t *testing.T) {
	if cacheeviction.EvictTaskKey(41, 1) == cacheeviction.EvictTaskKey(41, 2) {
		t.Fatal("eviction generations share an idempotency key")
	}
	if cacheeviction.DurabilityTaskKey(7, 1) == cacheeviction.DurabilityTaskKey(7, 2) {
		t.Fatal("durability generations share an idempotency key")
	}
}
