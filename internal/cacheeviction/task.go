package cacheeviction

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

func EvictTaskKey(contentID, generation int64) string {
	return EvictTaskKeyPrefix + strconv.FormatInt(contentID, 10) + ":" + strconv.FormatInt(generation, 10)
}

func DurabilityTaskKey(bucketID, generation int64) string {
	return DurabilityTaskKeyPrefix + strconv.FormatInt(bucketID, 10) + ":" + strconv.FormatInt(generation, 10)
}

const (
	EvictTaskKeyPrefix      = "cache-evict:"
	DurabilityTaskKeyPrefix = "cache-durability:"
)

var (
	ErrDurabilityThreshold = errors.New("minimum durable copies not met")
	ErrNoLongerEligible    = errors.New("cache entry is no longer eligible")
	ErrAccessChanged       = errors.New("cache access snapshot changed")
)

// Candidate is one cached content payload, not one object version: residency
// is content-addressed, so several versions of identical bytes share a single
// eviction decision.
type Candidate struct {
	ContentID  int64     `bun:"content_id"`
	BucketID   int64     `bun:"bucket_id"`
	Size       int64     `bun:"content_size"`
	AccessedAt time.Time `bun:"cache_accessed_at"`
}

// EvictInput identifies one cache generation. AccessedAt is present for an LRU
// authorization and omitted when remote durability directly authorizes removal.
type EvictInput struct {
	ContentID  int64      `json:"content_id"`
	Generation int64      `json:"generation"`
	AccessedAt *time.Time `json:"accessed_at,omitempty"`
}

type DurabilityInput struct {
	BucketID   int64 `json:"bucket_id"`
	Generation int64 `json:"generation"`
}

type AuthorizedDeletion struct {
	Content    model.StorageContent
	BucketName string
}

func NormalizeAccessTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func ParseEvictInput(task *model.Task) (EvictInput, error) {
	if task == nil {
		return EvictInput{}, errors.New("nil cache eviction task")
	}
	var input EvictInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return EvictInput{}, fmt.Errorf("decoding cache eviction input: %w", err)
	}
	if input.ContentID < 1 || input.Generation < 1 {
		return EvictInput{}, errors.New("cache eviction input is incomplete")
	}
	if input.AccessedAt != nil {
		normalized := NormalizeAccessTime(*input.AccessedAt)
		input.AccessedAt = &normalized
	}
	return input, nil
}

func ParseDurabilityInput(task *model.Task) (DurabilityInput, error) {
	if task == nil {
		return DurabilityInput{}, errors.New("nil durability reconciliation task")
	}
	var input DurabilityInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return DurabilityInput{}, fmt.Errorf("decoding durability input: %w", err)
	}
	if input.BucketID < 1 || input.Generation < 1 {
		return DurabilityInput{}, errors.New("durability input is incomplete")
	}
	return input, nil
}

func ValidateEvictInput(input *EvictInput) error {
	if input == nil || input.ContentID < 1 || input.Generation < 1 {
		return errors.New("content_id and generation are required")
	}
	if input.AccessedAt != nil {
		normalized := NormalizeAccessTime(*input.AccessedAt)
		input.AccessedAt = &normalized
	}
	return nil
}

func ValidateDurabilityInput(input *DurabilityInput) error {
	if input == nil || input.BucketID < 1 || input.Generation < 1 {
		return errors.New("bucket_id and generation are required")
	}
	return nil
}
