package cache

import "strings"

// EvictionPolicy controls when remotely durable objects are removed from the
// local cache.
type EvictionPolicy string

const (
	EvictionPolicyLRU         EvictionPolicy = "lru"
	EvictionPolicyAfterUpload EvictionPolicy = "after_upload"
	EvictionPolicyNone        EvictionPolicy = "none"
)

// ParseEvictionPolicy returns the canonical eviction policy. The legacy
// "manual" value is accepted as an alias for "none".
func ParseEvictionPolicy(value string) (EvictionPolicy, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(EvictionPolicyLRU):
		return EvictionPolicyLRU, true
	case string(EvictionPolicyAfterUpload):
		return EvictionPolicyAfterUpload, true
	case string(EvictionPolicyNone), "manual":
		return EvictionPolicyNone, true
	default:
		return "", false
	}
}
