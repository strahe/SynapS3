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

// ParseEvictionPolicy returns the canonical eviction policy.
func ParseEvictionPolicy(value string) (EvictionPolicy, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(EvictionPolicyLRU):
		return EvictionPolicyLRU, true
	case string(EvictionPolicyAfterUpload):
		return EvictionPolicyAfterUpload, true
	case string(EvictionPolicyNone):
		return EvictionPolicyNone, true
	default:
		return "", false
	}
}

// RehydratesColdReads reports whether a complete read from a storage provider
// restores the object to the local cache. Under after_upload nothing would
// release such a copy again.
func (p EvictionPolicy) RehydratesColdReads() bool {
	return p != EvictionPolicyAfterUpload
}

// EnqueuesAfterUploadEviction reports whether upload finalization must create
// an eviction task in the same transaction.
func (p EvictionPolicy) EnqueuesAfterUploadEviction() bool {
	return p == EvictionPolicyAfterUpload
}
