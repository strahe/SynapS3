package providerselect

import (
	"cmp"
	"slices"
)

type IngressEntry struct {
	CopyIndex      int
	BytesPerSecond int64
}

// RankIngress orders bound replicas by measured speed, then by replica slot.
func RankIngress(entries []IngressEntry) []IngressEntry {
	ranked := slices.Clone(entries)
	slices.SortStableFunc(ranked, func(a, b IngressEntry) int {
		if n := cmp.Compare(max(b.BytesPerSecond, 0), max(a.BytesPerSecond, 0)); n != 0 {
			return n
		}
		return cmp.Compare(a.CopyIndex, b.CopyIndex)
	})
	return ranked
}
