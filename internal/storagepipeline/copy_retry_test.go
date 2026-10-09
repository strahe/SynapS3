package storagepipeline

import (
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestCopyRetryDecision(t *testing.T) {
	base := CopyRetryFacts{Status: model.StorageCopyStatusFailed, Current: true, DataSetStatus: model.StorageDataSetStatusReady}
	for _, method := range []model.StorageCopyTransferMethod{model.StorageCopyTransferMethodPeerPull, model.StorageCopyTransferMethodCacheRestore, model.StorageCopyTransferMethodIngress} {
		for _, source := range []bool{false, true} {
			for _, cached := range []bool{false, true} {
				for _, otherIngress := range []bool{false, true} {
					facts := base
					facts.Method = method
					facts.CacheAvailable = cached
					facts.OtherIngress = otherIngress
					if source {
						facts.ReadableSources = 1
					}
					want := model.StorageCopyTransferMethodCacheRestore
					if source || !cached {
						want = model.StorageCopyTransferMethodPeerPull
					} else if method == model.StorageCopyTransferMethodIngress && !otherIngress {
						want = model.StorageCopyTransferMethodIngress
					}
					next, block, ok := DecideCopyRetry(facts)
					if !ok || next != want || block != "" {
						t.Fatalf("facts=%+v: next=%s block=%s ok=%v", facts, next, block, ok)
					}
				}
			}
		}
	}
	base.Method = model.StorageCopyTransferMethodPeerPull
	base.ReadableSources = 1
	for _, tt := range []struct {
		name   string
		change func(*CopyRetryFacts)
		block  CopyRetryBlock
	}{
		{"deleted", func(f *CopyRetryFacts) { f.ObjectDeleted = true }, CopyRetryObjectDeleted},
		{"replacement", func(f *CopyRetryFacts) { f.ReplacementInProgress = true }, CopyRetryReplacementInProgress},
		{"draining", func(f *CopyRetryFacts) { f.Current = false }, CopyRetryStorageServiceUnavailable},
		{"not ready", func(f *CopyRetryFacts) { f.DataSetStatus = model.StorageDataSetStatusFailed }, CopyRetryStorageServiceUnavailable},
		{"unresolved", func(f *CopyRetryFacts) { f.UnresolvedPull = true }, CopyRetryRecoveryRequiresAttention},
		{"owned", func(f *CopyRetryFacts) { f.Owned = true }, ""},
		{"sealed", func(f *CopyRetryFacts) { f.Sealed = true }, ""},
		{"pending", func(f *CopyRetryFacts) { f.Status = model.StorageCopyStatusPending }, ""},
		{"unknown method", func(f *CopyRetryFacts) { f.Method = "future" }, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := base
			tt.change(&f)
			_, block, ok := DecideCopyRetry(f)
			if ok || block != tt.block {
				t.Fatalf("block=%s ok=%v", block, ok)
			}
		})
	}
}
