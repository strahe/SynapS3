package transfer

import "testing"

func TestStoreRejectsNegativeUploadConcurrency(t *testing.T) {
	if _, err := NewStoreHandler(StoreDependencies{Coordinator: &CopyCoordinator{}, UploadConcurrency: -1}); err == nil {
		t.Fatal("created store handler with negative upload concurrency")
	}
}
