package worker_test

import (
	"testing"

	"github.com/strahe/synaps3/internal/types"
)

func onChainID(t *testing.T, value string) types.OnChainID {
	t.Helper()
	id, err := types.ParseOnChainID("test id", value)
	if err != nil {
		t.Fatalf("parse on-chain id %q: %v", value, err)
	}
	return id
}

func onChainIDPtr(t *testing.T, value string) *types.OnChainID {
	t.Helper()
	id := onChainID(t, value)
	return &id
}

func onChainIDPtrString(id *types.OnChainID) string {
	if id == nil {
		return ""
	}
	return id.String()
}
