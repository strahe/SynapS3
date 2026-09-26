package admin

import (
	"testing"

	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
)

func TestUploadSpeedViewUsesCurrentProfileURL(t *testing.T) {
	oldURL := "https://old.example"
	row := providerbenchmark.Result{
		State:          providerbenchmark.StateSucceeded,
		ServiceURLHash: providerbenchmark.URLHash(oldURL),
		SampleBytes:    1024,
	}
	profile := &observability.ProviderProfile{ServiceURL: "https://new.example"}
	if got := uploadSpeedView(row, profile, &oldURL); got.State != "stale" {
		t.Fatalf("old speed result should be stale after profile URL changes, got %q", got.State)
	}
	if got := uploadSpeedView(row, nil, &oldURL); got.State != string(providerbenchmark.StateSucceeded) {
		t.Fatalf("result should use observed URL when profile is absent, got %q", got.State)
	}
}
