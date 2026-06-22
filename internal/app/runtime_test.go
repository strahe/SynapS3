package app

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/config"
)

func TestNewRuntimeReportsAllMissingDependencies(t *testing.T) {
	t.Parallel()
	_, err := NewRuntime(context.Background(), RuntimeOptions{})
	if err == nil {
		t.Fatal("NewRuntime succeeded without dependencies")
	}
	for _, dependency := range []string{
		"config", "database", "settings", "logger", "filecoin storage", "filecoin wallet query",
		"filecoin wallet operator", "filecoin receipts", "filecoin readiness", "filecoin observability",
	} {
		if !strings.Contains(err.Error(), dependency) {
			t.Errorf("NewRuntime error = %q, missing dependency %q", err, dependency)
		}
	}
}

func TestRuntimeConfigurationHelpers(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		policy string
		want   bool
	}{
		{policy: "lru", want: true},
		{policy: "LRU", want: true},
		{policy: " LrU ", want: true},
		{policy: "manual", want: false},
		{policy: "none", want: false},
	} {
		if got := autoEvictEnabled(test.policy); got != test.want {
			t.Errorf("autoEvictEnabled(%q) = %v, want %v", test.policy, got, test.want)
		}
	}

	serverConfig := config.ServerConfig{
		Port: ":8080", MaxConnections: 1, MaxRequests: 1,
		TLS: config.TLSConfig{
			Enabled: true, CertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
			KeyFile: filepath.Join(t.TempDir(), "missing-key.pem"),
		},
	}
	if _, err := s3ServerOptions(serverConfig); err == nil || !strings.Contains(err.Error(), "TLS certificate") {
		t.Fatalf("s3ServerOptions error = %v, want TLS certificate context", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	accessConfig := config.LoggingS3AccessConfig{Enabled: true, Level: "debug"}
	if got := s3AccessLogger(logger, accessConfig); got == nil {
		t.Fatal("s3AccessLogger(enabled) = nil, want logger")
	}
	accessConfig.Enabled = false
	if got := s3AccessLogger(logger, accessConfig); got != nil {
		t.Fatalf("s3AccessLogger(disabled) = %#v, want nil", got)
	}
}
