package testpg

import (
	"context"
	"strings"
	"testing"
)

func TestRunCleanupRequiresExactResourceOwnership(t *testing.T) {
	token := strings.Repeat("a", 32)
	name := "s3t_" + token + "_01234567_c_0123456789ab"
	for _, test := range []struct {
		name string
		want byte
	}{
		{name: name, want: 'c'},
		{name: strings.Replace(name, "_c_", "_t_", 1), want: 't'},
		{name: strings.Replace(name, "_c_", "_s_", 1), want: 's'},
		{name: strings.Replace(name, token, strings.Repeat("b", 32), 1)},
		{name: name + "_extra"},
		{name: strings.Replace(name, "01234567", "other", 1)},
		{name: strings.Replace(name, "_c_", "_x_", 1)},
		{name: "postgres"},
	} {
		if got := resourceKind(test.name, token); got != test.want {
			t.Errorf("resource kind %q = %q, want %q", test.name, got, test.want)
		}
	}
	if err := CleanupRun(context.Background(), "invalid DSN", ""); err == nil {
		t.Fatal("cleanup accepted an empty namespace")
	}
	for _, database := range []string{"maintenance", "postgres", "template0", "template1"} {
		if err := dropDatabase(context.Background(), nil, "maintenance", database); err == nil {
			t.Errorf("cleanup accepted protected database %q", database)
		}
	}
}

func TestInvalidPostgresDSNDoesNotDiscloseCredentials(t *testing.T) {
	for _, dsn := range []string{
		"postgres://tester:private-password@localhost/testing?sslmode=invalid",
		"postgres://localhost/testing?password=private-password&sslmode=invalid",
		"host=localhost password='private-password' sslmode=invalid",
	} {
		_, err := parseTestConfig(dsn)
		if err == nil || strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), dsn) {
			t.Fatalf("invalid DSN credential redaction failed: %v", err)
		}
	}
}
