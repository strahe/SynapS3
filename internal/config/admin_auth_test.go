package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteAdminInitialPasswordFileReplacesExistingWithRestrictedMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, adminInitialPasswordFileName)
	if err := os.WriteFile(path, []byte("old-password\n"), 0o644); err != nil {
		t.Fatalf("WriteFile old password: %v", err)
	}

	gotPath, err := WriteAdminInitialPasswordFile(dir, "new-password")
	if err != nil {
		t.Fatalf("WriteAdminInitialPasswordFile() error = %v", err)
	}
	if gotPath != path {
		t.Fatalf("path = %q, want %q", gotPath, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile password: %v", err)
	}
	if strings.TrimSpace(string(data)) != "new-password" {
		t.Fatalf("password file = %q, want new-password", strings.TrimSpace(string(data)))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat password: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("password file mode = %o, want 600", got)
	}
}
