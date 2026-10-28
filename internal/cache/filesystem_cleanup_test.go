package cache

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDeleteUploadPartialFailureKeepsCapacityAccurate(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires unprivileged POSIX permissions")
	}
	root := t.TempDir()
	uploadDir := filepath.Join(root, ".multipart", "upload")
	blocked := filepath.Join(uploadDir, "blocked")
	if err := os.MkdirAll(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{filepath.Join(uploadDir, "1"), filepath.Join(blocked, "2")} {
		if err := os.WriteFile(name, []byte("abc"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f, err := NewFilesystem(root, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(blocked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o700) })
	if err := f.DeleteUpload(context.Background(), "upload"); err == nil {
		t.Fatal("expected partial deletion failure")
	}
	if got := f.UsedBytes(); got != 3 {
		t.Fatalf("partially removed upload uses %d bytes, want 3", got)
	}
	if err := os.Chmod(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteUpload(context.Background(), "upload"); err != nil {
		t.Fatal(err)
	}
	if got := f.UsedBytes(); got != 0 {
		t.Fatalf("removed upload uses %d bytes, want 0", got)
	}
}
