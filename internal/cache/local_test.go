package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLocalFilesystemListsUploadAndContentFiles(t *testing.T) {
	fs := newTestCache(t)
	ctx := t.Context()
	for _, uploadID := range []string{"upload-a", "upload-b"} {
		if err := os.MkdirAll(filepath.Join(fs.RootDir(), ".multipart", uploadID), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(fs.RootDir(), ".multipart", "not-a-directory"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct{ bucket, key string }{
		{"first", ".contents/12"},
		{"first", ".contents/not-an-id"},
		{"first", ".contents/0"},
		{"second", ".contents/3"},
	} {
		if _, err := fs.Put(ctx, entry.bucket, entry.key, strings.NewReader("bytes"), 5); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(fs.RootDir(), "first", ".contents", "13"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fs.Quarantine(ctx, "first", 12); err != nil {
		t.Fatal(err)
	}
	ids, err := fs.ListUploadIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"upload-a", "upload-b"}) {
		t.Fatalf("upload IDs = %v", ids)
	}
	for _, tt := range []struct {
		quarantined bool
		want        string
	}{
		{want: "second/3"},
		{quarantined: true, want: "first/12"},
	} {
		var visited []string
		err := fs.WalkContentFiles(ctx, tt.quarantined, func(bucket string, contentID int64) error {
			visited = append(visited, fmt.Sprintf("%s/%d", bucket, contentID))
			return nil
		})
		if err != nil || !slices.Equal(visited, []string{tt.want}) {
			t.Fatalf("quarantined=%t: visited=%v err=%v", tt.quarantined, visited, err)
		}
	}
	visitErr := errors.New("stop traversal")
	if err := fs.WalkContentFiles(ctx, true, func(string, int64) error { return visitErr }); !errors.Is(err, visitErr) {
		t.Fatalf("callback error = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := fs.WalkContentFiles(cancelled, false, func(string, int64) error {
		t.Fatal("visited content after cancellation")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled traversal = %v", err)
	}
}

func TestQuarantineRetainsBytesAcrossRestartUntilDeletion(t *testing.T) {
	fs := newTestCache(t)
	ctx := t.Context()
	const contents = "damaged bytes"
	if _, err := fs.Put(ctx, "bucket", ".contents/7", strings.NewReader(contents), int64(len(contents))); err != nil {
		t.Fatal(err)
	}
	if err := fs.Quarantine(ctx, "bucket", 7); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fs.RootDir(), "bucket", ".contents", "7")); !os.IsNotExist(err) {
		t.Fatalf("content remains readable: %v", err)
	}
	quarantine := filepath.Join(fs.RootDir(), "bucket", ".quarantine", "7")
	data, err := os.ReadFile(quarantine)
	if err != nil || string(data) != contents || fs.UsedBytes() != int64(len(contents)) {
		t.Fatalf("retained bytes=%q used=%d err=%v", data, fs.UsedBytes(), err)
	}
	restarted, err := NewFilesystem(fs.RootDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.UsedBytes() != int64(len(contents)) {
		t.Fatalf("restart lost quarantine charge: %d", restarted.UsedBytes())
	}
	for range 2 {
		if err := restarted.DeleteQuarantined(ctx, "bucket", 7); err != nil {
			t.Fatal(err)
		}
		if restarted.UsedBytes() != 0 {
			t.Fatalf("deleted quarantine still charged: %d", restarted.UsedBytes())
		}
	}
	if _, err := os.Stat(quarantine); !os.IsNotExist(err) {
		t.Fatalf("quarantine remains after deletion: %v", err)
	}
}

func TestQuarantineReplacementAccountsOnlySuccessfulRename(t *testing.T) {
	fs := newTestCache(t)
	ctx := t.Context()
	if _, err := fs.Put(ctx, "bucket", ".contents/9", strings.NewReader("old"), 3); err != nil {
		t.Fatal(err)
	}
	if err := fs.Quarantine(ctx, "bucket", 9); err != nil {
		t.Fatal(err)
	}
	if err := fs.Quarantine(ctx, "bucket", 9); !os.IsNotExist(err) {
		t.Fatalf("missing source rename = %v", err)
	}
	if fs.UsedBytes() != 3 {
		t.Fatalf("failed rename changed capacity charge: %d", fs.UsedBytes())
	}
	const replacement = "replacement"
	if _, err := fs.Put(ctx, "bucket", ".contents/9", strings.NewReader(replacement), int64(len(replacement))); err != nil {
		t.Fatal(err)
	}
	if err := fs.Quarantine(ctx, "bucket", 9); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(fs.RootDir(), "bucket", ".quarantine", "9"))
	if err != nil || string(data) != replacement || fs.UsedBytes() != int64(len(replacement)) {
		t.Fatalf("replacement bytes=%q used=%d err=%v", data, fs.UsedBytes(), err)
	}
}

func TestQuarantineDirectoryDestinationPreservesSourceAndCapacity(t *testing.T) {
	fs := newTestCache(t)
	ctx := t.Context()
	if _, err := fs.Put(ctx, "bucket", ".contents/12", strings.NewReader("data"), 4); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(fs.RootDir(), "bucket", ".quarantine", "12"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fs.Quarantine(ctx, "bucket", 12); err == nil {
		t.Fatal("quarantine succeeded over a directory")
	}
	data, err := os.ReadFile(filepath.Join(fs.RootDir(), "bucket", ".contents", "12"))
	if err != nil || string(data) != "data" || fs.UsedBytes() != 4 {
		t.Fatalf("source bytes=%q used=%d err=%v", data, fs.UsedBytes(), err)
	}
}
