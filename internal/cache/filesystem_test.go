package cache

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestCache(t *testing.T) *Filesystem {
	t.Helper()
	dir := t.TempDir()
	fs, err := NewFilesystem(dir, 0)
	if err != nil {
		t.Fatalf("NewFilesystem: %v", err)
	}
	return fs
}

func TestPutGetRoundtrip(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()
	data := []byte("hello world")

	info, err := fs.Put(ctx, "bkt", "key1", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if info.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", info.Size, len(data))
	}

	wantMD5 := md5.Sum(data)
	if info.ETag != hex.EncodeToString(wantMD5[:]) {
		t.Errorf("ETag = %s, want %s", info.ETag, hex.EncodeToString(wantMD5[:]))
	}

	wantSHA := sha256.Sum256(data)
	if info.Checksum != hex.EncodeToString(wantSHA[:]) {
		t.Errorf("Checksum = %s, want %s", info.Checksum, hex.EncodeToString(wantSHA[:]))
	}

	rc, getInfo, err := fs.Get(ctx, "bkt", "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("Get data = %q, want %q", got, data)
	}
	if getInfo.Size != int64(len(data)) {
		t.Errorf("Get Size = %d, want %d", getInfo.Size, len(data))
	}
}

func TestPutOverwrite(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	data1 := []byte("version1")
	data2 := []byte("version2-longer")

	_, err := fs.Put(ctx, "bkt", "key", bytes.NewReader(data1))
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if fs.UsedBytes() != int64(len(data1)) {
		t.Errorf("UsedBytes after v1 = %d, want %d", fs.UsedBytes(), len(data1))
	}

	_, err = fs.Put(ctx, "bkt", "key", bytes.NewReader(data2))
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	// UsedBytes should reflect the new file, not sum of old + new.
	if fs.UsedBytes() != int64(len(data2)) {
		t.Errorf("UsedBytes after v2 = %d, want %d", fs.UsedBytes(), len(data2))
	}

	rc, _, _ := fs.Get(ctx, "bkt", "key")
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data2) {
		t.Errorf("Get after overwrite = %q, want %q", got, data2)
	}
}

func TestDeleteAndExists(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()
	data := []byte("to-delete")

	fs.Put(ctx, "bkt", "key", bytes.NewReader(data))

	if !fs.Exists(ctx, "bkt", "key") {
		t.Error("Exists = false after Put, want true")
	}

	if err := fs.Delete(ctx, "bkt", "key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if fs.Exists(ctx, "bkt", "key") {
		t.Error("Exists = true after Delete, want false")
	}

	if fs.UsedBytes() != 0 {
		t.Errorf("UsedBytes after Delete = %d, want 0", fs.UsedBytes())
	}
}

func TestDeleteIdempotent(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	// Deleting a non-existent key should not error.
	if err := fs.Delete(ctx, "bkt", "nonexistent"); err != nil {
		t.Errorf("Delete non-existent: %v", err)
	}
}

func TestGetNonExistent(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	_, _, err := fs.Get(ctx, "bkt", "nope")
	if !os.IsNotExist(err) {
		t.Errorf("Get non-existent: err = %v, want os.ErrNotExist", err)
	}
}

func TestUsedBytesTracking(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	if fs.UsedBytes() != 0 {
		t.Fatalf("initial UsedBytes = %d, want 0", fs.UsedBytes())
	}

	d1 := []byte("aaaa") // 4 bytes
	d2 := []byte("bbbbbbb") // 7 bytes

	fs.Put(ctx, "b", "k1", bytes.NewReader(d1))
	if fs.UsedBytes() != 4 {
		t.Errorf("after k1: UsedBytes = %d, want 4", fs.UsedBytes())
	}

	fs.Put(ctx, "b", "k2", bytes.NewReader(d2))
	if fs.UsedBytes() != 11 {
		t.Errorf("after k2: UsedBytes = %d, want 11", fs.UsedBytes())
	}

	fs.Delete(ctx, "b", "k1")
	if fs.UsedBytes() != 7 {
		t.Errorf("after delete k1: UsedBytes = %d, want 7", fs.UsedBytes())
	}

	fs.Delete(ctx, "b", "k2")
	if fs.UsedBytes() != 0 {
		t.Errorf("after delete k2: UsedBytes = %d, want 0", fs.UsedBytes())
	}
}

func TestBucketDir(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	if err := fs.CreateBucketDir(ctx, "mybucket"); err != nil {
		t.Fatalf("CreateBucketDir: %v", err)
	}

	p := filepath.Join(fs.root, "mybucket")
	if _, err := os.Stat(p); err != nil {
		t.Errorf("bucket dir not created: %v", err)
	}

	if err := fs.DeleteBucketDir(ctx, "mybucket"); err != nil {
		t.Fatalf("DeleteBucketDir: %v", err)
	}

	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Errorf("bucket dir still exists after delete")
	}
}

func TestDeleteBucketDirAccounting(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	fs.Put(ctx, "bkt", "a", bytes.NewReader([]byte("aaa")))
	fs.Put(ctx, "bkt", "b", bytes.NewReader([]byte("bbbbb")))
	fs.Put(ctx, "other", "c", bytes.NewReader([]byte("cc")))

	if fs.UsedBytes() != 10 {
		t.Fatalf("UsedBytes = %d, want 10", fs.UsedBytes())
	}

	if err := fs.DeleteBucketDir(ctx, "bkt"); err != nil {
		t.Fatalf("DeleteBucketDir: %v", err)
	}

	// Should have subtracted the 8 bytes from "bkt" files.
	if fs.UsedBytes() != 2 {
		t.Errorf("UsedBytes after DeleteBucketDir = %d, want 2", fs.UsedBytes())
	}
}

func TestPathTraversal(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	cases := []struct {
		bucket, key string
	}{
		{"../escape", "key"},
		{"bkt", "../../etc/passwd"},
		{"..", ".."},
		{"bkt/../..", "key"},
	}

	for _, tc := range cases {
		t.Run(tc.bucket+"/"+tc.key, func(t *testing.T) {
			_, err := fs.Put(ctx, tc.bucket, tc.key, strings.NewReader("bad"))
			if err != ErrInvalidPath {
				t.Errorf("Put: err = %v, want ErrInvalidPath", err)
			}

			_, _, err = fs.Get(ctx, tc.bucket, tc.key)
			if err != ErrInvalidPath {
				t.Errorf("Get: err = %v, want ErrInvalidPath", err)
			}

			err = fs.Delete(ctx, tc.bucket, tc.key)
			if err != ErrInvalidPath {
				t.Errorf("Delete: err = %v, want ErrInvalidPath", err)
			}

			if fs.Exists(ctx, tc.bucket, tc.key) {
				t.Error("Exists = true for traversal path")
			}
		})
	}

	// Bucket dir operations
	err := fs.CreateBucketDir(ctx, "../escape")
	if err != ErrInvalidPath {
		t.Errorf("CreateBucketDir: err = %v, want ErrInvalidPath", err)
	}

	err = fs.DeleteBucketDir(ctx, "../escape")
	if err != ErrInvalidPath {
		t.Errorf("DeleteBucketDir: err = %v, want ErrInvalidPath", err)
	}

	// Empty bucket must not resolve to cache root (prevents os.RemoveAll(root)).
	err = fs.DeleteBucketDir(ctx, "")
	if err != ErrInvalidPath {
		t.Errorf("DeleteBucketDir empty bucket: err = %v, want ErrInvalidPath", err)
	}
	err = fs.DeleteBucketDir(ctx, ".")
	if err != ErrInvalidPath {
		t.Errorf("DeleteBucketDir dot bucket: err = %v, want ErrInvalidPath", err)
	}
}

func TestStartupWalk(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create a first cache, put some files.
	fs1, err := NewFilesystem(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	fs1.Put(ctx, "b", "k1", bytes.NewReader([]byte("aaaa")))
	fs1.Put(ctx, "b", "k2", bytes.NewReader([]byte("bbb")))

	// Simulate a stale temp file.
	tmpFile := filepath.Join(dir, "b", ".synaps3-stale.tmp")
	os.WriteFile(tmpFile, []byte("stale"), 0o644)

	// Create a new cache from the same dir — should recover usedBytes and remove temp.
	fs2, err := NewFilesystem(dir, 0)
	if err != nil {
		t.Fatal(err)
	}

	if fs2.UsedBytes() != 7 { // 4 + 3 (temp file should not be counted)
		t.Errorf("recovered UsedBytes = %d, want 7", fs2.UsedBytes())
	}

	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error("stale temp file was not removed on startup")
	}
}

func TestCapacityEnforcement(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create a cache with 10-byte limit.
	fs, err := NewFilesystem(dir, 10)
	if err != nil {
		t.Fatal(err)
	}

	// First write: 8 bytes — should succeed.
	_, err = fs.Put(ctx, "b", "k1", bytes.NewReader([]byte("12345678")))
	if err != nil {
		t.Fatalf("Put within capacity: %v", err)
	}

	// Second write: 5 bytes — would exceed 10-byte limit.
	_, err = fs.Put(ctx, "b", "k2", bytes.NewReader([]byte("12345")))
	if err != ErrCacheFull {
		t.Errorf("Put exceeding capacity: err = %v, want ErrCacheFull", err)
	}

	// Overwrite with smaller data: 3 bytes — should succeed (replaces 8 with 3).
	_, err = fs.Put(ctx, "b", "k1", bytes.NewReader([]byte("abc")))
	if err != nil {
		t.Fatalf("Put overwrite within capacity: %v", err)
	}

	if fs.UsedBytes() != 3 {
		t.Errorf("UsedBytes = %d, want 3", fs.UsedBytes())
	}

	// Now 7 bytes free — 5-byte write should succeed.
	_, err = fs.Put(ctx, "b", "k2", bytes.NewReader([]byte("12345")))
	if err != nil {
		t.Fatalf("Put after freeing space: %v", err)
	}
}

func TestCapacityExactBoundary(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Cache with 10-byte limit.
	fs, err := NewFilesystem(dir, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Write exactly 10 bytes — should succeed (exact fit).
	_, err = fs.Put(ctx, "b", "k1", bytes.NewReader([]byte("1234567890")))
	if err != nil {
		t.Fatalf("Put at exact capacity: %v", err)
	}
	if fs.UsedBytes() != 10 {
		t.Errorf("UsedBytes = %d, want 10", fs.UsedBytes())
	}

	// Overwrite with exactly 10 bytes — should succeed (same size).
	_, err = fs.Put(ctx, "b", "k1", bytes.NewReader([]byte("abcdefghij")))
	if err != nil {
		t.Fatalf("Overwrite at exact capacity: %v", err)
	}

	// Write 1 more byte — should fail.
	_, err = fs.Put(ctx, "b", "k2", bytes.NewReader([]byte("x")))
	if err != ErrCacheFull {
		t.Errorf("Put over capacity: err = %v, want ErrCacheFull", err)
	}
}

func TestConcurrentPuts(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()
	const goroutines = 10

	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			data := bytes.Repeat([]byte{byte(idx)}, 100)
			_, errs[idx] = fs.Put(ctx, "bkt", "same-key", bytes.NewReader(data))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: Put error: %v", i, err)
		}
	}

	// After concurrent writes, the file should contain exactly one version.
	rc, info, err := fs.Get(ctx, "bkt", "same-key")
	if err != nil {
		t.Fatalf("Get after concurrent puts: %v", err)
	}
	rc.Close()

	if info.Size != 100 {
		t.Errorf("Size = %d, want 100", info.Size)
	}

	// Per-key lock guarantees accurate accounting after concurrent writes.
	if fs.UsedBytes() != 100 {
		t.Errorf("UsedBytes after concurrent puts = %d, want 100", fs.UsedBytes())
	}
}

func TestNestedKeys(t *testing.T) {
	fs := newTestCache(t)
	ctx := context.Background()

	data := []byte("nested-value")
	_, err := fs.Put(ctx, "bkt", "a/b/c/deep.txt", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put nested key: %v", err)
	}

	rc, _, err := fs.Get(ctx, "bkt", "a/b/c/deep.txt")
	if err != nil {
		t.Fatalf("Get nested key: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("nested key data = %q, want %q", got, data)
	}
}
