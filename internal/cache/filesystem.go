package cache

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Filesystem implements the Cache interface using the local filesystem.
type Filesystem struct {
	root      string
	usedBytes atomic.Int64
}

var _ Cache = (*Filesystem)(nil)

// NewFilesystem creates a filesystem-backed cache rooted at dir.
func NewFilesystem(dir string) (*Filesystem, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating cache root %s: %w", dir, err)
	}
	fs := &Filesystem{root: dir}
	// TODO: walk existing files to initialise usedBytes on startup.
	return fs, nil
}

func (f *Filesystem) objectPath(bucket, key string) string {
	return filepath.Join(f.root, bucket, key)
}

func (f *Filesystem) Put(ctx context.Context, bucket, key string, r io.Reader) (*ObjectInfo, error) {
	dir := filepath.Dir(f.objectPath(bucket, key))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	dst := f.objectPath(bucket, key)
	tmp := dst + ".tmp"

	file, err := os.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	defer func() {
		// Clean up temp file on error.
		if file != nil {
			file.Close()
			os.Remove(tmp)
		}
	}()

	md5Hash := md5.New()
	sha256Hash := sha256.New()
	w := io.MultiWriter(file, md5Hash, sha256Hash)

	n, err := io.Copy(w, r)
	if err != nil {
		return nil, fmt.Errorf("writing cache file: %w", err)
	}

	// fsync to guarantee durability before acking to caller.
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("fsync cache file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("closing cache file: %w", err)
	}
	file = nil // prevent defer cleanup

	if err := os.Rename(tmp, dst); err != nil {
		return nil, fmt.Errorf("renaming temp to final: %w", err)
	}

	f.usedBytes.Add(n)

	info := &ObjectInfo{
		Path:     dst,
		Size:     n,
		ETag:     hex.EncodeToString(md5Hash.Sum(nil)),
		Checksum: hex.EncodeToString(sha256Hash.Sum(nil)),
	}

	slog.Debug("cached object", "bucket", bucket, "key", key, "size", n)
	return info, nil
}

func (f *Filesystem) Get(_ context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error) {
	p := f.objectPath(bucket, key)
	file, err := os.Open(p)
	if err != nil {
		return nil, nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}

	info := &ObjectInfo{
		Path: p,
		Size: stat.Size(),
	}
	return file, info, nil
}

func (f *Filesystem) Delete(_ context.Context, bucket, key string) error {
	p := f.objectPath(bucket, key)
	stat, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := os.Remove(p); err != nil {
		return err
	}

	f.usedBytes.Add(-stat.Size())
	return nil
}

func (f *Filesystem) Exists(_ context.Context, bucket, key string) bool {
	_, err := os.Stat(f.objectPath(bucket, key))
	return err == nil
}

func (f *Filesystem) UsedBytes() int64 {
	return f.usedBytes.Load()
}

func (f *Filesystem) CreateBucketDir(_ context.Context, bucket string) error {
	return os.MkdirAll(filepath.Join(f.root, bucket), 0o755)
}

func (f *Filesystem) DeleteBucketDir(_ context.Context, bucket string) error {
	return os.RemoveAll(filepath.Join(f.root, bucket))
}
