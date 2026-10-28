package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ListUploadIDs lists multipart directories retained on disk.
func (f *Filesystem) ListUploadIDs(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(f.root, ".multipart"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() {
			ids = append(ids, entry.Name())
		}
	}
	return ids, nil
}

// WalkContentFiles visits content-addressed files in bucket cache directories.
func (f *Filesystem) WalkContentFiles(ctx context.Context, quarantined bool, visit func(string, int64) error) error {
	buckets, err := os.ReadDir(f.root)
	if err != nil {
		return err
	}
	name := ".contents"
	if quarantined {
		name = ".quarantine"
	}
	for _, bucket := range buckets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bucket.IsDir() || bucket.Name() == ".multipart" {
			continue
		}
		files, err := os.ReadDir(filepath.Join(f.root, bucket.Name(), name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			id, err := strconv.ParseInt(file.Name(), 10, 64)
			if err != nil || id < 1 || !file.Type().IsRegular() {
				continue
			}
			if err := visit(bucket.Name(), id); err != nil {
				return err
			}
		}
	}
	return nil
}

// Quarantine moves a content file out of the readable path while retaining its capacity charge.
func (f *Filesystem) Quarantine(ctx context.Context, bucket string, contentID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if contentID < 1 {
		return ErrInvalidPath
	}
	id := strconv.FormatInt(contentID, 10)
	key := filepath.Join(".contents", id)
	src, err := f.safePath(bucket, key)
	if err != nil {
		return err
	}
	dst, err := f.safePath(bucket, ".quarantine", id)
	if err != nil {
		return err
	}
	mu := f.shardFor(bucket, key)
	mu.Lock()
	defer mu.Unlock()
	if err := ensurePrivateDir(filepath.Dir(dst)); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(filepath.Dir(dst))); err != nil {
		return err
	}
	var replaced int64
	stat, err := os.Stat(dst)
	if err == nil {
		if !stat.Mode().IsRegular() {
			return fmt.Errorf("quarantine destination is not a regular file")
		}
		replaced = stat.Size()
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	f.capacityMu.Lock()
	f.usedBytes.Add(-replaced)
	f.capacityMu.Unlock()
	if err := syncDir(filepath.Dir(dst)); err != nil {
		return err
	}
	return syncDir(filepath.Dir(src))
}

// DeleteQuarantined deletes a retained file and releases its capacity charge.
func (f *Filesystem) DeleteQuarantined(ctx context.Context, bucket string, contentID int64) error {
	if contentID < 1 {
		return ErrInvalidPath
	}
	return f.Delete(ctx, bucket, filepath.Join(".quarantine", strconv.FormatInt(contentID, 10)))
}
