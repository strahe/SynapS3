package cache

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

const privateDirMode os.FileMode = 0o700

// errLimitReader wraps an io.Reader and returns ErrCacheFull when the
// read limit is exceeded. Used to bound temp file writes during Put.
type errLimitReader struct {
	r         io.Reader
	remaining int64
}

func (l *errLimitReader) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		// Probe: does the underlying reader have more data beyond the limit?
		var probe [1]byte
		if n, _ := l.r.Read(probe[:]); n > 0 {
			return 0, ErrCacheFull // genuinely over limit
		}
		return 0, io.EOF // exact fit — data exhausted at limit
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.r.Read(p)
	l.remaining -= int64(n)
	return n, err
}

// Filesystem implements the Cache interface using the local filesystem.
type Filesystem struct {
	root      string
	maxBytes  int64 // 0 means unlimited
	usedBytes atomic.Int64
	keyShards [256]sync.Mutex // fixed-size shard locks to serialize per-key operations
}

var _ Cache = (*Filesystem)(nil)

// shardFor returns the shard mutex for a given bucket/key pair.
func (f *Filesystem) shardFor(bucket, key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(bucket + "/" + key))
	return &f.keyShards[h.Sum32()%256]
}

// NewFilesystem creates a filesystem-backed cache rooted at dir.
// maxBytes sets the capacity limit (0 = unlimited).
// On startup it walks existing files to initialise usedBytes and removes stale temp files.
func NewFilesystem(dir string, maxBytes int64) (*Filesystem, error) {
	absRoot, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolving cache root %s: %w", dir, err)
	}
	if err := ensurePrivateDir(absRoot); err != nil {
		return nil, fmt.Errorf("creating cache root %s: %w", absRoot, err)
	}

	f := &Filesystem{root: absRoot, maxBytes: maxBytes}

	var totalBytes int64
	var fileCount int
	var tmpRemoved int

	err = filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		// Remove stale temp files from interrupted writes.
		if strings.HasPrefix(d.Name(), ".synaps3-") && strings.HasSuffix(d.Name(), ".tmp") {
			if rmErr := os.Remove(path); rmErr == nil {
				tmpRemoved++
			}
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil // skip unreadable files
		}
		totalBytes += info.Size()
		fileCount++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking cache dir: %w", err)
	}

	f.usedBytes.Store(totalBytes)
	slog.Info("cache initialized", "root", absRoot, "files", fileCount, "bytes", totalBytes, "stale_tmp_removed", tmpRemoved)

	return f, nil
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, privateDirMode); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	if info.Mode().Perm() != privateDirMode {
		return os.Chmod(path, privateDirMode)
	}
	return nil
}

// RootDir returns the absolute path of the cache root directory.
func (f *Filesystem) RootDir() string {
	return f.root
}

// safePath validates and returns the absolute path for bucket/key.
// Returns ErrInvalidPath if the result escapes the cache root.
func (f *Filesystem) safePath(parts ...string) (string, error) {
	joined := filepath.Join(append([]string{f.root}, parts...)...)
	abs, err := filepath.Abs(joined)
	if err != nil {
		return "", ErrInvalidPath
	}
	// Ensure the resolved path is under root (with trailing separator to prevent prefix-matching attacks).
	rootPrefix := f.root + string(filepath.Separator)
	if !strings.HasPrefix(abs, rootPrefix) {
		return "", ErrInvalidPath
	}
	if len(parts) > 0 {
		namespace := filepath.Clean(parts[0])
		if namespace != parts[0] ||
			namespace == "." ||
			namespace == ".." ||
			filepath.IsAbs(namespace) ||
			strings.ContainsAny(namespace, `/\`) {
			return "", ErrInvalidPath
		}
		base, err := filepath.Abs(filepath.Join(f.root, namespace))
		if err != nil {
			return "", ErrInvalidPath
		}
		basePrefix := base + string(filepath.Separator)
		if abs != base && !strings.HasPrefix(abs, basePrefix) {
			return "", ErrInvalidPath
		}
	}
	return abs, nil
}

// fsyncDir fsyncs a directory to ensure rename/unlink durability.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func (f *Filesystem) Put(ctx context.Context, bucket, key string, r io.Reader) (*ObjectInfo, error) {
	staged, err := f.PutStaged(ctx, bucket, key, r)
	if err != nil {
		return nil, err
	}
	if err := staged.Commit(); err != nil {
		_ = staged.Rollback()
		return nil, err
	}
	return staged.Info, nil
}

func (f *Filesystem) PutStaged(ctx context.Context, bucket, key string, r io.Reader) (*StagedObject, error) {
	dst, err := f.safePath(bucket, key)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(dst)
	if err := ensurePrivateDir(dir); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	// Approximate old file size for capacity estimation (no shard lock).
	// This is racy but safe: Commit does the authoritative capacity check.
	var oldSizeApprox int64
	if stat, statErr := os.Stat(dst); statErr == nil {
		oldSizeApprox = stat.Size()
	}

	// Pre-flight capacity check (without shard lock — approximate).
	if f.maxBytes > 0 {
		avail := f.maxBytes - f.usedBytes.Load() + oldSizeApprox
		if avail <= 0 {
			return nil, ErrCacheFull
		}
	}

	// Use os.CreateTemp for unique temp files.
	file, err := os.CreateTemp(dir, ".synaps3-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := file.Name()
	defer func() {
		if file != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	md5Hash := md5.New()
	sha256Hash := sha256.New()
	w := io.MultiWriter(file, md5Hash, sha256Hash)

	// Bound the write to prevent disk exhaustion.
	src := r
	if f.maxBytes > 0 {
		avail := f.maxBytes - f.usedBytes.Load() + oldSizeApprox
		if avail <= 0 {
			return nil, ErrCacheFull
		}
		src = &errLimitReader{r: r, remaining: avail}
	}

	n, err := io.Copy(w, src)
	if err != nil {
		if errors.Is(err, ErrCacheFull) {
			return nil, ErrCacheFull
		}
		return nil, fmt.Errorf("writing cache file: %w", err)
	}

	// fsync to guarantee durability before returning.
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("fsync cache file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("closing cache file: %w", err)
	}
	file = nil // prevent defer cleanup

	info := &ObjectInfo{
		Path:     dst,
		Size:     n,
		ETag:     hex.EncodeToString(md5Hash.Sum(nil)),
		Checksum: hex.EncodeToString(sha256Hash.Sum(nil)),
	}

	committed := false
	return &StagedObject{
		Info: info,
		commit: func() error {
			if committed {
				return nil
			}

			// Acquire shard lock for atomic rename and accounting.
			mu := f.shardFor(bucket, key)
			mu.Lock()
			defer mu.Unlock()

			// Determine old file size for accounting.
			var oldSize int64
			if stat, statErr := os.Stat(dst); statErr == nil {
				oldSize = stat.Size()
			}

			// Reserve capacity.
			delta := n - oldSize
			var reserved bool
			if f.maxBytes > 0 && delta > 0 {
				newUsed := f.usedBytes.Add(delta)
				if newUsed > f.maxBytes {
					f.usedBytes.Add(-delta)
					return ErrCacheFull
				}
				reserved = true
			}

			if err := os.Rename(tmpPath, dst); err != nil {
				if reserved {
					f.usedBytes.Add(-delta)
				}
				return fmt.Errorf("renaming temp to final: %w", err)
			}

			// Fsync parent directory to ensure the rename is durable.
			if err := fsyncDir(dir); err != nil {
				slog.Warn("fsync parent dir failed", "dir", dir, "error", err)
			}

			// Apply remaining accounting (shrink or unlimited mode).
			if !reserved {
				f.usedBytes.Add(delta)
			}

			committed = true
			slog.Debug("cached object", "bucket", bucket, "key", key, "size", n)
			return nil
		},
		rollback: func() error {
			if committed {
				return nil
			}
			return os.Remove(tmpPath)
		},
	}, nil
}

func (f *Filesystem) Get(_ context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error) {
	p, err := f.safePath(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.Open(p)
	if err != nil {
		return nil, nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	info := &ObjectInfo{
		Path: p,
		Size: stat.Size(),
	}
	return file, info, nil
}

func (f *Filesystem) Delete(_ context.Context, bucket, key string) error {
	p, err := f.safePath(bucket, key)
	if err != nil {
		return err
	}

	// Serialize with Put to prevent usedBytes drift.
	mu := f.shardFor(bucket, key)
	mu.Lock()
	defer mu.Unlock()

	stat, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	f.usedBytes.Add(-stat.Size())

	// Fsync parent directory to ensure the unlink is durable.
	if dir := filepath.Dir(p); dir != "" {
		if syncErr := fsyncDir(dir); syncErr != nil {
			slog.Warn("fsync parent dir after delete failed", "dir", dir, "error", syncErr)
		}
	}

	return nil
}

func (f *Filesystem) Exists(_ context.Context, bucket, key string) bool {
	p, err := f.safePath(bucket, key)
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

func (f *Filesystem) UsedBytes() int64 {
	return f.usedBytes.Load()
}

func (f *Filesystem) CreateBucketDir(_ context.Context, bucket string) error {
	p, err := f.safePath(bucket)
	if err != nil {
		return err
	}
	return ensurePrivateDir(p)
}

func (f *Filesystem) DeleteBucketDir(_ context.Context, bucket string) error {
	p, err := f.safePath(bucket)
	if err != nil {
		return err
	}

	// Lock all shards to prevent concurrent Put/Delete from causing usedBytes drift.
	// Bucket deletion is rare, so the brief global lock is acceptable.
	for i := range f.keyShards {
		f.keyShards[i].Lock()
	}
	defer func() {
		for i := range f.keyShards {
			f.keyShards[i].Unlock()
		}
	}()

	// Walk to compute total size of files being removed for accounting.
	// Skip temp files — they were never counted in usedBytes.
	var totalSize int64
	_ = filepath.WalkDir(p, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".synaps3-") && strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		if info, infoErr := d.Info(); infoErr == nil {
			totalSize += info.Size()
		}
		return nil
	})

	if err := os.RemoveAll(p); err != nil {
		return err
	}

	if totalSize > 0 {
		f.usedBytes.Add(-totalSize)
	}
	return nil
}

// multipartDir returns the path to .multipart/<uploadID>.
func (f *Filesystem) multipartDir(uploadID string) (string, error) {
	return f.safePath(".multipart", uploadID)
}

// partPath returns the path to .multipart/<uploadID>/<partNumber>.
func (f *Filesystem) partPath(uploadID string, partNumber int) (string, error) {
	return f.safePath(".multipart", uploadID, fmt.Sprintf("%d", partNumber))
}

func (f *Filesystem) PutPart(_ context.Context, uploadID string, partNumber int, r io.Reader) (*ObjectInfo, error) {
	dst, err := f.partPath(uploadID, partNumber)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(dst)
	if err := ensurePrivateDir(dir); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	// Use shard lock keyed on uploadID+partNumber for per-part serialization.
	mu := f.shardFor(".multipart/"+uploadID, fmt.Sprintf("%d", partNumber))
	mu.Lock()
	defer mu.Unlock()

	var oldSize int64
	if stat, statErr := os.Stat(dst); statErr == nil {
		oldSize = stat.Size()
	}

	file, err := os.CreateTemp(dir, ".synaps3-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := file.Name()
	defer func() {
		if file != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	md5Hash := md5.New()
	sha256Hash := sha256.New()
	w := io.MultiWriter(file, md5Hash, sha256Hash)

	src := r
	if f.maxBytes > 0 {
		avail := f.maxBytes - f.usedBytes.Load() + oldSize
		if avail <= 0 {
			return nil, ErrCacheFull
		}
		src = &errLimitReader{r: r, remaining: avail}
	}

	n, err := io.Copy(w, src)
	if err != nil {
		if errors.Is(err, ErrCacheFull) {
			return nil, ErrCacheFull
		}
		return nil, fmt.Errorf("writing part file: %w", err)
	}

	delta := n - oldSize
	var reserved bool
	if f.maxBytes > 0 && delta > 0 {
		newUsed := f.usedBytes.Add(delta)
		if newUsed > f.maxBytes {
			f.usedBytes.Add(-delta)
			return nil, ErrCacheFull
		}
		reserved = true
	}

	if err := file.Sync(); err != nil {
		if reserved {
			f.usedBytes.Add(-delta)
		}
		return nil, fmt.Errorf("fsync part file: %w", err)
	}
	if err := file.Close(); err != nil {
		if reserved {
			f.usedBytes.Add(-delta)
		}
		return nil, fmt.Errorf("closing part file: %w", err)
	}
	file = nil

	if err := os.Rename(tmpPath, dst); err != nil {
		if reserved {
			f.usedBytes.Add(-delta)
		}
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("renaming part to final: %w", err)
	}

	if err := fsyncDir(dir); err != nil {
		slog.Warn("fsync multipart dir failed", "dir", dir, "error", err)
	}

	if !reserved {
		f.usedBytes.Add(delta)
	}

	return &ObjectInfo{
		Path:     dst,
		Size:     n,
		ETag:     hex.EncodeToString(md5Hash.Sum(nil)),
		Checksum: hex.EncodeToString(sha256Hash.Sum(nil)),
	}, nil
}

func (f *Filesystem) AssembleParts(_ context.Context, bucket, key, uploadID string, partNumbers []int) (*ObjectInfo, []string, error) {
	dst, err := f.safePath(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	dir := filepath.Dir(dst)
	if err := ensurePrivateDir(dir); err != nil {
		return nil, nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	mu := f.shardFor(bucket, key)
	mu.Lock()
	defer mu.Unlock()

	var oldSize int64
	if stat, statErr := os.Stat(dst); statErr == nil {
		oldSize = stat.Size()
	}

	file, err := os.CreateTemp(dir, ".synaps3-*.tmp")
	if err != nil {
		return nil, nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := file.Name()
	defer func() {
		if file != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	sha256Hash := sha256.New()
	w := io.MultiWriter(file, sha256Hash)

	var totalSize int64
	partETags := make([]string, 0, len(partNumbers))

	for _, pn := range partNumbers {
		partFile, pErr := f.partPath(uploadID, pn)
		if pErr != nil {
			return nil, nil, fmt.Errorf("part path %d: %w", pn, pErr)
		}
		pf, openErr := os.Open(partFile)
		if openErr != nil {
			return nil, nil, fmt.Errorf("opening part %d: %w", pn, openErr)
		}

		partMD5 := md5.New()
		pr := io.TeeReader(pf, partMD5)

		n, copyErr := io.Copy(w, pr)
		_ = pf.Close()
		if copyErr != nil {
			return nil, nil, fmt.Errorf("copying part %d: %w", pn, copyErr)
		}
		totalSize += n
		partETags = append(partETags, hex.EncodeToString(partMD5.Sum(nil)))
	}

	delta := totalSize - oldSize
	var reserved bool
	if f.maxBytes > 0 && delta > 0 {
		newUsed := f.usedBytes.Add(delta)
		if newUsed > f.maxBytes {
			f.usedBytes.Add(-delta)
			return nil, nil, ErrCacheFull
		}
		reserved = true
	}

	if err := file.Sync(); err != nil {
		if reserved {
			f.usedBytes.Add(-delta)
		}
		return nil, nil, fmt.Errorf("fsync assembled file: %w", err)
	}
	if err := file.Close(); err != nil {
		if reserved {
			f.usedBytes.Add(-delta)
		}
		return nil, nil, fmt.Errorf("closing assembled file: %w", err)
	}
	file = nil

	if err := os.Rename(tmpPath, dst); err != nil {
		if reserved {
			f.usedBytes.Add(-delta)
		}
		_ = os.Remove(tmpPath)
		return nil, nil, fmt.Errorf("renaming assembled to final: %w", err)
	}

	if err := fsyncDir(dir); err != nil {
		slog.Warn("fsync dir after assemble failed", "dir", dir, "error", err)
	}

	if !reserved {
		f.usedBytes.Add(delta)
	}

	info := &ObjectInfo{
		Path:     dst,
		Size:     totalSize,
		Checksum: hex.EncodeToString(sha256Hash.Sum(nil)),
	}
	return info, partETags, nil
}

func (f *Filesystem) DeleteUpload(_ context.Context, uploadID string) error {
	mpDir, err := f.multipartDir(uploadID)
	if err != nil {
		return err
	}

	// Walk without global lock — sum sizes first, then remove and adjust atomically.
	var totalSize int64
	_ = filepath.WalkDir(mpDir, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".synaps3-") && strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		if info, infoErr := d.Info(); infoErr == nil {
			totalSize += info.Size()
		}
		return nil
	})

	if err := os.RemoveAll(mpDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing upload dir %s: %w", mpDir, err)
	}

	if totalSize > 0 {
		f.usedBytes.Add(-totalSize)
	}
	return nil
}
