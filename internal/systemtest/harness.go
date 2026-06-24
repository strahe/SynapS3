//go:build systemtest

package systemtest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/app"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
	"golang.org/x/crypto/bcrypt"
)

const (
	AdminUsername = "admin"
	AdminPassword = "system-test-admin-password"
	OwnerAccess   = "SYSTEMTESTOWNER"
	OwnerSecret   = "system-test-owner-secret"

	offlinePrivateKeyPlaceholder = "system-test-private-key"
)

// Harness owns one complete application runtime and its external resources.
type Harness struct {
	AdminURL string
	s3Socket string

	runtime *app.Runtime
	db      *bun.DB
	tempDir string
	cancel  context.CancelFunc
	done    chan struct{}
	runErr  error

	cleanupOnce sync.Once
	cleanupErr  error
}

// NewHarness creates and starts an isolated production runtime.
func NewHarness(ctx context.Context, logger *slog.Logger) (_ *Harness, err error) {
	return newHarness(ctx, logger, "")
}

func newHarness(ctx context.Context, logger *slog.Logger, s3Address string) (_ *Harness, err error) {
	if logger == nil {
		return nil, errors.New("systemtest logger is required")
	}
	tempDir, err := os.MkdirTemp("", "synaps3-system-")
	if err != nil {
		return nil, fmt.Errorf("creating systemtest directory: %w", err)
	}
	cleanupDir := true
	defer func() {
		if cleanupDir {
			_ = os.RemoveAll(tempDir)
		}
	}()

	socketPath := s3Address
	if socketPath == "" {
		socketPath, err = shortSocketPath()
		if err != nil {
			return nil, err
		}
	}
	cleanupSocket := true
	defer func() {
		if cleanupSocket {
			_ = os.Remove(socketPath)
		}
	}()

	cfg, err := config.DefaultConfig()
	if err != nil {
		return nil, fmt.Errorf("creating systemtest config: %w", err)
	}
	cfg.Server.Port = "127.0.0.1:18080"
	cfg.Server.MaxConnections = 64
	cfg.Server.MaxRequests = 32
	cfg.Admin.Addr = "127.0.0.1:0"
	cfg.Cache.Dir = filepath.Join(tempDir, "cache")
	cfg.Cache.MaxSizeGB = 1
	cfg.Cache.EvictionPolicy = "lru"
	cfg.Database = config.DatabaseConfig{
		Driver: "sqlite",
		DSN: "file:" + filepath.ToSlash(filepath.Join(tempDir, "system.db")) +
			"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
	cfg.Filecoin.DefaultCopies = config.DefaultFilecoinCopies
	// MemoryFilecoin is injected directly; this only satisfies production settings validation.
	cfg.Filecoin.PrivateKey = offlinePrivateKeyPlaceholder
	cfg.Filecoin.Observability.Interval = 40 * time.Millisecond
	cfg.Filecoin.Observability.Timeout = time.Second
	cfg.Filecoin.Observability.Concurrency = 3
	cfg.Worker.Upload = config.WorkerPoolConfig{Concurrency: 1, PollInterval: 15 * time.Millisecond, MaxRetries: 3}
	cfg.Worker.Evictor = config.WorkerPoolConfig{Concurrency: 1, PollInterval: 15 * time.Millisecond, MaxRetries: 3}
	cfg.Worker.StorageCleanup = config.WorkerPoolConfig{Concurrency: 1, PollInterval: 25 * time.Millisecond, MaxRetries: 3}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(AdminPassword), bcrypt.MinCost)
	if err != nil {
		return nil, fmt.Errorf("hashing systemtest admin password: %w", err)
	}
	cfg.Admin.Auth = config.AdminAuthConfig{
		Enabled: true, Username: AdminUsername, PasswordHash: string(passwordHash),
		SessionSecret: "system-test-session-secret-with-enough-entropy", SessionTTL: time.Hour,
	}

	database, err := db.New(cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("opening systemtest database: %w", err)
	}
	cleanupDB := true
	defer func() {
		if cleanupDB {
			_ = database.Close()
		}
	}()
	if err := db.Ping(ctx, database); err != nil {
		return nil, fmt.Errorf("pinging systemtest database: %w", err)
	}
	if err := db.RunMigrations(ctx, database); err != nil {
		return nil, fmt.Errorf("migrating systemtest database: %w", err)
	}

	iam := s3iam.NewService(repository.NewRepositories(database))
	if err := iam.CreateAccount(auth.Account{Access: OwnerAccess, Secret: OwnerSecret, Role: auth.RoleUserPlus}); err != nil {
		return nil, fmt.Errorf("creating systemtest S3 owner: %w", err)
	}
	configPath := filepath.Join(tempDir, "config.toml")
	if err := os.WriteFile(configPath, nil, 0o600); err != nil {
		return nil, fmt.Errorf("creating systemtest settings file: %w", err)
	}
	settings, err := admin.NewSettingsService(cfg, config.Source{Path: configPath, Explicit: true, Exists: true})
	if err != nil {
		return nil, fmt.Errorf("creating systemtest settings: %w", err)
	}

	filecoin := NewMemoryFilecoin()
	runtime, err := app.NewRuntime(ctx, app.RuntimeOptions{
		Config: cfg, Database: database, Settings: settings, Logger: logger,
		Filecoin: app.FilecoinServices{
			Storage: filecoin, WalletQuery: filecoin, Wallet: filecoin, Receipts: filecoin,
			Readiness: filecoin, Observability: filecoin,
		},
		S3Addresses: []string{socketPath}, ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("creating systemtest runtime: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	harness := &Harness{
		AdminURL: "http://" + runtime.AdminAddr(), s3Socket: socketPath,
		runtime: runtime, db: database, tempDir: tempDir, cancel: cancel, done: make(chan struct{}),
	}
	go func() {
		harness.runErr = runtime.Run(runCtx)
		close(harness.done)
	}()
	if err := harness.waitReady(ctx, 5*time.Second); err != nil {
		cancel()
		select {
		case <-harness.done:
			err = errors.Join(err, harness.runErr)
		case <-time.After(5 * time.Second):
			err = errors.Join(err, errors.New("systemtest runtime did not stop after startup failure"))
		}
		return nil, err
	}

	cleanupDB = false
	cleanupDir = false
	cleanupSocket = false
	return harness, nil
}

func shortSocketPath() (string, error) {
	base := "/tmp"
	if info, err := os.Stat(base); err != nil || !info.IsDir() {
		base = os.TempDir()
	}
	file, err := os.CreateTemp(base, "s3st-")
	if err != nil {
		return "", fmt.Errorf("reserving systemtest socket path: %w", err)
	}
	path := file.Name()
	if closeErr := file.Close(); closeErr != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("closing socket reservation: %w", closeErr)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("releasing socket reservation: %w", err)
	}
	return path + ".sock", nil
}

func (h *Harness) waitReady(ctx context.Context, timeout time.Duration) error {
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		adminConn, adminErr := net.DialTimeout("tcp", h.runtime.AdminAddr(), 100*time.Millisecond)
		if adminErr == nil {
			_ = adminConn.Close()
		}
		_, socketErr := os.Stat(h.s3Socket)
		if adminErr == nil && socketErr == nil {
			return nil
		}
		select {
		case <-h.done:
			return fmt.Errorf("systemtest runtime stopped during startup: %w", h.runErr)
		case <-readyCtx.Done():
			return fmt.Errorf("waiting for systemtest listeners: admin=%v s3=%v: %w", adminErr, socketErr, readyCtx.Err())
		case <-ticker.C:
		}
	}
}

// Done is closed when the application runtime stops.
func (h *Harness) Done() <-chan struct{} { return h.done }

// Err returns the runtime result after Done is closed.
func (h *Harness) Err() error { return h.runErr }

// S3SocketPath returns the runtime's Unix socket path for black-box S3 clients.
func (h *Harness) S3SocketPath() string { return h.s3Socket }

// S3Client returns a SigV4 client connected to the runtime's Unix socket.
func (h *Harness) S3Client(accessKey, secretKey string) *awss3.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", h.s3Socket)
	}
	awsConfig := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		HTTPClient:  &http.Client{Transport: transport},
	}
	return awss3.NewFromConfig(awsConfig, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String("http://synaps3.system.test")
		options.UsePathStyle = true
	})
}

// Close stops the runtime, closes its database, and removes temporary state.
func (h *Harness) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.cancel()
	select {
	case <-h.done:
	case <-ctx.Done():
		return fmt.Errorf("waiting for systemtest runtime shutdown: %w", ctx.Err())
	}
	h.cleanupOnce.Do(func() {
		socketErr := os.Remove(h.s3Socket)
		if errors.Is(socketErr, os.ErrNotExist) {
			socketErr = nil
		}
		h.cleanupErr = errors.Join(h.db.Close(), socketErr, os.RemoveAll(h.tempDir))
	})
	return errors.Join(h.runErr, h.cleanupErr)
}
