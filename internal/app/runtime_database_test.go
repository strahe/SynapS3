package app

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	wallettask "github.com/strahe/synaps3/internal/task/wallet"
	"github.com/strahe/synaps3/internal/testpg"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestMain(m *testing.M) { testpg.Main(m) }

func TestRuntimeRefusesMarkedBaselineWithoutDeploymentIdentity(t *testing.T) {
	database := testutil.NewTestDB(t)
	var markers []string
	if err := database.NewRaw("SELECT name FROM bun_migrations ORDER BY id").Scan(t.Context(), &markers); err != nil {
		t.Fatal(err)
	}
	if len(markers) == 0 || markers[0] != migrations.InitialSchemaName {
		t.Fatalf("baseline markers = %v", markers)
	}
	if _, err := database.ExecContext(t.Context(), "DROP TABLE deployment_identity"); err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateTarget(t.Context(), database); err != nil {
		t.Fatalf("validating recorded migration prefix: %v", err)
	}
	cfg, err := config.DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Filecoin.Network = "calibration"
	cfg.Filecoin.PrivateKey = "runtime-test-private-key"
	cfg.Database.DSN = "postgres://runtime-test@localhost/runtime-test"
	cfg.Admin.Auth.PasswordHash = "$2a$10$7EqJtq98hPqEX7fNZaFWoOhi6r4aIvJrDWHtqK4V0GaQYe7TzTx6W"
	cfg.Admin.Auth.SessionSecret = "runtime-test-session-secret-with-enough-entropy"
	cfg.Cache.Dir = filepath.Join(t.TempDir(), "cache")
	services := FilecoinServices{
		ChainID:     uint64(testutil.DefaultContextIdentity.ChainID),
		Storage:     &testutil.MockStorageClient{},
		Wallet:      identityWallet{identity: testutil.DefaultWalletExecutionIdentity()},
		WalletQuery: identityWalletQuery{identity: testutil.DefaultWalletExecutionIdentity()},
		Receipts: struct {
			wallettask.WalletReceiptChecker
		}{},
		Readiness:     struct{ ReadinessProbe }{},
		Observability: struct{ observability.RefreshChecker }{},
		Terminator:    &testutil.MockServiceTerminator{},
		Epochs:        &testutil.MockChainEpochReader{},
		CommitNonces:  struct{ synapse.CommitNonceReader }{},
		RemovalChain:  struct{ synapse.RemovalChainReader }{},
	}
	runtime, err := NewRuntime(t.Context(), RuntimeOptions{
		Config: cfg, Database: database, Settings: &admin.SettingsService{}, Filecoin: services,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	var pgError *pgconn.PgError
	if runtime != nil || err == nil || !strings.Contains(err.Error(), "validating deployment identity") ||
		!errors.As(err, &pgError) || pgError.Code != "42P01" {
		t.Fatalf("runtime = %v, error = %v, want deployment schema refusal", runtime, err)
	}
	if _, err := os.Stat(cfg.Cache.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache after refused startup: %v", err)
	}
	var after []string
	if err := database.NewRaw("SELECT name FROM bun_migrations ORDER BY id").Scan(t.Context(), &after); err != nil || !slices.Equal(markers, after) {
		t.Fatalf("migration markers after refusal = %v, %v", after, err)
	}
}
