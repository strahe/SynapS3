package testpg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestTemplatePreparationFailureReclaimsDatabase(t *testing.T) {
	admin := openTestDB(t, DSN(t))
	want := errors.New("injected template preparation failure")
	var name string
	template, err := NewTemplate(t.Context(), func(_ context.Context, config *pgx.ConnConfig) error {
		name = config.Database
		return want
	})
	if template != nil || !errors.Is(err, want) {
		t.Fatalf("template preparation = %v, %v; want original failure", template, err)
	}
	var exists bool
	if err := admin.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists); err != nil || exists {
		t.Fatalf("failed template remains: exists=%v, err=%v", exists, err)
	}
}

func TestCloneCancellationWhileWaitingReclaimsResources(t *testing.T) {
	admin := openTestDB(t, DSN(t))
	template, err := NewTemplate(t.Context(), func(context.Context, *pgx.ConnConfig) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	owner, err := resourceOwner()
	if err != nil {
		t.Fatal(err)
	}
	prefix := "s3t_" + owner + "_c_"
	countClones := func() (int, int) {
		t.Helper()
		var databases int
		if err := admin.QueryRowContext(t.Context(), "SELECT count(*) FROM pg_database WHERE left(datname::text, length($1)) = $1", prefix).Scan(&databases); err != nil {
			t.Fatal(err)
		}
		resourceRegistry.Lock()
		defer resourceRegistry.Unlock()
		var registered int
		for name := range resourceRegistry.cleanup {
			if strings.HasPrefix(name, prefix) {
				registered++
			}
		}
		return databases, registered
	}
	beforeDatabases, beforeRegistered := countClones()
	for range cap(template.cloneGate) {
		template.cloneGate <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	config, drop, err := template.Clone(ctx)
	for range cap(template.cloneGate) {
		<-template.cloneGate
	}
	if !errors.Is(err, context.DeadlineExceeded) || config != nil || drop != nil {
		t.Fatalf("clone waiting for capacity: config=%v, cleanup=%v, err=%v; want deadline cancellation", config != nil, drop != nil, err)
	}
	if databases, registered := countClones(); databases != beforeDatabases || registered != beforeRegistered {
		t.Fatalf("cancelled clone remains: databases=%d, registered=%d; want %d, %d", databases, registered, beforeDatabases, beforeRegistered)
	}
	config, drop, err = template.Clone(t.Context())
	if err != nil {
		t.Fatalf("clone template after cancelled wait: %v", err)
	}
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := drop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("fresh clone connection: %v", err)
	}
}

func TestCloneCleanupReportsConnectionLeakWithoutDamagingTemplate(t *testing.T) {
	admin := openTestDB(t, DSN(t))
	template, err := NewTemplate(t.Context(), func(context.Context, *pgx.ConnConfig) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	config, drop, err := template.Clone(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := drop(ctx); err != nil {
			t.Error(err)
		}
	})
	connection, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := drop(t.Context()); err == nil || !strings.Contains(err.Error(), "unclosed connections") {
		t.Fatalf("clone cleanup = %v, want connection leak", err)
	}
	var exists bool
	if err := admin.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", config.Database).Scan(&exists); err != nil || exists {
		t.Fatalf("leaking clone remains: exists=%v, err=%v", exists, err)
	}
	_ = connection.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := drop(t.Context()); err != nil {
		t.Fatalf("repeat cleanup: %v", err)
	}
	freshConfig, freshDrop, err := template.Clone(t.Context())
	if err != nil {
		t.Fatalf("clone template after forced cleanup: %v", err)
	}
	fresh := stdlib.OpenDB(*freshConfig)
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := freshDrop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := fresh.PingContext(t.Context()); err != nil {
		t.Fatalf("fresh clone connection: %v", err)
	}
}

func TestCleanupRunPreservesOtherNamespacesAndMalformedNames(t *testing.T) {
	dsn := DSN(t)
	admin := openTestDB(t, dsn)
	token, err := NewRunToken()
	if err != nil {
		t.Fatal(err)
	}
	otherToken, err := NewRunToken()
	if err != nil {
		t.Fatal(err)
	}
	owned := "s3t_" + token + "_01234567_s_0123456789ab"
	other := "s3t_" + otherToken + "_01234567_s_0123456789ab"
	malformed := "s3t_" + token + "_invalid_s_0123456789ab"
	for _, name := range []string{owned, other, malformed} {
		if _, err := admin.ExecContext(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
				t.Error(err)
			}
		})
	}
	ownedDatabase := strings.Replace(owned, "_s_", "_c_", 1)
	otherDatabase := strings.Replace(other, "_s_", "_c_", 1)
	for _, name := range []string{ownedDatabase, otherDatabase} {
		if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0 STRATEGY WAL_LOG"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
				t.Error(err)
			}
		})
	}
	if err := CleanupRun(t.Context(), dsn, token); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		want bool
	}{{owned, false}, {other, true}, {malformed, true}} {
		var exists bool
		if err := admin.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", test.name).Scan(&exists); err != nil || exists != test.want {
			t.Errorf("schema %q remains=%v, want %v: %v", test.name, exists, test.want, err)
		}
	}
	for _, test := range []struct {
		name string
		want bool
	}{{ownedDatabase, false}, {otherDatabase, true}} {
		var exists bool
		if err := admin.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", test.name).Scan(&exists); err != nil || exists != test.want {
			t.Errorf("database %q remains=%v, want %v: %v", test.name, exists, test.want, err)
		}
	}
}
