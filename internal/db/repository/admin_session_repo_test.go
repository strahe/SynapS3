package repository_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/adminsession"
	"github.com/strahe/synaps3/internal/db/repository"
)

func adminSession(t *testing.T, repos *repository.Repositories, name string, now time.Time) *adminsession.Session {
	t.Helper()
	digest := sha256.Sum256([]byte(name))
	session := &adminsession.Session{SessionIDHash: base64.RawURLEncoding.EncodeToString(digest[:]), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := repos.AdminSessions.Create(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	return session
}

func TestAdminSessionRepoLifecycle(t *testing.T) {
	database := testDB(t)
	repos := repository.NewRepositories(database)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	session := adminSession(t, repos, "session", now)
	other := adminSession(t, repos, "other", now)
	key := session.SessionIDHash
	if active, err := repository.NewRepositories(database).AdminSessions.GetActive(ctx, key, now); err != nil || active == nil {
		t.Fatalf("session after reopening repositories=%v err=%v", active, err)
	}
	if ok, err := repos.AdminSessions.ExtendActive(ctx, key, now, now.Add(2*time.Hour)); err != nil || !ok {
		t.Fatalf("extend=%t err=%v", ok, err)
	}
	if ok, err := repos.AdminSessions.ExtendActive(ctx, key, now, now.Add(90*time.Minute)); err != nil || !ok {
		t.Fatalf("concurrent older extension=%t err=%v", ok, err)
	}
	active, err := repos.AdminSessions.GetActive(ctx, key, now)
	if err != nil || active == nil || !active.ExpiresAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("extension shortened expiry: session=%v err=%v", active, err)
	}
	if active, err := repos.AdminSessions.GetActive(ctx, key, now.Add(2*time.Hour)); err != nil || active != nil {
		t.Fatalf("expired session=%v err=%v", active, err)
	}
	if err := repos.AdminSessions.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if ok, err := repos.AdminSessions.ExtendActive(ctx, key, now, now.Add(3*time.Hour)); err != nil || ok {
		t.Fatalf("deleted session resurrected=%t err=%v", ok, err)
	}
	if active, err := repository.NewRepositories(database).AdminSessions.GetActive(ctx, key, now); err != nil || active != nil {
		t.Fatalf("deleted session after reopening=%v err=%v", active, err)
	}
	if active, err := repos.AdminSessions.GetActive(ctx, other.SessionIDHash, now); err != nil || active == nil {
		t.Fatalf("other session affected=%v err=%v", active, err)
	}
	if err := repos.AdminSessions.Delete(ctx, key); err != nil {
		t.Fatalf("repeated delete=%v", err)
	}
}

func TestAdminSessionRepoRefreshLogoutOrdering(t *testing.T) {
	for _, deleteFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "delete-first", false: "refresh-first"}[deleteFirst], func(t *testing.T) {
			database := testDB(t)
			repos := repository.NewRepositories(database)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			now := time.Now().UTC().Truncate(time.Second)
			session := adminSession(t, repos, "ordered", now)
			tx, err := database.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			transactional := repository.NewRepositories(tx)
			if deleteFirst {
				err = transactional.AdminSessions.Delete(ctx, session.SessionIDHash)
			} else {
				_, err = transactional.AdminSessions.ExtendActive(ctx, session.SessionIDHash, now, now.Add(2*time.Hour))
			}
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				if !deleteFirst {
					result <- repos.AdminSessions.Delete(ctx, session.SessionIDHash)
					return
				}
				active, err := repos.AdminSessions.ExtendActive(ctx, session.SessionIDHash, now, now.Add(3*time.Hour))
				if active {
					err = adminsession.ErrInvalidSession
				}
				result <- err
			}()
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if active, err := repos.AdminSessions.GetActive(ctx, session.SessionIDHash, now); err != nil || active != nil {
				t.Fatalf("session after logout=%v err=%v", active, err)
			}
		})
	}
}

func TestAdminSessionRepoCleanupPreservesRenewal(t *testing.T) {
	database := testDB(t)
	repos := repository.NewRepositories(database)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	session := adminSession(t, repos, "renewing", now)
	adminSession(t, repos, "expired-one", now.Add(-2*time.Hour))
	adminSession(t, repos, "expired-two", now.Add(-2*time.Hour))
	if deleted, err := repos.AdminSessions.DeleteExpired(ctx, now, 1); err != nil || deleted != 1 {
		t.Fatalf("bounded cleanup=%d err=%v", deleted, err)
	}
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if active, err := repository.NewRepositories(tx).AdminSessions.ExtendActive(ctx, session.SessionIDHash, now, now.Add(3*time.Hour)); err != nil || !active {
		t.Fatalf("renewal=%t err=%v", active, err)
	}
	if deleted, err := repos.AdminSessions.DeleteExpired(ctx, now.Add(2*time.Hour), 128); err != nil || deleted != 1 {
		t.Fatalf("cleanup while renewal holds row=%d err=%v", deleted, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if deleted, err := repos.AdminSessions.DeleteExpired(ctx, now.Add(2*time.Hour), 128); err != nil || deleted != 0 {
		t.Fatalf("cleanup after renewal=%d err=%v", deleted, err)
	}
	if active, err := repos.AdminSessions.GetActive(ctx, session.SessionIDHash, now.Add(2*time.Hour)); err != nil || active == nil {
		t.Fatalf("renewed session deleted=%v err=%v", active, err)
	}
}
