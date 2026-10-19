package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/adminsession"
	"github.com/uptrace/bun"
)

type BunAdminSessionRepo struct {
	db bun.IDB
}

var _ adminsession.Store = (*BunAdminSessionRepo)(nil)

func (r *BunAdminSessionRepo) Create(ctx context.Context, session *adminsession.Session) error {
	if session == nil || !session.Valid() {
		return adminsession.ErrInvalidSession
	}
	if _, err := r.db.NewInsert().Model(session).Exec(ctx); err != nil {
		return fmt.Errorf("creating admin session: %w", err)
	}
	return nil
}

func (r *BunAdminSessionRepo) GetActive(ctx context.Context, hash string, now time.Time) (*adminsession.Session, error) {
	session := new(adminsession.Session)
	err := r.db.NewSelect().Model(session).Where("session_id_hash = ?", hash).Where("expires_at > ?", now).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading admin session: %w", err)
	}
	return session, nil
}

func (r *BunAdminSessionRepo) ExtendActive(ctx context.Context, hash string, now, expiresAt time.Time) (bool, error) {
	if !expiresAt.After(now) {
		return false, adminsession.ErrInvalidSession
	}
	res, err := r.db.NewUpdate().Model((*adminsession.Session)(nil)).
		Set("expires_at = GREATEST(expires_at, ?)", expiresAt).
		Where("session_id_hash = ?", hash).Where("expires_at > ?", now).Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("extending admin session: %w", err)
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (r *BunAdminSessionRepo) Delete(ctx context.Context, hash string) error {
	if _, err := r.db.NewDelete().Model((*adminsession.Session)(nil)).Where("session_id_hash = ?", hash).Exec(ctx); err != nil {
		return fmt.Errorf("deleting admin session: %w", err)
	}
	return nil
}

func (r *BunAdminSessionRepo) DeleteExpired(ctx context.Context, now time.Time, limit int) (int64, error) {
	if limit < 1 {
		return 0, ErrInvalidInput
	}
	res, err := r.db.NewRaw(`WITH expired AS (
		SELECT session_id_hash FROM admin_sessions
		WHERE expires_at <= ? ORDER BY expires_at, session_id_hash
		LIMIT ? FOR NO KEY UPDATE SKIP LOCKED
	)
	DELETE FROM admin_sessions AS session USING expired
	WHERE session.session_id_hash = expired.session_id_hash AND session.expires_at <= ?`, now, limit, now).Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("deleting expired admin sessions: %w", err)
	}
	return res.RowsAffected()
}
