package adminsession

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"github.com/uptrace/bun"
)

var ErrInvalidSession = errors.New("invalid admin session")

type Session struct {
	bun.BaseModel `bun:"table:admin_sessions,alias:admin_session"`
	SessionIDHash string    `bun:"session_id_hash,pk,type:text"`
	CreatedAt     time.Time `bun:"created_at,notnull"`
	ExpiresAt     time.Time `bun:"expires_at,notnull"`
}

func (s Session) Valid() bool {
	decoded, err := base64.RawURLEncoding.DecodeString(s.SessionIDHash)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == s.SessionIDHash &&
		!s.CreatedAt.IsZero() && s.ExpiresAt.After(s.CreatedAt)
}

type Store interface {
	Create(context.Context, *Session) error
	GetActive(context.Context, string, time.Time) (*Session, error)
	ExtendActive(context.Context, string, time.Time, time.Time) (bool, error)
	Delete(context.Context, string) error
	DeleteExpired(context.Context, time.Time, int) (int64, error)
}
