package model

import (
	"context"
	"time"

	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
)

// S3Account stores S3 credentials and roles used by the VersityGW IAM adapter.
type S3Account struct {
	bun.BaseModel `bun:"table:s3_accounts"`

	AccessKey string    `bun:"type:text,pk"`
	SecretKey string    `bun:"type:text,notnull"`
	Role      auth.Role `bun:"type:text,notnull"`
	IsRoot    bool      `bun:",notnull,default:false"`
	CreatedAt time.Time `bun:",nullzero,notnull"`
	UpdatedAt time.Time `bun:",nullzero,notnull"`
	Name      string    `bun:"type:text,notnull"`
}

var _ bun.BeforeAppendModelHook = (*S3Account)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (s *S3Account) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	return nil
}
