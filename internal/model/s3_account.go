package model

import (
	"time"

	"github.com/uptrace/bun"
	"github.com/versity/versitygw/auth"
)

// S3Account stores S3 credentials and roles used by the VersityGW IAM adapter.
type S3Account struct {
	bun.BaseModel `bun:"table:s3_accounts"`

	AccessKey string    `bun:",pk"`
	SecretKey string    `bun:",notnull"`
	Role      auth.Role `bun:",notnull"`
	IsRoot    bool      `bun:",notnull,default:false"`
	CreatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}
