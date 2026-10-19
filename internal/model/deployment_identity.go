package model

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

type DeploymentIdentity struct {
	bun.BaseModel `bun:"table:deployment_identity"`
	ID            int16     `bun:"type:smallint,pk"`
	Payer         string    `bun:"type:text,notnull"`
	ChainID       int64     `bun:"type:bigint,notnull"`
	CreatedAt     time.Time `bun:",notnull"`
}

var _ bun.BeforeAppendModelHook = (*DeploymentIdentity)(nil)

func (identity *DeploymentIdentity) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); ok && identity.CreatedAt.IsZero() {
		identity.CreatedAt = time.Now().UTC()
	}
	return nil
}
