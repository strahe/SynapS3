package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

var (
	ErrDeploymentIdentityMissing  = errors.New("database has business records but no deployment identity")
	ErrDeploymentIdentityMismatch = errors.New("database belongs to another Filecoin wallet or network")
)

type BindDeploymentIdentityInput struct {
	Payer   string
	ChainID int64
}

type DeploymentIdentityRepository interface {
	BindOrVerify(context.Context, BindDeploymentIdentityInput) (*model.DeploymentIdentity, error)
}

type BunDeploymentIdentityRepo struct{ db bun.IDB }

var _ DeploymentIdentityRepository = (*BunDeploymentIdentityRepo)(nil)

func (r *BunDeploymentIdentityRepo) BindOrVerify(ctx context.Context, input BindDeploymentIdentityInput) (*model.DeploymentIdentity, error) {
	if !common.IsHexAddress(input.Payer) || common.HexToAddress(input.Payer) == (common.Address{}) || input.ChainID <= 0 {
		return nil, ErrInvalidInput
	}
	input.Payer = strings.ToLower(common.HexToAddress(input.Payer).Hex())
	var identity model.DeploymentIdentity
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		_, err := db.NewRaw(`INSERT INTO deployment_identity (id, payer, chain_id, created_at)
			SELECT 1, ?, ?, ?
			WHERE NOT EXISTS (SELECT 1 FROM s3_accounts)
			  AND NOT EXISTS (SELECT 1 FROM buckets)
			  AND NOT EXISTS (SELECT 1 FROM tasks)
			  AND NOT EXISTS (SELECT 1 FROM task_schedules)
			  AND NOT EXISTS (SELECT 1 FROM wallet_operations)
			  AND NOT EXISTS (SELECT 1 FROM observability_collection_states)
			  AND NOT EXISTS (SELECT 1 FROM observability_provider_states)
			  AND NOT EXISTS (SELECT 1 FROM provider_profiles)
			  AND NOT EXISTS (SELECT 1 FROM provider_tier_snapshots)
			  AND NOT EXISTS (SELECT 1 FROM provider_upload_speed_tests)
			ON CONFLICT (id) DO NOTHING`, input.Payer, input.ChainID, time.Now().UTC()).Exec(ctx)
		if err != nil {
			return fmt.Errorf("binding deployment identity: %w", err)
		}
		if err := db.NewSelect().Model(&identity).Where("id = 1").Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrDeploymentIdentityMissing
			}
			return fmt.Errorf("reading deployment identity: %w", err)
		}
		if identity.Payer != input.Payer || identity.ChainID != input.ChainID {
			return ErrDeploymentIdentityMismatch
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &identity, nil
}
