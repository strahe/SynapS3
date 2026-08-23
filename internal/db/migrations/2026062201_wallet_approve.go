package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func init() {
	Migrations.MustRegister(
		transactionalMigration(up2026062201WalletApprove),
		transactionalMigration(down2026062201WalletApprove),
	)
}

func up2026062201WalletApprove(ctx context.Context, db bun.IDB) error {
	done, err := walletApproveReached2026062201(ctx, db, true)
	if err != nil || done {
		return err
	}
	if db.Dialect().Name() == dialect.PG {
		if _, err := db.ExecContext(ctx, "ALTER TABLE wallet_operations DROP CONSTRAINT chk_wallet_operations_type"); err != nil {
			return fmt.Errorf("dropping wallet operation type constraint: %w", err)
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE wallet_operations DROP CONSTRAINT chk_wallet_operations_amount"); err != nil {
			return fmt.Errorf("dropping wallet operation amount constraint: %w", err)
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE wallet_operations ADD CONSTRAINT chk_wallet_operations_type CHECK ("type" IN ('fund', 'withdraw', 'approve'))`); err != nil {
			return fmt.Errorf("adding wallet operation type constraint: %w", err)
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE wallet_operations ADD CONSTRAINT chk_wallet_operations_amount CHECK ((type = 'approve' AND amount = '0') OR (type IN ('fund', 'withdraw') AND amount ~ '^[1-9][0-9]*$'))`); err != nil {
			return fmt.Errorf("adding wallet operation amount constraint: %w", err)
		}
		return nil
	}
	return rebuildSQLiteWalletOperations(ctx, db, true)
}

func down2026062201WalletApprove(ctx context.Context, db bun.IDB) error {
	done, err := walletApproveReached2026062201(ctx, db, false)
	if err != nil || done {
		return err
	}
	var approveCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM wallet_operations WHERE type = 'approve'").Scan(&approveCount); err != nil {
		return fmt.Errorf("counting approve wallet operations: %w", err)
	}
	if approveCount > 0 {
		return fmt.Errorf("cannot remove wallet approve support while approve operations exist")
	}

	if db.Dialect().Name() == dialect.PG {
		if _, err := db.ExecContext(ctx, "ALTER TABLE wallet_operations DROP CONSTRAINT chk_wallet_operations_type"); err != nil {
			return fmt.Errorf("dropping wallet operation type constraint: %w", err)
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE wallet_operations DROP CONSTRAINT chk_wallet_operations_amount"); err != nil {
			return fmt.Errorf("dropping wallet operation amount constraint: %w", err)
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE wallet_operations ADD CONSTRAINT chk_wallet_operations_type CHECK ("type" IN ('fund', 'withdraw'))`); err != nil {
			return fmt.Errorf("restoring wallet operation type constraint: %w", err)
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE wallet_operations ADD CONSTRAINT chk_wallet_operations_amount CHECK (amount ~ '^[1-9][0-9]*$')`); err != nil {
			return fmt.Errorf("restoring wallet operation amount constraint: %w", err)
		}
		return nil
	}
	return rebuildSQLiteWalletOperations(ctx, db, false)
}

func rebuildSQLiteWalletOperations(ctx context.Context, db bun.IDB, allowApprove bool) error {
	typeCheck := `"type" IN ('fund', 'withdraw')`
	amountCheck := `amount GLOB '[1-9]*' AND amount NOT GLOB '*[^0-9]*'`
	if allowApprove {
		typeCheck = `"type" IN ('fund', 'withdraw', 'approve')`
		amountCheck = `(("type" = 'approve' AND amount = '0') OR ("type" IN ('fund', 'withdraw') AND amount GLOB '[1-9]*' AND amount NOT GLOB '*[^0-9]*'))`
	}

	statements := []string{
		"DROP INDEX IF EXISTS idx_wallet_operations_request",
		"DROP INDEX IF EXISTS idx_wallet_operations_status_created",
		"ALTER TABLE wallet_operations RENAME TO wallet_operations_2026062201_old",
		fmt.Sprintf(`CREATE TABLE wallet_operations (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			client_request_id TEXT NOT NULL,
			amount TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			tx_hash TEXT,
			last_error TEXT,
			lease_until TIMESTAMP,
			started_at TIMESTAMP,
			submitted_at TIMESTAMP,
			completed_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
			updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
			CONSTRAINT chk_wallet_operations_type CHECK (%s),
			CONSTRAINT chk_wallet_operations_status CHECK (status IN ('pending', 'running', 'submitted', 'confirmed', 'failed', 'unknown')),
			CONSTRAINT chk_wallet_operations_amount CHECK (%s)
		)`, typeCheck, amountCheck),
		`INSERT INTO wallet_operations (
			id, type, client_request_id, amount, status, tx_hash, last_error, lease_until,
			started_at, submitted_at, completed_at, created_at, updated_at
		)
		SELECT
			id, type, client_request_id, amount, status, tx_hash, last_error, lease_until,
			started_at, submitted_at, completed_at, created_at, updated_at
		FROM wallet_operations_2026062201_old`,
		"DROP TABLE wallet_operations_2026062201_old",
		"CREATE UNIQUE INDEX idx_wallet_operations_request ON wallet_operations (type, client_request_id)",
		"CREATE INDEX idx_wallet_operations_status_created ON wallet_operations (status, created_at, id)",
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("rebuilding wallet_operations table: %w", err)
		}
	}
	return nil
}

func walletApproveReached2026062201(ctx context.Context, db bun.IDB, wantApprove bool) (bool, error) {
	typeConstraint, err := walletConstraintExists2026062201(ctx, db, "chk_wallet_operations_type")
	if err != nil {
		return false, fmt.Errorf("checking wallet operation type constraint: %w", err)
	}
	amountConstraint, err := walletConstraintExists2026062201(ctx, db, "chk_wallet_operations_amount")
	if err != nil {
		return false, fmt.Errorf("checking wallet operation amount constraint: %w", err)
	}
	typeAllowsApprove, err := walletConstraintAllowsApprove2026062201(
		ctx,
		db,
		"chk_wallet_operations_type",
		`"type" IN ('fund', 'withdraw', 'approve')`,
	)
	if err != nil {
		return false, fmt.Errorf("checking wallet operation type constraint definition: %w", err)
	}
	amountAllowsApprove, err := walletConstraintAllowsApprove2026062201(
		ctx,
		db,
		"chk_wallet_operations_amount",
		`"type" = 'approve'`,
	)
	if err != nil {
		return false, fmt.Errorf("checking wallet operation amount constraint definition: %w", err)
	}

	if typeConstraint && amountConstraint && typeAllowsApprove == amountAllowsApprove {
		return typeAllowsApprove == wantApprove, nil
	}
	return false, fmt.Errorf(
		"2026062201_wallet_approve has partial schema state: type_constraint=%t, amount_constraint=%t, type_allows_approve=%t, amount_allows_approve=%t",
		typeConstraint,
		amountConstraint,
		typeAllowsApprove,
		amountAllowsApprove,
	)
}

func walletConstraintExists2026062201(ctx context.Context, db bun.IDB, name string) (bool, error) {
	if db.Dialect().Name() == dialect.PG {
		return queryExists(ctx, db, `SELECT COUNT(*)
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = current_schema()
			  AND t.relname = 'wallet_operations'
			  AND c.conname = ?`, name)
	}
	var schemaSQL string
	if err := db.NewRaw("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'wallet_operations'").Scan(ctx, &schemaSQL); err != nil {
		return false, err
	}
	return strings.Contains(schemaSQL, name), nil
}

func walletConstraintAllowsApprove2026062201(
	ctx context.Context,
	db bun.IDB,
	name, sqliteNeedle string,
) (bool, error) {
	var definition string
	if db.Dialect().Name() == dialect.PG {
		return queryExists(ctx, db, `SELECT COUNT(*)
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = current_schema()
			  AND t.relname = 'wallet_operations'
			  AND c.conname = ?
			  AND position('approve' IN pg_get_constraintdef(c.oid)) > 0`, name)
	}
	if err := db.NewRaw(`SELECT COALESCE((
		SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'wallet_operations'
	), '')`).Scan(ctx, &definition); err != nil {
		return false, err
	}
	return strings.Contains(definition, sqliteNeedle), nil
}
