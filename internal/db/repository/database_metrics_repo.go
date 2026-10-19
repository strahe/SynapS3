package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
)

type DatabaseMetricsSnapshot struct {
	Tasks   []TaskStatusCount
	Objects []ObjectStateCount
}

// DatabaseMetrics reads a complete sample without permitting database writes.
func (r *Repositories) DatabaseMetrics(ctx context.Context) (DatabaseMetricsSnapshot, error) {
	db, ok := r.db.(*bun.DB)
	if !ok {
		return DatabaseMetricsSnapshot{}, fmt.Errorf("database metrics require a connection pool")
	}
	var sample DatabaseMetricsSnapshot
	err := db.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		repos := NewRepositories(tx)
		var err error
		sample.Tasks, err = repos.Tasks.CountByScope(ctx, TaskScopeWork)
		if err != nil {
			return err
		}
		sample.Objects, err = repos.Objects.CountByState(ctx)
		return err
	})
	if err != nil {
		return DatabaseMetricsSnapshot{}, err
	}
	return sample, nil
}
