package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// BunTaskRepo implements TaskRepository using Bun ORM.
// Only producer operations (Create, GetByID) are included.
// Claim/Complete/Fail are worker-layer concerns.
type BunTaskRepo struct {
	db bun.IDB
}

var _ TaskRepository = (*BunTaskRepo)(nil)

func (r *BunTaskRepo) Create(ctx context.Context, task *model.Task) error {
	_, err := r.db.NewInsert().Model(task).Exec(ctx)
	if err != nil {
		return fmt.Errorf("inserting task: %w", err)
	}
	return nil
}

func (r *BunTaskRepo) GetByID(ctx context.Context, id int64) (*model.Task, error) {
	task := new(model.Task)
	err := r.db.NewSelect().
		Model(task).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting task by id: %w", err)
	}
	return task, nil
}
