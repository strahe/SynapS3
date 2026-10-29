package repository

import (
	"context"
	"slices"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func checkCurrentObjectConditions(ctx context.Context, db bun.IDB, bucketID int64, key string, conditions []ObjectCondition) error {
	if !slices.ContainsFunc(conditions, func(condition ObjectCondition) bool { return condition != nil }) {
		return nil
	}
	r := &BunObjectRepo{db: db}
	version, err := r.GetCurrentVersionByBucketAndKey(ctx, bucketID, key)
	if err != nil {
		return err
	}
	return checkObjectConditions(version, conditions)
}

func checkObjectConditions(version *model.ObjectVersion, conditions []ObjectCondition) error {
	for _, condition := range conditions {
		if condition != nil {
			if err := condition(version); err != nil {
				return err
			}
		}
	}
	return nil
}
