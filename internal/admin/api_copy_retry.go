package admin

import (
	"context"

	"github.com/strahe/synaps3/internal/model"
)

func (s *Server) retryTaskForSubject(ctx context.Context, subjectType, subjectKey string, types ...model.TaskType) (*int64, string, error) {
	if s.taskService == nil {
		return nil, "", nil
	}
	row, err := s.repos.Tasks.LatestForSubject(ctx, subjectType, subjectKey, types...)
	if err != nil {
		return nil, "", err
	}
	if row == nil {
		return nil, "Task history is unavailable.", nil
	}
	allowed, err := s.taskService.RetryableContext(ctx, row)
	if err != nil || !allowed {
		return nil, "", err
	}
	return &row.ID, "", nil
}
