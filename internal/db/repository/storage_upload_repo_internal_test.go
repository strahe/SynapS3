package repository

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCompleteUploadTasksForVersionRequiresStatuses(t *testing.T) {
	if err := completeUploadTasksForVersion(context.Background(), nil, "", time.Now(), nil); err != nil {
		t.Fatalf("empty versionID error = %v, want nil", err)
	}

	err := completeUploadTasksForVersion(context.Background(), nil, "version-1", time.Now(), nil)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty statuses error = %v, want ErrInvalidInput", err)
	}
}
