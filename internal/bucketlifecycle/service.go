package bucketlifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

// Service coordinates bucket lifecycle operations shared by multiple entrypoints.
type Service struct {
	repos  *repository.Repositories
	cache  cache.Cache
	logger *slog.Logger
}

var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketNotEmpty     = errors.New("bucket not empty")
	ErrDeleteNotSupported = errors.New("bucket deletion is not supported")
)

type DeleteOptions struct {
	Recursive bool
}

func New(repos *repository.Repositories, c cache.Cache, logger *slog.Logger) *Service {
	return &Service{
		repos:  repos,
		cache:  c,
		logger: logger,
	}
}

func (s *Service) Create(ctx context.Context, name string) (*model.Bucket, error) {
	return s.CreateWithACL(ctx, name, nil)
}

func (s *Service) CreateWithACL(ctx context.Context, name string, acl []byte) (*model.Bucket, error) {
	bucket := &model.Bucket{
		Name:   name,
		ACL:    acl,
		Status: model.BucketStatusActive,
	}

	if err := s.repos.Buckets.Create(ctx, bucket); err != nil {
		return nil, fmt.Errorf("creating bucket %q: %w", name, err)
	}

	if err := s.cache.CreateBucketDir(ctx, name); err != nil && s.logger != nil {
		s.logger.Warn("pre-creating cache dir failed (non-fatal)", "bucket", name, "error", err)
	}

	if s.logger != nil {
		s.logger.Info("bucket created", "bucket", name, "id", bucket.ID)
	}
	return bucket, nil
}

func (s *Service) Delete(_ context.Context, _ string, _ DeleteOptions) (*model.Bucket, error) {
	return nil, ErrDeleteNotSupported
}
