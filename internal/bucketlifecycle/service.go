package bucketlifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/task"
)

// Service coordinates bucket lifecycle operations shared by multiple entrypoints.
type Service struct {
	repos *repository.Repositories
	cache cache.Cache
	tasks *taskengine.Service
	// defaultCopies is the configured replica target a bucket materialises when
	// the caller does not name one. A bucket's policy is stored, not inherited,
	// so later config changes leave existing buckets alone.
	defaultCopies int
	logger        *slog.Logger
}

var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketNotEmpty     = errors.New("bucket not empty")
	ErrDeleteNotSupported = errors.New("bucket deletion is not supported")
	ErrOwnerNotFound      = errors.New("bucket owner not found")
)

type CreateOptions struct {
	Name                 string
	ACL                  []byte
	OwnerAccessKey       *string
	DefaultCopies        *int
	MinimumDurableCopies *int
}

type DeleteOptions struct {
	Recursive bool
}

func (s *Service) SetTaskService(service *taskengine.Service) {
	s.tasks = service
}

func New(repos *repository.Repositories, c cache.Cache, defaultCopies int, logger *slog.Logger) *Service {
	return &Service{
		repos:         repos,
		cache:         c,
		defaultCopies: model.ClampStorageCopies(defaultCopies),
		logger:        logger,
	}
}

func (s *Service) Create(ctx context.Context, name string) (*model.Bucket, error) {
	return s.CreateWithACL(ctx, name, nil)
}

func (s *Service) CreateWithACL(ctx context.Context, name string, acl []byte) (*model.Bucket, error) {
	return s.CreateWithOptions(ctx, CreateOptions{Name: name, ACL: acl})
}

func (s *Service) CreateWithOptions(ctx context.Context, options CreateOptions) (*model.Bucket, error) {
	if s.tasks == nil {
		return nil, errors.New("bucket lifecycle requires a task service")
	}
	// The durability policy is materialised here: a bucket that exists knows how
	// many replica slots it has without anyone re-reading configuration.
	defaultCopies := s.defaultCopies
	if options.DefaultCopies != nil {
		defaultCopies = *options.DefaultCopies
	}
	if !model.ValidStorageCopies(defaultCopies) {
		return nil, fmt.Errorf("creating bucket %q: replica target %d out of range: %w", options.Name, defaultCopies, repository.ErrInvalidInput)
	}
	minimumDurableCopies := defaultCopies
	if options.MinimumDurableCopies != nil {
		minimumDurableCopies = *options.MinimumDurableCopies
	}
	if !model.ValidStorageCopies(minimumDurableCopies) || minimumDurableCopies > defaultCopies {
		return nil, fmt.Errorf("creating bucket %q: minimum durable copies %d out of range: %w", options.Name, minimumDurableCopies, repository.ErrInvalidInput)
	}
	bucket := &model.Bucket{
		Name:                 options.Name,
		ACL:                  options.ACL,
		OwnerAccessKey:       options.OwnerAccessKey,
		DefaultCopies:        defaultCopies,
		MinimumDurableCopies: minimumDurableCopies,
		Status:               model.BucketStatusProvisioning,
	}

	if err := s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if options.OwnerAccessKey != nil {
			owner, err := txRepos.S3Accounts.LockByAccessKey(ctx, *options.OwnerAccessKey)
			if err != nil {
				return err
			}
			if owner == nil {
				return ErrOwnerNotFound
			}
		}
		if err := txRepos.Buckets.Create(ctx, bucket); err != nil {
			return err
		}
		_, _, err := s.tasks.EnqueueInTransaction(ctx, txRepos, taskengine.EnqueueRequest{
			Type:           model.TaskTypeBucketProvision,
			IdempotencyKey: ProvisionKey(bucket.ID, bucket.DefaultCopies),
			Input:          ProvisionInput{BucketID: bucket.ID},
			SubjectType:    "bucket",
			SubjectKey:     strconv.FormatInt(bucket.ID, 10),
		})
		return err
	}); err != nil {
		return nil, fmt.Errorf("creating bucket %q: %w", options.Name, err)
	}

	if err := s.cache.CreateBucketDir(ctx, options.Name); err != nil && s.logger != nil {
		s.logger.Warn("pre-creating cache dir failed (non-fatal)", "bucket", options.Name, "error", err)
	}

	if s.logger != nil {
		s.logger.Info("bucket created", "bucket", options.Name, "id", bucket.ID)
	}
	return bucket, nil
}

func (s *Service) EnsureCacheBucketDir(ctx context.Context, name string) {
	if err := s.cache.CreateBucketDir(ctx, name); err != nil && s.logger != nil {
		s.logger.Warn("pre-creating cache dir failed (non-fatal)", "bucket", name, "error", err)
	}
}

func (s *Service) Delete(_ context.Context, _ string, _ DeleteOptions) (*model.Bucket, error) {
	return nil, ErrDeleteNotSupported
}

// ScheduleProvision enqueues provisioning for the bucket's current replica
// target. It is idempotent per target: raising the target schedules the slots
// that were just opened, lowering it schedules a run that finds nothing to do.
func (s *Service) ScheduleProvision(ctx context.Context, repos *repository.Repositories, bucket *model.Bucket) error {
	if s.tasks == nil {
		return errors.New("bucket lifecycle requires a task service")
	}
	_, _, err := s.tasks.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type:           model.TaskTypeBucketProvision,
		IdempotencyKey: ProvisionKey(bucket.ID, bucket.DefaultCopies),
		Input:          ProvisionInput{BucketID: bucket.ID},
		SubjectType:    "bucket",
		SubjectKey:     strconv.FormatInt(bucket.ID, 10),
	})
	return err
}
