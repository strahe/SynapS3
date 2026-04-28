package s3iam

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/securetoken"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

// Service adapts the SynapS3 database account store to VersityGW's IAMService.
type Service struct {
	repos *repository.Repositories
}

var _ auth.IAMService = (*Service)(nil)

func NewService(repos *repository.Repositories) *Service {
	return &Service{repos: repos}
}

func (s *Service) EnsureRootAccount(ctx context.Context) (auth.Account, error) {
	root, err := s.repos.S3Accounts.GetRoot(ctx)
	if err != nil {
		return auth.Account{}, err
	}
	if root != nil {
		return toAuthAccount(root), nil
	}

	accessKey, err := generateAccessKey()
	if err != nil {
		return auth.Account{}, err
	}
	secretKey, err := generateSecretKey()
	if err != nil {
		return auth.Account{}, err
	}
	account := &model.S3Account{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Role:      auth.RoleAdmin,
		IsRoot:    true,
	}
	if err := s.repos.S3Accounts.Create(ctx, account); err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			root, getErr := s.repos.S3Accounts.GetRoot(ctx)
			if getErr != nil {
				return auth.Account{}, getErr
			}
			if root != nil {
				return toAuthAccount(root), nil
			}
		}
		return auth.Account{}, err
	}
	return toAuthAccount(account), nil
}

func (s *Service) CreateAccount(account auth.Account) error {
	account.Access = strings.TrimSpace(account.Access)
	if account.Access == "" || account.Secret == "" || !account.Role.IsValid() {
		return fmt.Errorf("invalid S3 account")
	}
	if existing, err := s.repos.S3Accounts.GetRoot(context.Background()); err != nil {
		return err
	} else if existing != nil && existing.AccessKey == account.Access {
		return auth.ErrUserExists
	}
	err := s.repos.S3Accounts.Create(context.Background(), &model.S3Account{
		AccessKey: account.Access,
		SecretKey: account.Secret,
		Role:      account.Role,
		IsRoot:    false,
	})
	if errors.Is(err, repository.ErrAlreadyExists) {
		return auth.ErrUserExists
	}
	return err
}

func (s *Service) GetUserAccount(access string) (auth.Account, error) {
	account, err := s.repos.S3Accounts.GetByAccessKey(context.Background(), access)
	if err != nil {
		return auth.Account{}, err
	}
	if account == nil {
		return auth.Account{}, auth.ErrNoSuchUser
	}
	return toAuthAccount(account), nil
}

func (s *Service) UpdateUserAccount(access string, props auth.MutableProps) error {
	root, err := s.repos.S3Accounts.GetRoot(context.Background())
	if err != nil {
		return err
	}
	if root != nil && root.AccessKey == access {
		return auth.ErrNoSuchUser
	}
	update := repository.S3AccountUpdate{SecretKey: props.Secret, Role: props.Role}
	err = s.repos.S3Accounts.Update(context.Background(), access, update)
	if errors.Is(err, repository.ErrNotFound) {
		return auth.ErrNoSuchUser
	}
	return err
}

func (s *Service) DeleteUserAccount(access string) error {
	ctx := context.Background()
	err := s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		account, err := txRepos.S3Accounts.LockByAccessKey(ctx, access)
		if err != nil {
			return err
		}
		if account == nil || account.IsRoot {
			return auth.ErrNoSuchUser
		}
		count, err := txRepos.Buckets.CountByOwner(ctx, access)
		if err != nil {
			return err
		}
		if count > 0 {
			return accountOwnsBucketsError(count)
		}
		return txRepos.S3Accounts.Delete(ctx, access)
	})
	if errors.Is(err, repository.ErrNotFound) {
		return auth.ErrNoSuchUser
	}
	return err
}

func (s *Service) ListUserAccounts() ([]auth.Account, error) {
	accounts, err := s.repos.S3Accounts.ListNonRoot(context.Background())
	if err != nil {
		return nil, err
	}
	result := make([]auth.Account, 0, len(accounts))
	for _, account := range accounts {
		result = append(result, toAuthAccount(&account))
	}
	return result, nil
}

func (s *Service) Shutdown() error {
	return nil
}

func toAuthAccount(account *model.S3Account) auth.Account {
	if account == nil {
		return auth.Account{}
	}
	return auth.Account{
		Access: account.AccessKey,
		Secret: account.SecretKey,
		Role:   account.Role,
	}
}

func generateAccessKey() (string, error) {
	return securetoken.URL(20)
}

func generateSecretKey() (string, error) {
	return securetoken.URL(32)
}

func accountOwnsBucketsError(count int) s3err.APIError {
	return s3err.APIError{
		Code:           "XAdminUserOwnsBuckets",
		Description:    fmt.Sprintf("S3 user owns %d bucket(s); transfer bucket ownership before deleting.", count),
		HTTPStatusCode: http.StatusConflict,
	}
}
