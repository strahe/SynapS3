package admin

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/securetoken"
	"github.com/versity/versitygw/auth"
)

type s3UserListItem struct {
	AccessKey   string `json:"access_key"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	BucketCount int    `json:"bucket_count"`
}

type s3UserCredentialsResponse struct {
	AccessKey string `json:"access_key"`
	Name      string `json:"name"`
	SecretKey string `json:"secret_key"`
	Role      string `json:"role"`
}

type s3UserCreateRequest struct {
	Name string `json:"name"`
	Role string `json:"role,omitempty"`
}

type s3UserUpdateRequest struct {
	Name *string `json:"name"`
	Role *string `json:"role"`
}

func (s *Server) handleAPIListS3Users(w http.ResponseWriter, r *http.Request) {
	ok, status, reason := s.s3UsersAvailable()
	if !ok {
		writeJSON(w, status, settingsErrorResponse{Error: reason})
		return
	}

	accounts, err := s.repos.S3Accounts.ListNonRoot(r.Context())
	if err != nil {
		s.logger.Error("api: failed to list S3 users", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	bucketCounts, err := s.bucketOwnerCounts(r.Context())
	if err != nil {
		s.logger.Error("api: failed to count S3 user buckets", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	items := make([]s3UserListItem, 0, len(accounts))
	for _, account := range accounts {
		items = append(items, s3UserListItem{
			AccessKey:   account.AccessKey,
			Name:        account.Name,
			Role:        string(account.Role),
			BucketCount: bucketCounts[account.AccessKey],
		})
	}
	slices.SortFunc(items, func(a, b s3UserListItem) int {
		return cmp.Compare(a.AccessKey, b.AccessKey)
	})
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleAPICreateS3User(w http.ResponseWriter, r *http.Request) {
	var req s3UserCreateRequest
	if !s.decodeS3UserWriteJSON(w, r, &req) {
		return
	}
	role, ok := parseS3UserRole(req.Role, auth.RoleUserPlus)
	if !ok {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user role"})
		return
	}
	name, ok := normalizeS3UserName(req.Name)
	if !ok {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "S3 user name must be at most 128 characters and contain no control characters"})
		return
	}

	credentials, err := generateS3Credentials()
	if err != nil {
		s.logger.Error("api: failed to generate S3 user credentials", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	account := &model.S3Account{
		AccessKey: credentials.AccessKey,
		Name:      name,
		SecretKey: credentials.SecretKey,
		Role:      role,
	}
	if err := s.repos.S3Accounts.Create(r.Context(), account); err != nil {
		if errors.Is(err, repository.ErrS3AccountNameExists) {
			writeJSON(w, http.StatusConflict, settingsErrorResponse{Error: "S3 user name already exists"})
			return
		}
		if errors.Is(err, repository.ErrAlreadyExists) {
			writeJSON(w, http.StatusConflict, settingsErrorResponse{Error: "S3 user already exists"})
			return
		}
		s.logger.Error("api: failed to create S3 user", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}

	writeJSON(w, http.StatusCreated, s3UserCredentialsResponse{
		AccessKey: credentials.AccessKey,
		Name:      name,
		SecretKey: credentials.SecretKey,
		Role:      string(role),
	})
}

func (s *Server) handleAPIUpdateS3User(w http.ResponseWriter, r *http.Request) {
	accessKey := r.PathValue("accessKey")
	if s.isS3RootAccess(accessKey) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "root S3 user cannot be modified"})
		return
	}

	var req s3UserUpdateRequest
	if !s.decodeS3UserWriteJSON(w, r, &req) {
		return
	}
	if req.Name == nil && req.Role == nil {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "name or role is required"})
		return
	}
	update := repository.S3AccountUpdate{}
	if req.Name != nil {
		name, ok := normalizeS3UserName(*req.Name)
		if !ok {
			writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "S3 user name must be at most 128 characters and contain no control characters"})
			return
		}
		update.Name = &name
	}
	if req.Role != nil {
		role, ok := parseS3UserRole(*req.Role, "")
		if !ok {
			writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user role"})
			return
		}
		update.Role = role
	}
	if err := s.repos.S3Accounts.Update(r.Context(), accessKey, update); err != nil {
		if errors.Is(err, repository.ErrS3AccountNameExists) {
			writeJSON(w, http.StatusConflict, settingsErrorResponse{Error: "S3 user name already exists"})
			return
		}
		if errors.Is(err, repository.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, settingsErrorResponse{Error: "S3 user not found"})
			return
		}
		s.logger.Error("api: failed to update S3 user", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	account, err := s.repos.S3Accounts.GetByAccessKey(r.Context(), accessKey)
	if err != nil || account == nil {
		s.logger.Error("api: failed to load S3 user after update", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	bucketCount, err := s.bucketOwnerCount(r.Context(), accessKey)
	if err != nil {
		s.logger.Error("api: failed to count S3 user buckets after update", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s3UserListItem{AccessKey: accessKey, Name: account.Name, Role: string(account.Role), BucketCount: bucketCount})
}

func (s *Server) handleAPIRotateS3UserSecret(w http.ResponseWriter, r *http.Request) {
	accessKey := r.PathValue("accessKey")
	if s.isS3RootAccess(accessKey) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "root S3 user cannot be modified"})
		return
	}
	if !s.requireS3UserWrite(w, r) {
		return
	}
	if !s.decodeOptionalEmptyJSON(w, r) {
		return
	}

	account, err := s.repos.S3Accounts.GetByAccessKey(r.Context(), accessKey)
	if err != nil {
		s.logger.Error("api: failed to load S3 user for secret rotation", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	if account == nil {
		writeJSON(w, http.StatusNotFound, settingsErrorResponse{Error: "S3 user not found"})
		return
	}
	secretKey, err := securetoken.URL(32)
	if err != nil {
		s.logger.Error("api: failed to generate S3 user secret", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	if err := s.s3IAM.UpdateUserAccount(accessKey, auth.MutableProps{Secret: &secretKey}); err != nil {
		s.logger.Error("api: failed to rotate S3 user secret", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s3UserCredentialsResponse{
		AccessKey: accessKey,
		Name:      account.Name,
		SecretKey: secretKey,
		Role:      string(account.Role),
	})
}

func normalizeS3UserName(value string) (string, bool) {
	if strings.ContainsFunc(value, unicode.IsControl) {
		return "", false
	}
	name := strings.TrimSpace(value)
	if utf8.RuneCountInString(name) > 128 {
		return "", false
	}
	return name, true
}

func (s *Server) handleAPIDeleteS3User(w http.ResponseWriter, r *http.Request) {
	accessKey := r.PathValue("accessKey")
	if s.isS3RootAccess(accessKey) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "root S3 user cannot be modified"})
		return
	}
	if !s.requireS3UserWrite(w, r) {
		return
	}
	var bucketCount int
	err := s.repos.WithTx(r.Context(), func(txRepos *repository.Repositories) error {
		account, err := txRepos.S3Accounts.LockByAccessKey(r.Context(), accessKey)
		if err != nil {
			return err
		}
		if account == nil {
			return auth.ErrNoSuchUser
		}
		if account.IsRoot {
			return errS3RootMutation
		}
		bucketCount, err = txRepos.Buckets.CountByOwner(r.Context(), accessKey)
		if err != nil {
			return err
		}
		if bucketCount > 0 {
			return errS3UserOwnsBuckets
		}
		return txRepos.S3Accounts.Delete(r.Context(), accessKey)
	})
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrNoSuchUser), errors.Is(err, repository.ErrNotFound):
			writeJSON(w, http.StatusNotFound, settingsErrorResponse{Error: "S3 user not found"})
			return
		case errors.Is(err, errS3RootMutation):
			writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "root S3 user cannot be modified"})
			return
		case errors.Is(err, errS3UserOwnsBuckets):
			writeJSON(w, http.StatusConflict, settingsErrorResponse{
				Error: fmt.Sprintf("S3 user owns %d bucket(s); transfer bucket ownership before deleting", bucketCount),
			})
			return
		default:
			s.logger.Error("api: failed to delete S3 user", "error", err, "access_key", accessKey)
			writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) bucketOwnerCount(ctx context.Context, accessKey string) (int, error) {
	if s.repos == nil {
		return 0, nil
	}
	return s.repos.Buckets.CountByOwner(ctx, accessKey)
}

func (s *Server) bucketOwnerCounts(ctx context.Context) (map[string]int, error) {
	counts := map[string]int{}
	if s.repos == nil {
		return counts, nil
	}
	return s.repos.Buckets.AggregateCountsByOwner(ctx)
}

func parseS3UserRole(value string, defaultRole auth.Role) (auth.Role, bool) {
	if value == "" {
		return defaultRole, defaultRole != ""
	}
	role := auth.Role(value)
	return role, role.IsValid()
}

func (s *Server) s3UsersStatus() settingsS3UsersStatus {
	ok, _, reason := s.s3UsersAvailable()
	return settingsS3UsersStatus{Available: ok, Reason: reason}
}

func (s *Server) s3UsersAvailable() (bool, int, string) {
	if s.s3IAM == nil {
		return false, http.StatusForbidden, "S3 user management is unavailable while setup mode is active"
	}
	return true, http.StatusOK, ""
}

func (s *Server) requireS3UserWrite(w http.ResponseWriter, r *http.Request) bool {
	ok, status, reason := s.s3UsersAvailable()
	if !ok {
		writeJSON(w, status, settingsErrorResponse{Error: reason})
		return false
	}
	return true
}

func (s *Server) decodeS3UserWriteJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if !s.requireS3UserWrite(w, r) {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "S3 user writes require application/json"})
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user payload"})
		return false
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user payload"})
		return false
	}
	return true
}

func (s *Server) decodeOptionalEmptyJSON(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil || r.ContentLength == 0 {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "S3 user writes require application/json"})
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	var body struct{}
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user payload"})
		return false
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user payload"})
		return false
	}
	return true
}

func (s *Server) isS3RootAccess(accessKey string) bool {
	return accessKey == "" || accessKey == s.s3RootAccess
}

var (
	errS3RootMutation    = errors.New("root S3 account cannot be modified")
	errS3UserOwnsBuckets = errors.New("S3 user owns buckets")
)
