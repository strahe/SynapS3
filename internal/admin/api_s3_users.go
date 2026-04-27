package admin

import (
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/strahe/synaps3/internal/securetoken"
	"github.com/versity/versitygw/auth"
)

type s3UserListItem struct {
	AccessKey string `json:"access_key"`
	Role      string `json:"role"`
}

type s3UserCredentialsResponse struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Role      string `json:"role"`
}

type s3UserCreateRequest struct {
	Role string `json:"role,omitempty"`
}

type s3UserUpdateRequest struct {
	Role string `json:"role"`
}

func (s *Server) handleAPIListS3Users(w http.ResponseWriter, _ *http.Request) {
	ok, status, reason := s.s3UsersAvailable()
	if !ok {
		writeJSON(w, status, settingsErrorResponse{Error: reason})
		return
	}

	accounts, err := s.s3IAM.ListUserAccounts()
	if err != nil {
		s.logger.Error("api: failed to list S3 users", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	items := make([]s3UserListItem, 0, len(accounts))
	for _, account := range accounts {
		if account.Access == s.s3RootAccess {
			continue
		}
		items = append(items, s3UserListItem{
			AccessKey: account.Access,
			Role:      string(account.Role),
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

	credentials, err := generateS3Credentials()
	if err != nil {
		s.logger.Error("api: failed to generate S3 user credentials", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	account := auth.Account{
		Access: credentials.AccessKey,
		Secret: credentials.SecretKey,
		Role:   role,
	}
	if err := s.s3IAM.CreateAccount(account); err != nil {
		if errors.Is(err, auth.ErrUserExists) {
			writeJSON(w, http.StatusConflict, settingsErrorResponse{Error: "S3 user already exists"})
			return
		}
		s.logger.Error("api: failed to create S3 user", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}

	writeJSON(w, http.StatusCreated, s3UserCredentialsResponse{
		AccessKey: credentials.AccessKey,
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
	role, ok := parseS3UserRole(req.Role, "")
	if !ok {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid S3 user role"})
		return
	}
	if err := s.s3IAM.UpdateUserAccount(accessKey, auth.MutableProps{Role: role}); err != nil {
		if errors.Is(err, auth.ErrNoSuchUser) {
			writeJSON(w, http.StatusNotFound, settingsErrorResponse{Error: "S3 user not found"})
			return
		}
		s.logger.Error("api: failed to update S3 user", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	writeJSON(w, http.StatusOK, s3UserListItem{AccessKey: accessKey, Role: string(role)})
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

	account, err := s.s3IAM.GetUserAccount(accessKey)
	if err != nil {
		if errors.Is(err, auth.ErrNoSuchUser) {
			writeJSON(w, http.StatusNotFound, settingsErrorResponse{Error: "S3 user not found"})
			return
		}
		s.logger.Error("api: failed to load S3 user for secret rotation", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
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
		SecretKey: secretKey,
		Role:      string(account.Role),
	})
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
	if _, err := s.s3IAM.GetUserAccount(accessKey); err != nil {
		if errors.Is(err, auth.ErrNoSuchUser) {
			writeJSON(w, http.StatusNotFound, settingsErrorResponse{Error: "S3 user not found"})
			return
		}
		s.logger.Error("api: failed to load S3 user for deletion", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	if err := s.s3IAM.DeleteUserAccount(accessKey); err != nil {
		s.logger.Error("api: failed to delete S3 user", "error", err, "access_key", accessKey)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	if !s.settingsWritable() {
		return false, http.StatusForbidden, "S3 user management requires loopback admin binding"
	}
	if s.s3IAMDirRestartPending() {
		return false, http.StatusConflict, "S3 user management requires restart after changing IAM directory"
	}
	return true, http.StatusOK, ""
}

func (s *Server) s3IAMDirRestartPending() bool {
	if s.settings == nil || strings.TrimSpace(s.s3IAMDir) == "" {
		return false
	}
	current := strings.TrimSpace(s.settings.S3IAMDir())
	live := strings.TrimSpace(s.s3IAMDir)
	if current == "" || current == live {
		return false
	}
	return filepath.Clean(current) != filepath.Clean(live)
}

func (s *Server) requireS3UserWrite(w http.ResponseWriter, r *http.Request) bool {
	ok, status, reason := s.s3UsersAvailable()
	if !ok {
		writeJSON(w, status, settingsErrorResponse{Error: reason})
		return false
	}
	if r.Header.Get(settingsWriteHeader) != settingsWriteHeaderValue {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "missing settings write header"})
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
