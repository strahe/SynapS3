package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/objectkey"
	"github.com/strahe/synaps3/internal/objectlimits"
)

const (
	objectVersionRestoreTimeout     = time.Hour
	objectVersionAlreadyCurrentCode = "object_version_already_current"
)

type objectVersionRestorer interface {
	RestoreObjectVersion(ctx context.Context, bucketName, key, sourceVersionID, expectedCurrentVersionID string) (string, error)
}

type restoreObjectVersionRequest struct {
	Key                      string `json:"key"`
	VersionID                string `json:"version_id"`
	ExpectedCurrentVersionID string `json:"expected_current_version_id"`
}

type restoreObjectVersionResponse struct {
	Key             string `json:"key"`
	SourceVersionID string `json:"source_version_id"`
	VersionID       string `json:"version_id"`
}

// WithObjectVersionRestorer enables admin object version restore operations.
func (s *Server) WithObjectVersionRestorer(restorer objectVersionRestorer) *Server {
	s.objectVersionRestorer = restorer
	return s
}

func (s *Server) handleAPIRestoreObjectVersion(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	var req restoreObjectVersionRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	req.VersionID = strings.TrimSpace(req.VersionID)
	req.ExpectedCurrentVersionID = strings.TrimSpace(req.ExpectedCurrentVersionID)
	if req.Key == "" || req.VersionID == "" || req.ExpectedCurrentVersionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key, version_id, and expected_current_version_id are required"})
		return
	}
	if err := objectkey.Validate(req.Key); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if s.objectVersionRestorer == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "object version restore is unavailable"})
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(objectVersionRestoreTimeout)); err != nil {
		s.logger.Warn("api: failed to set object version restore write deadline", "error", err, "bucket", bucketName, "key", req.Key)
	}

	ctx, cancel := context.WithTimeout(r.Context(), objectVersionRestoreTimeout)
	defer cancel()
	versionID, err := s.objectVersionRestorer.RestoreObjectVersion(
		ctx,
		bucketName,
		req.Key,
		req.VersionID,
		req.ExpectedCurrentVersionID,
	)
	if err != nil {
		s.writeObjectVersionRestoreError(w, err, bucketName, req.Key, req.VersionID)
		return
	}
	writeJSON(w, http.StatusOK, restoreObjectVersionResponse{
		Key:             req.Key,
		SourceVersionID: req.VersionID,
		VersionID:       versionID,
	})
}

func (s *Server) writeObjectVersionRestoreError(w http.ResponseWriter, err error, bucketName, key, sourceVersionID string) {
	switch {
	case errors.Is(err, repository.ErrInvalidInput), errors.Is(err, objectlimits.ErrTooSmall), errors.Is(err, objectlimits.ErrTooLarge):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid restore request"})
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object version not found"})
	case errors.Is(err, repository.ErrAlreadyCurrent):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "selected version already matches the current object",
			"code":  objectVersionAlreadyCurrentCode,
		})
	case errors.Is(err, repository.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "object versions changed; refresh and confirm the restore again"})
	case errors.Is(err, cache.ErrCacheFull):
		writeJSON(w, http.StatusInsufficientStorage, map[string]string{"error": "cache capacity exceeded"})
	default:
		s.logger.Error("api: failed to restore object version", "error", err, "bucket", bucketName, "key", key, "sourceVersionID", sourceVersionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
	}
}
