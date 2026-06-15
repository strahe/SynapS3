package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/objectkey"
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

type objectUploader interface {
	PutObject(context.Context, s3response.PutObjectInput) (s3response.PutObjectOutput, error)
}

type objectUploadResponse struct {
	Key         string `json:"key"`
	VersionID   string `json:"version_id"`
	ETag        string `json:"etag"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
}

const objectUploadWriteTimeout = time.Hour

// WithObjectUploader enables admin object uploads through the existing object write path.
func (s *Server) WithObjectUploader(uploader objectUploader) *Server {
	s.objectUploader = uploader
	return s
}

func (s *Server) handleAPIUploadObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "object key is required"})
		return
	}
	if err := objectkey.Validate(key); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if s.objectUploader == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "object uploads are unavailable"})
		return
	}
	if r.ContentLength >= 0 {
		if err := objectlimits.ValidateFOCUploadSize(r.ContentLength); err != nil {
			s.writeObjectUploadSizeError(w, err)
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, objectlimits.MaxFOCUploadSize)
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(objectUploadWriteTimeout)); err != nil {
		s.logger.Warn("api: failed to set object upload write deadline", "error", err, "bucket", bucketName, "key", key)
	}

	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var contentLength *int64
	if r.ContentLength >= 0 {
		contentLength = &r.ContentLength
	}
	out, err := s.objectUploader.PutObject(ctx, s3response.PutObjectInput{
		Bucket:        &bucketName,
		Key:           &key,
		ContentType:   &contentType,
		ContentLength: contentLength,
		Body:          r.Body,
	})
	if err != nil {
		s.writeObjectUploadError(w, err, bucketName, key)
		return
	}

	var size int64
	if out.Size != nil {
		size = *out.Size
	} else {
		s.logger.Error("api: object uploader did not return size for successful upload", "bucket", bucketName, "key", key)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error: uploader did not return object size"})
		return
	}
	writeJSON(w, http.StatusOK, objectUploadResponse{
		Key:         key,
		VersionID:   out.VersionID,
		ETag:        out.ETag,
		Size:        size,
		ContentType: contentType,
	})
}

func (s *Server) writeObjectUploadError(w http.ResponseWriter, err error, bucketName string, key string) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		s.writeObjectUploadSizeError(w, &objectlimits.SizeError{Size: maxBytesErr.Limit + 1, Err: objectlimits.ErrTooLarge})
		return
	}
	if errors.Is(err, objectlimits.ErrTooSmall) || errors.Is(err, objectlimits.ErrTooLarge) {
		s.writeObjectUploadSizeError(w, err)
		return
	}
	if errors.Is(err, cache.ErrCacheFull) {
		writeJSON(w, http.StatusInsufficientStorage, map[string]string{"error": "cache capacity exceeded"})
		return
	}
	if errors.Is(err, cache.ErrInvalidPath) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid object path"})
		return
	}
	var s3Err s3err.S3Error
	if errors.As(err, &s3Err) {
		apiErr := s3Err.BaseError()
		status := s3Err.StatusCode()
		if status == 0 {
			status = http.StatusInternalServerError
		}
		if apiErr.Code == "EntityTooLarge" {
			status = http.StatusRequestEntityTooLarge
		}
		message := apiErr.Code
		if message == "" {
			message = apiErr.Description
		}
		if message == "" {
			message = "upload failed"
		}
		writeJSON(w, status, map[string]string{"error": message})
		return
	}
	s.logger.Error("api: failed to upload object", "error", err, "bucket", bucketName, "key", key)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
}

func (s *Server) writeObjectUploadSizeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	code := "EntityTooSmall"
	if errors.Is(err, objectlimits.ErrTooLarge) {
		status = http.StatusRequestEntityTooLarge
		code = "EntityTooLarge"
	}
	writeJSON(w, status, map[string]string{"error": code + ": " + err.Error()})
}
