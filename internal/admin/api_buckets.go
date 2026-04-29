package admin

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/versity/versitygw/auth"
)

const internalRootOwnerAccessKey = "__internal_root__"

type bucketListItem struct {
	ID             int64   `json:"id"`
	Name           string  `json:"name"`
	OwnerAccessKey *string `json:"owner_access_key"`
	Status         string  `json:"status"`
	ProofSetID     *string `json:"proof_set_id"`
	ObjectCount    int64   `json:"object_count"`
	TotalSizeBytes int64   `json:"total_size_bytes"`
	CreatedAt      string  `json:"created_at"`
}

type bucketCreateRequest struct {
	Name           string `json:"name"`
	OwnerAccessKey string `json:"owner_access_key"`
}

type bucketMutationResponse struct {
	ID             int64   `json:"id"`
	Name           string  `json:"name"`
	OwnerAccessKey *string `json:"owner_access_key"`
	Status         string  `json:"status"`
}

type bucketDetailResponse struct {
	ID                 int64   `json:"id"`
	Name               string  `json:"name"`
	OwnerAccessKey     *string `json:"owner_access_key"`
	Status             string  `json:"status"`
	ProofSetID         *string `json:"proof_set_id"`
	ObjectCount        int64   `json:"object_count"`
	TotalSizeBytes     int64   `json:"total_size_bytes"`
	CreatedAt          string  `json:"created_at"`
	UpdatedAt          string  `json:"updated_at"`
	VersioningStatus   string  `json:"versioning_status"`
	VersioningEnforced bool    `json:"versioning_enforced"`
}

type bucketOwnerUpdateRequest struct {
	OwnerAccessKey string `json:"owner_access_key"`
}

func (s *Server) handleAPIListBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	buckets, err := s.repos.Buckets.List(ctx)
	if err != nil {
		s.logger.Error("api: failed to list buckets", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	// Single query for all bucket stats instead of N+1.
	statsMap, err := s.repos.Objects.AggregateByBucket(ctx)
	if err != nil {
		s.logger.Warn("api: failed to aggregate object stats by bucket", "error", err)
		statsMap = make(map[int64]repository.BucketObjectStats)
	}

	items := make([]bucketListItem, 0, len(buckets))
	for _, b := range buckets {
		if !b.Status.IsAdminVisible() {
			continue
		}
		stats := statsMap[b.ID]
		items = append(items, bucketListItem{
			ID:             b.ID,
			Name:           b.Name,
			OwnerAccessKey: s.adminOwnerAccessKey(b.OwnerAccessKey),
			Status:         string(b.Status),
			ProofSetID:     b.ProofSetID,
			ObjectCount:    stats.Count,
			TotalSizeBytes: stats.TotalSize,
			CreatedAt:      b.CreatedAt.Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, items)
}

// bucketNameRe matches valid S3-compatible bucket names (3-63 chars, lowercase
// alphanumeric and hyphens, no leading/trailing hyphen).
var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func (s *Server) handleAPICreateBucket(w http.ResponseWriter, r *http.Request) {
	if !s.requireBucketWrite(w, r) {
		return
	}
	var req bucketCreateRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bucket name is required"})
		return
	}
	if !bucketNameRe.MatchString(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name: must be 3-63 lowercase alphanumeric characters or hyphens, cannot start or end with a hyphen"})
		return
	}
	ownerAccessKey := strings.TrimSpace(req.OwnerAccessKey)
	actualOwnerAccessKey, ok := s.resolveS3BucketOwner(w, ownerAccessKey, http.StatusBadRequest)
	if !ok {
		return
	}
	acl, err := bucketOwnerACL(actualOwnerAccessKey)
	if err != nil {
		s.logger.Error("api: failed to build bucket ACL", "error", err, "owner", actualOwnerAccessKey)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	var bucket *model.Bucket
	err = s.repos.WithTx(r.Context(), func(txRepos *repository.Repositories) error {
		owner, err := txRepos.S3Accounts.LockByAccessKey(r.Context(), actualOwnerAccessKey)
		if err != nil {
			return err
		}
		if owner == nil {
			return auth.ErrNoSuchUser
		}
		bucket = &model.Bucket{
			Name:           name,
			ACL:            acl,
			OwnerAccessKey: &actualOwnerAccessKey,
			Status:         model.BucketStatusActive,
		}
		return txRepos.Buckets.Create(r.Context(), bucket)
	})
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "bucket already exists"})
			return
		}
		if errors.Is(err, auth.ErrNoSuchUser) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "S3 owner not found"})
			return
		}
		s.logger.Error("api: failed to create bucket", "error", err, "name", name)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	s.bucketLifecycle.EnsureCacheBucketDir(r.Context(), name)

	writeJSON(w, http.StatusCreated, bucketMutationResponse{
		ID:             bucket.ID,
		Name:           bucket.Name,
		OwnerAccessKey: s.adminOwnerAccessKey(bucket.OwnerAccessKey),
		Status:         string(bucket.Status),
	})
}

func (s *Server) handleAPIGetBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket detail", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	objectCount, err := s.repos.Objects.CountByBucket(ctx, bucket.ID)
	if err != nil {
		s.logger.Error("api: failed to count bucket objects", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	totalSize, err := s.repos.Objects.TotalSizeByBucket(ctx, bucket.ID)
	if err != nil {
		s.logger.Error("api: failed to sum bucket object size", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	writeJSON(w, http.StatusOK, bucketDetailResponse{
		ID:                 bucket.ID,
		Name:               bucket.Name,
		OwnerAccessKey:     s.adminOwnerAccessKey(bucket.OwnerAccessKey),
		Status:             string(bucket.Status),
		ProofSetID:         bucket.ProofSetID,
		ObjectCount:        objectCount,
		TotalSizeBytes:     totalSize,
		CreatedAt:          bucket.CreatedAt.Format(time.RFC3339),
		UpdatedAt:          bucket.UpdatedAt.Format(time.RFC3339),
		VersioningStatus:   "Enabled",
		VersioningEnforced: true,
	})
}

func (s *Server) handleAPIUpdateBucketOwner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	if !s.requireBucketWrite(w, r) {
		return
	}

	var req bucketOwnerUpdateRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	ownerAccessKey := strings.TrimSpace(req.OwnerAccessKey)

	actualOwnerAccessKey, ok := s.resolveS3BucketOwner(w, ownerAccessKey, http.StatusNotFound)
	if !ok {
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for owner update", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	acl, err := bucketOwnerACL(actualOwnerAccessKey)
	if err != nil {
		s.logger.Error("api: failed to build bucket ACL", "error", err, "owner", actualOwnerAccessKey)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if err := s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		owner, err := txRepos.S3Accounts.LockByAccessKey(ctx, actualOwnerAccessKey)
		if err != nil {
			return err
		}
		if owner == nil {
			return auth.ErrNoSuchUser
		}
		return txRepos.Buckets.SetOwnerAndACL(ctx, bucketName, &actualOwnerAccessKey, acl)
	}); err != nil {
		if errors.Is(err, auth.ErrNoSuchUser) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "S3 owner not found"})
			return
		}
		s.logger.Error("api: failed to update bucket owner", "error", err, "name", bucketName, "owner", actualOwnerAccessKey)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	writeJSON(w, http.StatusOK, bucketMutationResponse{
		ID:             bucket.ID,
		Name:           bucket.Name,
		OwnerAccessKey: s.adminOwnerAccessKey(&actualOwnerAccessKey),
		Status:         string(bucket.Status),
	})
}

func (s *Server) handleAPIDeleteBucket(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"error": "bucket deletion is not currently supported",
	})
}

func decodeBucketStrictJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return false
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return false
	}
	return true
}

func (s *Server) requireBucketWrite(w http.ResponseWriter, r *http.Request) bool {
	if !s.settingsWritable() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "bucket writes require loopback admin binding"})
		return false
	}
	if r.Header.Get(settingsWriteHeader) != settingsWriteHeaderValue {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing settings write header"})
		return false
	}
	return true
}

func (s *Server) resolveS3BucketOwner(w http.ResponseWriter, accessKey string, missingStatus int) (string, bool) {
	if strings.TrimSpace(accessKey) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "owner_access_key is required"})
		return "", false
	}
	if s.s3IAM == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "S3 user management is unavailable"})
		return "", false
	}
	if accessKey == internalRootOwnerAccessKey {
		if strings.TrimSpace(s.s3RootAccess) == "" {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "internal root owner is unavailable"})
			return "", false
		}
		return s.s3RootAccess, true
	}
	if _, err := s.s3IAM.GetUserAccount(accessKey); err != nil {
		if errors.Is(err, auth.ErrNoSuchUser) {
			writeJSON(w, missingStatus, map[string]string{"error": "S3 owner not found"})
			return "", false
		}
		s.logger.Error("api: failed to load S3 owner", "error", err, "owner", accessKey)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return "", false
	}
	return accessKey, true
}

func (s *Server) adminOwnerAccessKey(owner *string) *string {
	if owner == nil {
		return nil
	}
	if s.isS3RootAccess(*owner) {
		root := internalRootOwnerAccessKey
		return &root
	}
	return owner
}

func bucketOwnerACL(owner string) ([]byte, error) {
	return json.Marshal(auth.ACL{
		Owner: owner,
		Grantees: []auth.Grantee{{
			Permission: auth.PermissionFullControl,
			Access:     owner,
			Type:       types.TypeCanonicalUser,
		}},
	})
}

type objectListItem struct {
	ID               int64   `json:"id"`
	Key              string  `json:"key"`
	CurrentVersionID string  `json:"current_version_id"`
	Size             int64   `json:"size"`
	State            string  `json:"state"`
	ContentType      string  `json:"content_type"`
	ETag             string  `json:"etag"`
	PieceCID         *string `json:"piece_cid,omitempty"`
	CreatedAt        string  `json:"created_at"`
	UpdatedAt        string  `json:"updated_at"`
}

type objectListResponse struct {
	Objects    []objectListItem `json:"objects"`
	HasMore    bool             `json:"has_more"`
	NextMarker string           `json:"next_marker,omitempty"`
}

func (s *Server) handleAPIBucketObjects(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}
	if !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	prefix := r.URL.Query().Get("prefix")
	after := r.URL.Query().Get("after")
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	objects, err := s.repos.Objects.ListByBucket(ctx, bucket.ID, prefix, after, limit+1)
	if err != nil {
		s.logger.Error("api: failed to list objects", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	hasMore := len(objects) > limit
	if hasMore {
		objects = objects[:limit]
	}

	items := make([]objectListItem, 0, len(objects))
	for _, o := range objects {
		items = append(items, objectListItem{
			ID:               o.ID,
			Key:              o.Key,
			CurrentVersionID: o.CurrentVersionID,
			Size:             o.Size,
			State:            string(o.State),
			ContentType:      o.ContentType,
			ETag:             o.ETag,
			PieceCID:         o.PieceCID,
			CreatedAt:        o.CreatedAt.Format(time.RFC3339),
			UpdatedAt:        o.UpdatedAt.Format(time.RFC3339),
		})
	}

	resp := objectListResponse{
		Objects: items,
		HasMore: hasMore,
	}
	if hasMore && len(items) > 0 {
		resp.NextMarker = items[len(items)-1].Key
	}

	writeJSON(w, http.StatusOK, resp)
}

type objectVersionListItem struct {
	VersionID   string  `json:"version_id"`
	Key         string  `json:"key"`
	Size        int64   `json:"size"`
	State       string  `json:"state"`
	ContentType string  `json:"content_type"`
	ETag        string  `json:"etag"`
	PieceCID    *string `json:"piece_cid,omitempty"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
	IsCurrent   bool    `json:"is_current"`
}

type objectVersionListResponse struct {
	Versions          []objectVersionListItem `json:"versions"`
	HasMore           bool                    `json:"has_more"`
	NextVersionMarker string                  `json:"next_version_marker,omitempty"`
}

func (s *Server) handleAPIBucketObjectVersions(w http.ResponseWriter, r *http.Request) {
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

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	afterVersionID := r.URL.Query().Get("version_marker")

	versions, err := s.repos.Objects.ListVersionsByKey(ctx, bucket.ID, key, afterVersionID, limit+1)
	if err != nil {
		s.logger.Error("api: failed to list object versions", "error", err, "bucket", bucketName, "key", key)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	hasMore := len(versions) > limit
	if hasMore {
		versions = versions[:limit]
	}
	items := make([]objectVersionListItem, 0, len(versions))
	for _, v := range versions {
		items = append(items, objectVersionListItem{
			VersionID:   v.VersionID,
			Key:         v.Key,
			Size:        v.Size,
			State:       string(v.State),
			ContentType: v.ContentType,
			ETag:        v.ETag,
			PieceCID:    v.PieceCID,
			CreatedAt:   v.CreatedAt.Format(time.RFC3339),
			UpdatedAt:   v.UpdatedAt.Format(time.RFC3339),
			IsCurrent:   v.VersionID == v.CurrentVersionID,
		})
	}

	resp := objectVersionListResponse{
		Versions: items,
		HasMore:  hasMore,
	}
	if hasMore && len(items) > 0 {
		resp.NextVersionMarker = items[len(items)-1].VersionID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAPIDownloadObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !s.settingsWritable() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "object downloads require loopback admin binding"})
		return
	}

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
	if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {
		s.logger.Warn("api: failed to clear object download write deadline", "error", err, "bucket", bucketName, "key", key)
	}

	reader := s.objectReader
	if reader == nil {
		reader = objectreader.New(s.repos, s.cache, nil, s.logger)
	}

	versionID := r.URL.Query().Get("version_id")
	var out *objectreader.Result
	var err error
	if versionID != "" {
		out, err = reader.OpenVersion(ctx, bucketName, key, versionID, objectreader.AdminVisibility)
	} else {
		out, err = reader.Open(ctx, bucketName, key, objectreader.AdminVisibility)
	}
	if err != nil {
		switch {
		case errors.Is(err, objectreader.ErrInvalidArgument):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		case errors.Is(err, objectreader.ErrNoSuchBucket), errors.Is(err, objectreader.ErrNoSuchKey), errors.Is(err, objectreader.ErrNoSuchVersion):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "object not found"})
		default:
			s.logger.Error("api: failed to open object download", "error", err, "bucket", bucketName, "key", key)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		}
		return
	}
	defer func() { _ = out.Body.Close() }()

	filename := path.Base(key)
	if filename == "." || filename == "/" || filename == "" {
		filename = "download"
	}
	w.Header().Set("Content-Type", out.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(out.Size, 10))
	w.Header().Set("ETag", `"`+out.ETag+`"`)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, out.Body); err != nil {
		s.logger.Warn("api: object download stream failed", "error", err, "bucket", bucketName, "key", key)
	}
}
