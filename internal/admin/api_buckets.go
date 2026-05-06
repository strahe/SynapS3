package admin

import (
	"context"
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

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/versity/versitygw/auth"
)

const (
	internalRootOwnerAccessKey = "__internal_root__"

	objectAdminStatusUnavailable = "unavailable"
	objectAdminStatusUploading   = "uploading"
	objectAdminStatusSyncing     = "syncing"
	objectAdminStatusSuccess     = "success"
	objectAdminStatusWarning     = "warning"
)

type bucketListItem struct {
	ID             int64   `json:"id"`
	Name           string  `json:"name"`
	OwnerAccessKey *string `json:"owner_access_key"`
	Status         string  `json:"status"`
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
	ID                 int64                           `json:"id"`
	Name               string                          `json:"name"`
	OwnerAccessKey     *string                         `json:"owner_access_key"`
	Status             string                          `json:"status"`
	ObjectCount        int64                           `json:"object_count"`
	TotalSizeBytes     int64                           `json:"total_size_bytes"`
	CreatedAt          string                          `json:"created_at"`
	UpdatedAt          string                          `json:"updated_at"`
	VersioningStatus   string                          `json:"versioning_status"`
	VersioningEnforced bool                            `json:"versioning_enforced"`
	DataSets           []storageDataSetSummaryResponse `json:"data_sets"`
}

type storageDataSetSummaryResponse struct {
	ID                int64                     `json:"id"`
	BucketID          int64                     `json:"bucket_id"`
	BucketName        string                    `json:"bucket_name,omitempty"`
	CopyIndex         int                       `json:"copy_index"`
	ProviderID        string                    `json:"provider_id"`
	ProviderIdentity  *providerIdentityResponse `json:"provider_identity,omitempty"`
	DataSetID         *string                   `json:"data_set_id,omitempty"`
	ClientDataSetID   *string                   `json:"client_data_set_id,omitempty"`
	Status            string                    `json:"status"`
	CreatedByUploadID *int64                    `json:"created_by_upload_id,omitempty"`
	LastUsedUploadID  *int64                    `json:"last_used_upload_id,omitempty"`
	CreatedAt         string                    `json:"created_at"`
	UpdatedAt         string                    `json:"updated_at"`
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
	dataSets := make([]storageDataSetSummaryResponse, 0)
	if s.repos.Uploads != nil {
		summaries, err := s.repos.Uploads.ListDataSetSummaries(ctx, bucket.ID)
		if err != nil {
			s.logger.Error("api: failed to list bucket storage data sets", "error", err, "name", bucketName)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		dataSets = s.storageDataSetSummaryResponses(summaries)
	}

	writeJSON(w, http.StatusOK, bucketDetailResponse{
		ID:                 bucket.ID,
		Name:               bucket.Name,
		OwnerAccessKey:     s.adminOwnerAccessKey(bucket.OwnerAccessKey),
		Status:             string(bucket.Status),
		ObjectCount:        objectCount,
		TotalSizeBytes:     totalSize,
		CreatedAt:          bucket.CreatedAt.Format(time.RFC3339),
		UpdatedAt:          bucket.UpdatedAt.Format(time.RFC3339),
		VersioningStatus:   "Enabled",
		VersioningEnforced: true,
		DataSets:           dataSets,
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
			Type:       s3types.TypeCanonicalUser,
		}},
	})
}

func (s *Server) storageDataSetSummaryResponses(summaries []repository.StorageDataSetSummary) []storageDataSetSummaryResponse {
	out := make([]storageDataSetSummaryResponse, 0, len(summaries))
	providerIDs := make([]idtypes.OnChainID, 0, len(summaries))
	for _, summary := range summaries {
		providerIDs = append(providerIDs, summary.ProviderID)
	}
	identities := s.providerIdentities(providerIDs)
	for _, summary := range summaries {
		out = append(out, storageDataSetSummaryResponse{
			ID:                summary.ID,
			BucketID:          summary.BucketID,
			BucketName:        summary.BucketName,
			CopyIndex:         summary.CopyIndex,
			ProviderID:        summary.ProviderID.String(),
			ProviderIdentity:  providerIdentityFromSnapshot(identities, summary.ProviderID),
			DataSetID:         onChainIDStringPtr(summary.DataSetID),
			ClientDataSetID:   onChainIDStringPtr(summary.ClientDataSetID),
			Status:            string(summary.Status),
			CreatedByUploadID: summary.CreatedByUploadID,
			LastUsedUploadID:  summary.LastUsedUploadID,
			CreatedAt:         summary.CreatedAt.Format(time.RFC3339),
			UpdatedAt:         summary.UpdatedAt.Format(time.RFC3339),
		})
	}
	return out
}

func (s *Server) providerIdentities(providerIDs []idtypes.OnChainID) map[string]*providerIdentityResponse {
	if s.providerIdentity == nil {
		return nil
	}
	return s.providerIdentity.ProviderIdentities(providerIDs)
}

func providerIdentityFromSnapshot(identities map[string]*providerIdentityResponse, providerID idtypes.OnChainID) *providerIdentityResponse {
	if identities == nil || providerID.IsZero() {
		return nil
	}
	return identities[providerID.String()]
}

func providerIdentityFromSnapshotPtr(identities map[string]*providerIdentityResponse, providerID *idtypes.OnChainID) *providerIdentityResponse {
	if providerID == nil {
		return nil
	}
	return providerIdentityFromSnapshot(identities, *providerID)
}

func onChainIDStringPtr(id *idtypes.OnChainID) *string {
	if id == nil {
		return nil
	}
	value := id.String()
	return &value
}

type objectListItem struct {
	ID               int64                   `json:"id"`
	Key              string                  `json:"key"`
	CurrentVersionID string                  `json:"current_version_id"`
	Size             int64                   `json:"size"`
	State            string                  `json:"state"`
	Status           string                  `json:"status"`
	UploadStatus     *string                 `json:"upload_status,omitempty"`
	Progress         *uploadProgressResponse `json:"progress,omitempty"`
	Location         objectLocation          `json:"location"`
	ContentType      string                  `json:"content_type"`
	ETag             string                  `json:"etag"`
	PieceCID         *string                 `json:"piece_cid,omitempty"`
	CreatedAt        string                  `json:"created_at"`
	UpdatedAt        string                  `json:"updated_at"`
}

type objectLocation struct {
	Cache    bool `json:"cache"`
	Filecoin bool `json:"filecoin"`
}

type objectStatusDetailResponse struct {
	VersionID     string                  `json:"version_id"`
	State         string                  `json:"state"`
	Status        string                  `json:"status"`
	UploadStatus  *string                 `json:"upload_status,omitempty"`
	Progress      *uploadProgressResponse `json:"progress,omitempty"`
	FailedAtState *string                 `json:"failed_at_state,omitempty"`
	Message       *string                 `json:"message,omitempty"`
	UpdatedAt     string                  `json:"updated_at"`
}

type objectProvenanceResponse struct {
	VersionID       string                            `json:"version_id"`
	State           string                            `json:"state"`
	Status          string                            `json:"status"`
	UploadStatus    *string                           `json:"upload_status,omitempty"`
	Progress        *uploadProgressResponse           `json:"progress,omitempty"`
	PieceCID        *string                           `json:"piece_cid,omitempty"`
	RequestedCopies int                               `json:"requested_copies"`
	SuccessCopies   int                               `json:"success_copies"`
	Copies          []objectProvenanceCopyResponse    `json:"copies"`
	Failures        []objectProvenanceFailureResponse `json:"failures"`
	UpdatedAt       string                            `json:"updated_at"`
}

type objectProvenanceCopyResponse struct {
	CopyIndex        int                       `json:"copy_index"`
	Status           string                    `json:"status"`
	ProviderID       *string                   `json:"provider_id,omitempty"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
	DataSetID        *string                   `json:"data_set_id,omitempty"`
	PieceID          *string                   `json:"piece_id,omitempty"`
	Role             string                    `json:"role"`
	RetrievalURL     *string                   `json:"retrieval_url,omitempty"`
	IsNewDataSet     bool                      `json:"is_new_data_set"`
}

type objectProvenanceFailureResponse struct {
	AttemptIndex     int                       `json:"attempt_index"`
	ProviderID       *string                   `json:"provider_id,omitempty"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
	Role             string                    `json:"role"`
	Stage            *string                   `json:"stage,omitempty"`
	Error            *string                   `json:"error,omitempty"`
}

type uploadProgressResponse struct {
	Scope         string `json:"scope"`
	Attempt       int    `json:"attempt"`
	UploadedBytes int64  `json:"uploaded_bytes"`
	TotalBytes    int64  `json:"total_bytes"`
	Percent       *int   `json:"percent,omitempty"`
	Done          bool   `json:"done"`
	UpdatedAt     string `json:"updated_at"`
}

func objectAdminStatusWithUpload(state model.ObjectState, inCache, inFilecoin bool, uploadStatus *model.StorageUploadStatus) string {
	if state == model.ObjectStateFailed {
		return objectAdminStatusWarning
	}
	if uploadStatus != nil {
		switch *uploadStatus {
		case model.StorageUploadStatusPartial,
			model.StorageUploadStatusFailed,
			model.StorageUploadStatusRejected:
			return objectAdminStatusWarning
		case model.StorageUploadStatusStoredOnPrimary:
			return objectAdminStatusUploading
		case model.StorageUploadStatusPrimaryCommitted:
			return objectAdminStatusSyncing
		case model.StorageUploadStatusAllCopiesCommitted:
			return objectAdminStatusSuccess
		}
	}
	if !inCache && !inFilecoin {
		return objectAdminStatusUnavailable
	}
	switch state {
	case model.ObjectStateCached, model.ObjectStateUploading:
		return objectAdminStatusUploading
	case model.ObjectStateCommitting, model.ObjectStateReplicating:
		return objectAdminStatusSyncing
	case model.ObjectStateStored, model.ObjectStateCacheEvicted:
		return objectAdminStatusSuccess
	default:
		return objectAdminStatusUnavailable
	}
}

type objectAdminUploadInfo struct {
	Status   *model.StorageUploadStatus
	Message  *string
	Progress *uploadProgressResponse
}

func (s *Server) objectAdminStorageUpload(ctx context.Context, version model.ObjectVersion) (*model.StorageUpload, error) {
	if s.repos.Uploads == nil {
		return nil, nil
	}
	if version.StorageUploadID != nil {
		return s.repos.Uploads.GetByID(ctx, *version.StorageUploadID)
	}
	return s.repos.Uploads.FindLatestUploadBySourceVersion(ctx, version.VersionID)
}

func (s *Server) objectAdminUploadInfo(ctx context.Context, version model.ObjectVersion) (objectAdminUploadInfo, error) {
	upload, err := s.objectAdminStorageUpload(ctx, version)
	if err != nil || upload == nil {
		return objectAdminUploadInfo{}, err
	}
	return objectAdminUploadInfo{
		Status:   &upload.Status,
		Message:  uploadStatusMessage(upload),
		Progress: uploadProgressResponseFromUpload(upload),
	}, nil
}

func (s *Server) objectAdminUploadInfos(ctx context.Context, versions []model.ObjectVersion) (map[string]objectAdminUploadInfo, error) {
	infos := make(map[string]objectAdminUploadInfo, len(versions))
	if s.repos.Uploads == nil || len(versions) == 0 {
		return infos, nil
	}
	uploadIDSet := make(map[int64]struct{})
	versionIDSet := make(map[string]struct{})
	for _, version := range versions {
		if version.StorageUploadID != nil {
			uploadIDSet[*version.StorageUploadID] = struct{}{}
		} else {
			versionIDSet[version.VersionID] = struct{}{}
		}
	}
	uploadIDs := make([]int64, 0, len(uploadIDSet))
	for uploadID := range uploadIDSet {
		uploadIDs = append(uploadIDs, uploadID)
	}
	uploadsByID, err := s.repos.Uploads.GetByIDs(ctx, uploadIDs)
	if err != nil {
		return nil, err
	}
	versionIDs := make([]string, 0, len(versionIDSet))
	for versionID := range versionIDSet {
		versionIDs = append(versionIDs, versionID)
	}
	uploadsByVersionID, err := s.repos.Uploads.FindLatestUploadsBySourceVersions(ctx, versionIDs)
	if err != nil {
		return nil, err
	}
	for _, version := range versions {
		var upload model.StorageUpload
		var ok bool
		if version.StorageUploadID != nil {
			upload, ok = uploadsByID[*version.StorageUploadID]
		} else {
			upload, ok = uploadsByVersionID[version.VersionID]
		}
		if !ok {
			continue
		}
		status := upload.Status
		infos[version.VersionID] = objectAdminUploadInfo{
			Status:   &status,
			Message:  uploadStatusMessage(&upload),
			Progress: uploadProgressResponseFromUpload(&upload),
		}
	}
	return infos, nil
}

func uploadStatusString(status *model.StorageUploadStatus) *string {
	if status == nil {
		return nil
	}
	value := string(*status)
	return &value
}

func uploadStatusMessage(upload *model.StorageUpload) *string {
	if upload == nil {
		return nil
	}
	if upload.ErrorMessage != nil && *upload.ErrorMessage != "" {
		return upload.ErrorMessage
	}
	if upload.AcceptError != nil && *upload.AcceptError != "" {
		return upload.AcceptError
	}
	return nil
}

func uploadProgressResponseFromUpload(upload *model.StorageUpload) *uploadProgressResponse {
	if upload == nil || upload.ProgressUpdatedAt == nil || upload.PrimaryStoreAttempt <= 0 {
		return nil
	}
	uploaded := upload.PrimaryBytesUploaded
	if uploaded < 0 {
		uploaded = 0
	}
	total := upload.ContentSize
	if total < 0 {
		total = 0
	}
	if uploaded > total {
		uploaded = total
	}
	percent := model.UploadProgressPercent(uploaded, total)
	return &uploadProgressResponse{
		Scope:         "primary_store",
		Attempt:       upload.PrimaryStoreAttempt,
		UploadedBytes: uploaded,
		TotalBytes:    total,
		Percent:       percent,
		Done:          uploaded >= total,
		UpdatedAt:     upload.ProgressUpdatedAt.Format(time.RFC3339),
	}
}

type objectFolderItem struct {
	Name   string `json:"name"`
	Prefix string `json:"prefix"`
}

type objectListResponse struct {
	Folders    []objectFolderItem `json:"folders"`
	Objects    []objectListItem   `json:"objects"`
	HasMore    bool               `json:"has_more"`
	NextMarker string             `json:"next_marker,omitempty"`
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
	delimiter := r.URL.Query().Get("delimiter")
	if delimiter != "" && delimiter != "/" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "delimiter must be /"})
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	folders := make([]objectFolderItem, 0)
	var objects []model.ObjectVersion
	var hasMore bool
	var nextMarker string
	var listErr error
	if delimiter == "" {
		objects, listErr = s.repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, prefix, after, limit+1)
		if listErr == nil {
			hasMore = len(objects) > limit
			if hasMore {
				objects = objects[:limit]
			}
			if hasMore && len(objects) > 0 {
				nextMarker = objects[len(objects)-1].Key
			}
		}
	} else {
		folders, objects, hasMore, nextMarker, listErr = s.listBucketObjectEntries(ctx, bucket.ID, prefix, delimiter, after, limit)
	}
	if listErr != nil {
		s.logger.Error("api: failed to list objects", "error", listErr, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	uploadInfos, err := s.objectAdminUploadInfos(ctx, objects)
	if err != nil {
		s.logger.Error("api: failed to load object upload statuses", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	items := make([]objectListItem, 0, len(objects))
	for _, o := range objects {
		uploadInfo := uploadInfos[o.VersionID]
		items = append(items, objectListItem{
			ID:               o.ObjectID,
			Key:              o.Key,
			CurrentVersionID: o.VersionID,
			Size:             o.Size,
			State:            string(o.State),
			Status:           objectAdminStatusWithUpload(o.State, o.InCache, o.InFilecoin, uploadInfo.Status),
			UploadStatus:     uploadStatusString(uploadInfo.Status),
			Progress:         uploadInfo.Progress,
			Location:         objectLocation{Cache: o.InCache, Filecoin: o.InFilecoin},
			ContentType:      o.ContentType,
			ETag:             o.ETag,
			PieceCID:         o.PieceCID,
			CreatedAt:        o.CreatedAt.Format(time.RFC3339),
			UpdatedAt:        o.UpdatedAt.Format(time.RFC3339),
		})
	}

	resp := objectListResponse{
		Folders: folders,
		Objects: items,
		HasMore: hasMore,
	}
	if hasMore {
		resp.NextMarker = nextMarker
	}

	writeJSON(w, http.StatusOK, resp)
}

const adminObjectListingBatchSize = 1000

func (s *Server) listBucketObjectEntries(ctx context.Context, bucketID int64, prefix, delimiter, afterKey string, maxKeys int) ([]objectFolderItem, []model.ObjectVersion, bool, string, error) {
	if maxKeys <= 0 {
		return []objectFolderItem{}, []model.ObjectVersion{}, false, "", nil
	}

	folders := make([]objectFolderItem, 0)
	objects := make([]model.ObjectVersion, 0)
	seenFolders := make(map[string]struct{})
	cursor := afterKey
	lastMarker := afterKey
	includeCursor := false

	for {
		var rows []model.ObjectVersion
		var err error
		if includeCursor {
			rows, err = s.repos.Objects.ListCurrentVersionsByBucketAtOrAfter(ctx, bucketID, prefix, cursor, adminObjectListingBatchSize)
			includeCursor = false
		} else {
			rows, err = s.repos.Objects.ListCurrentVersionsByBucket(ctx, bucketID, prefix, cursor, adminObjectListingBatchSize)
		}
		if err != nil {
			return nil, nil, false, "", err
		}
		if len(rows) == 0 {
			return folders, objects, false, "", nil
		}

		resumeAtPrefixBound := false
		for rowIndex := 0; rowIndex < len(rows); rowIndex++ {
			obj := rows[rowIndex]
			cursor = obj.Key
			if prefix != "" && obj.Key == prefix {
				lastMarker = obj.Key
				continue
			}

			if commonPrefix, ok := adminListingCommonPrefix(obj.Key, prefix, delimiter); ok {
				if afterKey != "" && commonPrefix <= afterKey {
					rowIndex, cursor, lastMarker, includeCursor, resumeAtPrefixBound = adminListingAdvancePastCurrentPrefix(rows, rowIndex, commonPrefix, delimiter)
					if resumeAtPrefixBound {
						break
					}
					continue
				}
				if _, exists := seenFolders[commonPrefix]; exists {
					rowIndex, cursor, lastMarker, includeCursor, resumeAtPrefixBound = adminListingAdvancePastCurrentPrefix(rows, rowIndex, commonPrefix, delimiter)
					if resumeAtPrefixBound {
						break
					}
					continue
				}
				if len(folders)+len(objects) >= maxKeys {
					return folders, objects, true, lastMarker, nil
				}
				seenFolders[commonPrefix] = struct{}{}
				folders = append(folders, objectFolderItem{
					Name:   adminListingFolderName(commonPrefix, prefix, delimiter),
					Prefix: commonPrefix,
				})
				rowIndex, cursor, lastMarker, includeCursor, resumeAtPrefixBound = adminListingAdvancePastCurrentPrefix(rows, rowIndex, commonPrefix, delimiter)
				if resumeAtPrefixBound {
					break
				}
				continue
			}

			if len(folders)+len(objects) >= maxKeys {
				return folders, objects, true, lastMarker, nil
			}
			objects = append(objects, obj)
			lastMarker = obj.Key
		}

		if resumeAtPrefixBound {
			continue
		}
		if len(rows) < adminObjectListingBatchSize {
			return folders, objects, false, "", nil
		}
	}
}

func adminListingAdvancePastCurrentPrefix(rows []model.ObjectVersion, rowIndex int, commonPrefix, delimiter string) (int, string, string, bool, bool) {
	lastIndex := rowIndex
	for lastIndex+1 < len(rows) && strings.HasPrefix(rows[lastIndex+1].Key, commonPrefix) {
		lastIndex++
	}

	cursor := rows[lastIndex].Key
	if lastIndex != len(rows)-1 || len(rows) < adminObjectListingBatchSize {
		return lastIndex, cursor, cursor, false, false
	}
	upper, ok := adminListingCommonPrefixUpperBound(commonPrefix, delimiter)
	if !ok || upper <= cursor {
		return lastIndex, cursor, cursor, false, false
	}
	return lastIndex, upper, cursor, true, true
}

func adminListingCommonPrefix(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}
	suffix := strings.TrimPrefix(key, prefix)
	before, _, found := strings.Cut(suffix, delimiter)
	if !found {
		return "", false
	}
	return prefix + before + delimiter, true
}

func adminListingCommonPrefixUpperBound(commonPrefix, delimiter string) (string, bool) {
	if delimiter != "/" || !strings.HasSuffix(commonPrefix, delimiter) {
		return "", false
	}
	// The admin API only accepts "/" as delimiter; "0" is the next ASCII byte after "/".
	return strings.TrimSuffix(commonPrefix, delimiter) + "0", true
}

func adminListingFolderName(commonPrefix, prefix, delimiter string) string {
	name := strings.TrimPrefix(commonPrefix, prefix)
	trimmed := strings.TrimSuffix(name, delimiter)
	if trimmed == "" {
		return name
	}
	return trimmed
}

type objectVersionListItem struct {
	VersionID    string                  `json:"version_id"`
	Key          string                  `json:"key"`
	Size         int64                   `json:"size"`
	State        string                  `json:"state"`
	Status       string                  `json:"status"`
	UploadStatus *string                 `json:"upload_status,omitempty"`
	Progress     *uploadProgressResponse `json:"progress,omitempty"`
	Location     objectLocation          `json:"location"`
	ContentType  string                  `json:"content_type"`
	ETag         string                  `json:"etag"`
	PieceCID     *string                 `json:"piece_cid,omitempty"`
	CreatedAt    string                  `json:"created_at"`
	UpdatedAt    string                  `json:"updated_at"`
	IsCurrent    bool                    `json:"is_current"`
}

type objectVersionListResponse struct {
	Versions          []objectVersionListItem `json:"versions"`
	HasMore           bool                    `json:"has_more"`
	NextVersionMarker string                  `json:"next_version_marker,omitempty"`
}

func (s *Server) handleAPIBucketObjectStatusDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	versionID := strings.TrimSpace(r.URL.Query().Get("version_id"))
	if versionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "version_id is required"})
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

	version, err := s.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil {
		s.logger.Error("api: failed to get object version status detail", "error", err, "bucket", bucketName, "versionID", versionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if version == nil || version.BucketID != bucket.ID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object version not found"})
		return
	}
	uploadInfo, err := s.objectAdminUploadInfo(ctx, *version)
	if err != nil {
		s.logger.Error("api: failed to load object upload status detail", "error", err, "bucket", bucketName, "versionID", versionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	var failedAtState *string
	if version.FailedAtState != nil {
		state := string(*version.FailedAtState)
		failedAtState = &state
	}
	message := version.LastError
	if message == nil {
		message = uploadInfo.Message
	}
	writeJSON(w, http.StatusOK, objectStatusDetailResponse{
		VersionID:     version.VersionID,
		State:         string(version.State),
		Status:        objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin, uploadInfo.Status),
		UploadStatus:  uploadStatusString(uploadInfo.Status),
		Progress:      uploadInfo.Progress,
		FailedAtState: failedAtState,
		Message:       message,
		UpdatedAt:     version.UpdatedAt.Format(time.RFC3339),
	})
}

func (s *Server) handleAPIBucketObjectProvenance(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	versionID := strings.TrimSpace(r.URL.Query().Get("version_id"))
	if versionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "version_id is required"})
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

	version, err := s.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil {
		s.logger.Error("api: failed to get object version provenance", "error", err, "bucket", bucketName, "versionID", versionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if version == nil || version.BucketID != bucket.ID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "object version not found"})
		return
	}

	resp := objectProvenanceResponse{
		VersionID: version.VersionID,
		State:     string(version.State),
		Status:    objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin, nil),
		Copies:    make([]objectProvenanceCopyResponse, 0),
		Failures:  make([]objectProvenanceFailureResponse, 0),
		UpdatedAt: version.UpdatedAt.Format(time.RFC3339),
	}

	upload, err := s.objectAdminStorageUpload(ctx, *version)
	if err != nil {
		s.logger.Error("api: failed to load object provenance upload", "error", err, "bucket", bucketName, "versionID", versionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if upload == nil {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	provenance, err := s.repos.Uploads.GetUploadProvenance(ctx, upload.ID)
	if err != nil {
		s.logger.Error("api: failed to load object provenance", "error", err, "bucket", bucketName, "versionID", versionID, "uploadID", upload.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if provenance == nil {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	resp.UploadStatus = uploadStatusString(&provenance.Upload.Status)
	resp.Progress = uploadProgressResponseFromUpload(&provenance.Upload)
	resp.Status = objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin, &provenance.Upload.Status)
	resp.PieceCID = provenance.Upload.PieceCID
	resp.RequestedCopies = provenance.Upload.RequestedCopies
	resp.UpdatedAt = provenance.Upload.UpdatedAt.Format(time.RFC3339)
	providerIDs := make([]idtypes.OnChainID, 0, len(provenance.Copies)+len(provenance.Failures))
	for _, copyRow := range provenance.Copies {
		if copyRow.ProviderID != nil {
			providerIDs = append(providerIDs, *copyRow.ProviderID)
		}
	}
	for _, failure := range provenance.Failures {
		if failure.ProviderID != nil {
			providerIDs = append(providerIDs, *failure.ProviderID)
		}
	}
	providerIdentities := s.providerIdentities(providerIDs)
	for _, copyRow := range provenance.Copies {
		if copyRow.Status == model.StorageUploadCopyStatusCommitted {
			resp.SuccessCopies++
		}
		resp.Copies = append(resp.Copies, objectProvenanceCopyResponse{
			CopyIndex:        copyRow.CopyIndex,
			Status:           string(copyRow.Status),
			ProviderID:       onChainIDStringPtr(copyRow.ProviderID),
			ProviderIdentity: providerIdentityFromSnapshotPtr(providerIdentities, copyRow.ProviderID),
			DataSetID:        onChainIDStringPtr(copyRow.DataSetID),
			PieceID:          onChainIDStringPtr(copyRow.PieceID),
			Role:             copyRow.Role,
			RetrievalURL:     copyRow.RetrievalURL,
			IsNewDataSet:     copyRow.IsNewDataSet,
		})
	}
	for _, failure := range provenance.Failures {
		resp.Failures = append(resp.Failures, objectProvenanceFailureResponse{
			AttemptIndex:     failure.AttemptIndex,
			ProviderID:       onChainIDStringPtr(failure.ProviderID),
			ProviderIdentity: providerIdentityFromSnapshotPtr(providerIdentities, failure.ProviderID),
			Role:             failure.Role,
			Stage:            failure.Stage,
			Error:            failure.ErrorMessage,
		})
	}
	writeJSON(w, http.StatusOK, resp)
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
	versionRows := make([]model.ObjectVersion, 0, len(versions))
	for _, v := range versions {
		versionRows = append(versionRows, v.ObjectVersion)
	}
	uploadInfos, err := s.objectAdminUploadInfos(ctx, versionRows)
	if err != nil {
		s.logger.Error("api: failed to load object version upload statuses", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	items := make([]objectVersionListItem, 0, len(versions))
	for _, v := range versions {
		uploadInfo := uploadInfos[v.VersionID]
		items = append(items, objectVersionListItem{
			VersionID:    v.VersionID,
			Key:          v.Key,
			Size:         v.Size,
			State:        string(v.State),
			Status:       objectAdminStatusWithUpload(v.State, v.InCache, v.InFilecoin, uploadInfo.Status),
			UploadStatus: uploadStatusString(uploadInfo.Status),
			Progress:     uploadInfo.Progress,
			Location:     objectLocation{Cache: v.InCache, Filecoin: v.InFilecoin},
			ContentType:  v.ContentType,
			ETag:         v.ETag,
			PieceCID:     v.PieceCID,
			CreatedAt:    v.CreatedAt.Format(time.RFC3339),
			UpdatedAt:    v.UpdatedAt.Format(time.RFC3339),
			IsCurrent:    v.IsCurrent,
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
