package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/objectkey"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagecleanup"
	taskengine "github.com/strahe/synaps3/internal/task"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/versity/versitygw/auth"
	"golang.org/x/sync/errgroup"
)

const (
	internalRootOwnerAccessKey = "__internal_root__"

	objectAdminStatusUnavailable = "unavailable"
	objectAdminStatusUploading   = "uploading"
	objectAdminStatusSyncing     = "syncing"
	objectAdminStatusSuccess     = "success"
	objectAdminStatusWarning     = "warning"

	permanentDeleteCacheCleanupTimeout     = 30 * time.Second
	permanentDeleteCacheCleanupConcurrency = 10
)

type bucketListItem struct {
	ID                   int64                              `json:"id"`
	Name                 string                             `json:"name"`
	OwnerAccessKey       *string                            `json:"owner_access_key"`
	DefaultCopies        int                                `json:"default_copies"`
	MinimumDurableCopies int                                `json:"minimum_durable_copies"`
	Status               string                             `json:"status"`
	ObjectCount          int64                              `json:"object_count"`
	TotalSizeBytes       int64                              `json:"total_size_bytes"`
	StorageHealth        bucketStorageHealthSummaryResponse `json:"storage_health"`
	CreatedAt            string                             `json:"created_at"`
}

type bucketCreateRequest struct {
	Name                 string `json:"name"`
	OwnerAccessKey       string `json:"owner_access_key"`
	DefaultCopies        *int   `json:"default_copies"`
	MinimumDurableCopies *int   `json:"minimum_durable_copies"`
}

type bucketMutationResponse struct {
	ID                   int64   `json:"id"`
	Name                 string  `json:"name"`
	OwnerAccessKey       *string `json:"owner_access_key"`
	DefaultCopies        int     `json:"default_copies"`
	MinimumDurableCopies int     `json:"minimum_durable_copies"`
	Status               string  `json:"status"`
}

type bucketDetailResponse struct {
	ID                   int64                              `json:"id"`
	Name                 string                             `json:"name"`
	OwnerAccessKey       *string                            `json:"owner_access_key"`
	DefaultCopies        int                                `json:"default_copies"`
	MinimumDurableCopies int                                `json:"minimum_durable_copies"`
	Status               string                             `json:"status"`
	ObjectCount          int64                              `json:"object_count"`
	TotalSizeBytes       int64                              `json:"total_size_bytes"`
	StorageHealth        bucketStorageHealthSummaryResponse `json:"storage_health"`
	CreatedAt            string                             `json:"created_at"`
	UpdatedAt            string                             `json:"updated_at"`
	VersioningStatus     string                             `json:"versioning_status"`
	VersioningEnforced   bool                               `json:"versioning_enforced"`
	DataSets             []storageDataSetSummaryResponse    `json:"data_sets"`
	// Replacements is the bucket's full history, newest first.
	Replacements []providerReplacementResponse `json:"replacements"`
}

type storageDataSetSummaryResponse struct {
	ID                 int64                     `json:"id"`
	BucketID           int64                     `json:"bucket_id"`
	BucketName         string                    `json:"bucket_name,omitempty"`
	CopyIndex          int                       `json:"copy_index"`
	Generation         int64                     `json:"generation"`
	IsCurrent          bool                      `json:"is_current"`
	Replaceable        bool                      `json:"replaceable"`
	ProviderID         string                    `json:"provider_id"`
	ProviderIdentity   *providerIdentityResponse `json:"provider_identity,omitempty"`
	DataSetID          *string                   `json:"data_set_id,omitempty"`
	ClientDataSetID    *string                   `json:"client_data_set_id,omitempty"`
	Status             string                    `json:"status"`
	CreatedByContentID *int64                    `json:"created_by_content_id,omitempty"`
	LastUsedContentID  *int64                    `json:"last_used_content_id,omitempty"`
	CommittedCopies    int64                     `json:"committed_copies"`
	ReadableCopies     int64                     `json:"readable_copies"`
	PhysicalBytes      int64                     `json:"physical_bytes"`
	ReferencedVersions int64                     `json:"referenced_version_count"`
	CurrentVersions    int64                     `json:"current_version_count"`
	CreatedAt          string                    `json:"created_at"`
	UpdatedAt          string                    `json:"updated_at"`
	StorageHealth      *dataSetStorageHealthInfo `json:"storage_health,omitempty"`
}

type dataSetStorageHealthInfo struct {
	Status           string                     `json:"status"`
	ReasonCodes      []observability.ReasonCode `json:"reason_codes"`
	ActivePieceCount *int64                     `json:"active_piece_count,omitempty"`
	HasActivePieces  *bool                      `json:"has_active_pieces,omitempty"`
	LastCheckedAt    string                     `json:"last_checked_at,omitempty"`
	LastError        *string                    `json:"last_error,omitempty"`
	Stale            bool                       `json:"stale"`
}

type bucketOwnerUpdateRequest struct {
	OwnerAccessKey string `json:"owner_access_key"`
}

type bucketCopyPolicyUpdateRequest struct {
	DefaultCopies        json.RawMessage `json:"default_copies"`
	MinimumDurableCopies json.RawMessage `json:"minimum_durable_copies"`
}

func boundedBucketCopies(copies int) int {
	return model.ClampStorageCopies(copies)
}

func validateBucketDefaultCopies(copies *int) error {
	if copies == nil {
		return nil
	}
	if !model.ValidStorageCopies(*copies) {
		return fmt.Errorf("default_copies must be between %d and %d", model.StorageCopiesMin, model.StorageCopiesMax)
	}
	return nil
}

func validateBucketMinimumDurableCopies(copies *int) error {
	if copies == nil {
		return nil
	}
	if !model.ValidStorageCopies(*copies) {
		return fmt.Errorf("minimum_durable_copies must be between %d and %d", model.StorageCopiesMin, model.StorageCopiesMax)
	}
	return nil
}

func parseBucketCopyPolicyValue(raw json.RawMessage, field string) (*int, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, nil
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, true, fmt.Errorf("%s must be an integer or null", field)
	}
	if !model.ValidStorageCopies(value) {
		return nil, true, fmt.Errorf("%s must be between %d and %d", field, model.StorageCopiesMin, model.StorageCopiesMax)
	}
	return &value, true, nil
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
	storageHealthMap, storageHealthFailed := s.bucketStorageHealthSummaries(ctx, 0)

	items := make([]bucketListItem, 0, len(buckets))
	for _, b := range buckets {
		if !b.Status.IsAdminVisible() {
			continue
		}
		stats := statsMap[b.ID]
		items = append(items, bucketListItem{
			ID:                   b.ID,
			Name:                 b.Name,
			OwnerAccessKey:       s.adminOwnerAccessKey(b.OwnerAccessKey),
			DefaultCopies:        b.DefaultCopies,
			MinimumDurableCopies: b.MinimumDurableCopies,
			Status:               string(b.Status),
			ObjectCount:          stats.Count,
			TotalSizeBytes:       stats.TotalSize,
			StorageHealth:        bucketStorageHealthSummaryForBucket(storageHealthMap, b.ID, storageHealthFailed),
			CreatedAt:            b.CreatedAt.Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, items)
}

// bucketNameRe matches valid S3-compatible bucket names (3-63 chars, lowercase
// alphanumeric and hyphens, no leading/trailing hyphen).
var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func (s *Server) handleAPICreateBucket(w http.ResponseWriter, r *http.Request) {
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
	if err := validateBucketDefaultCopies(req.DefaultCopies); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := validateBucketMinimumDurableCopies(req.MinimumDurableCopies); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	targetCopies := s.filecoinDefaultCopies
	if req.DefaultCopies != nil {
		targetCopies = *req.DefaultCopies
	}
	if req.MinimumDurableCopies != nil && *req.MinimumDurableCopies > targetCopies {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "minimum_durable_copies cannot exceed the effective replica target"})
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

	bucket, err := s.bucketLifecycle.CreateWithOptions(r.Context(), bucketlifecycle.CreateOptions{
		Name:                 name,
		ACL:                  acl,
		OwnerAccessKey:       &actualOwnerAccessKey,
		DefaultCopies:        req.DefaultCopies,
		MinimumDurableCopies: req.MinimumDurableCopies,
	})
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "bucket already exists"})
			return
		}
		if errors.Is(err, bucketlifecycle.ErrOwnerNotFound) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "S3 owner not found"})
			return
		}
		s.logger.Error("api: failed to create bucket", "error", err, "name", name)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusCreated, bucketMutationResponse{
		ID:                   bucket.ID,
		Name:                 bucket.Name,
		OwnerAccessKey:       s.adminOwnerAccessKey(bucket.OwnerAccessKey),
		DefaultCopies:        bucket.DefaultCopies,
		MinimumDurableCopies: bucket.MinimumDurableCopies,
		Status:               string(bucket.Status),
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

	stats, err := s.repos.Objects.BucketStats(ctx, bucket.ID)
	if err != nil {
		s.logger.Error("api: failed to get bucket object stats", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	dataSets := make([]storageDataSetSummaryResponse, 0)
	if s.repos.Contents != nil {
		summaries, err := s.repos.Contents.ListDataSetSummaries(ctx, bucket.ID)
		if err != nil {
			s.logger.Error("api: failed to list bucket storage data sets", "error", err, "name", bucketName)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		dataSets = s.storageDataSetSummaryResponses(ctx, summaries)
	}
	storageHealthMap, storageHealthFailed := s.bucketStorageHealthSummaries(ctx, bucket.ID)

	writeJSON(w, http.StatusOK, bucketDetailResponse{
		ID:                   bucket.ID,
		Name:                 bucket.Name,
		OwnerAccessKey:       s.adminOwnerAccessKey(bucket.OwnerAccessKey),
		DefaultCopies:        bucket.DefaultCopies,
		MinimumDurableCopies: bucket.MinimumDurableCopies,
		Status:               string(bucket.Status),
		ObjectCount:          stats.Count,
		TotalSizeBytes:       stats.TotalSize,
		StorageHealth:        bucketStorageHealthSummaryForBucket(storageHealthMap, bucket.ID, storageHealthFailed),
		CreatedAt:            bucket.CreatedAt.Format(time.RFC3339),
		UpdatedAt:            bucket.UpdatedAt.Format(time.RFC3339),
		VersioningStatus:     "Enabled",
		VersioningEnforced:   true,
		DataSets:             dataSets,
		Replacements:         s.bucketReplacementResponses(ctx, bucket.Name, bucket.ID),
	})
}

func (s *Server) handleAPIUpdateBucketOwner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
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
		ID:                   bucket.ID,
		Name:                 bucket.Name,
		OwnerAccessKey:       s.adminOwnerAccessKey(&actualOwnerAccessKey),
		DefaultCopies:        bucket.DefaultCopies,
		MinimumDurableCopies: bucket.MinimumDurableCopies,
		Status:               string(bucket.Status),
	})
}

func (s *Server) handleAPIUpdateBucketCopyPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	var req bucketCopyPolicyUpdateRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	defaultCopies, setDefaultCopies, err := parseBucketCopyPolicyValue(req.DefaultCopies, "default_copies")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	minimumCopies, setMinimumCopies, err := parseBucketCopyPolicyValue(req.MinimumDurableCopies, "minimum_durable_copies")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// A bucket stores its policy, so an explicit null is a reset to the current
	// configured target rather than a standing inheritance.
	if setDefaultCopies && defaultCopies == nil {
		configured := s.filecoinDefaultCopies
		defaultCopies = &configured
	}
	if !setDefaultCopies && !setMinimumCopies {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "copy policy update requires at least one field"})
		return
	}

	var bucket *model.Bucket
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	err = s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		updated, err := txRepos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
			Name:                    bucketName,
			SetDefaultCopies:        setDefaultCopies,
			DefaultCopies:           defaultCopies,
			SetMinimumDurableCopies: setMinimumCopies,
			MinimumDurableCopies:    minimumCopies,
		})
		if err != nil {
			return err
		}
		if updated == nil || !updated.Status.IsAdminVisible() {
			return repository.ErrNotFound
		}
		if err := s.bucketLifecycle.ScheduleProvision(ctx, txRepos, updated); err != nil {
			return err
		}
		generation, err := txRepos.CacheEvictions.NextDurabilityGeneration(ctx, updated.ID)
		if err != nil {
			return err
		}
		taskRow, _, err := s.taskService.EnqueueInTransaction(ctx, txRepos, taskengine.EnqueueRequest{
			Type:           model.TaskTypeCacheReconcileDurability,
			IdempotencyKey: cacheeviction.DurabilityTaskKey(updated.ID, generation),
			Input:          cacheeviction.DurabilityInput{BucketID: updated.ID, Generation: generation},
			SubjectType:    "bucket", SubjectKey: strconv.FormatInt(updated.ID, 10),
		})
		if err != nil {
			return err
		}
		if err := txRepos.CacheEvictions.BindDurabilityTask(ctx, updated.ID, generation, taskRow.ID); err != nil {
			return err
		}
		bucket = updated
		return nil
	})
	if errors.Is(err, repository.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}
	if errors.Is(err, repository.ErrReplicaTargetLowered) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "lowering the replica target is not supported yet; existing replicas above the new target would keep running",
		})
		return
	}
	if errors.Is(err, repository.ErrInvalidInput) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "minimum_durable_copies cannot exceed the effective replica target"})
		return
	}
	if err != nil {
		s.logger.Error("api: failed to update bucket copy policy", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	writeJSON(w, http.StatusOK, bucketMutationResponse{
		ID:                   bucket.ID,
		Name:                 bucket.Name,
		OwnerAccessKey:       s.adminOwnerAccessKey(bucket.OwnerAccessKey),
		DefaultCopies:        bucket.DefaultCopies,
		MinimumDurableCopies: bucket.MinimumDurableCopies,
		Status:               string(bucket.Status),
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

func (s *Server) storageDataSetSummaryResponses(ctx context.Context, summaries []repository.StorageDataSetSummary) []storageDataSetSummaryResponse {
	out := make([]storageDataSetSummaryResponse, 0, len(summaries))
	providerIDs := make([]idtypes.OnChainID, 0, len(summaries))
	localIDs := make([]int64, 0, len(summaries))
	for _, summary := range summaries {
		providerIDs = append(providerIDs, summary.ProviderID)
		localIDs = append(localIDs, summary.ID)
	}
	identities := s.providerIdentities(providerIDs)
	healthByLocalID, healthFailed := s.dataSetStorageHealthStates(ctx, localIDs)
	for _, summary := range summaries {
		storageHealth := s.dataSetStorageHealthInfo(healthByLocalID[summary.ID])
		if healthFailed {
			storageHealth = dataSetStorageHealthQueryFailureInfo()
		}
		out = append(out, storageDataSetSummaryResponse{
			ID:         summary.ID,
			BucketID:   summary.BucketID,
			BucketName: summary.BucketName,
			CopyIndex:  summary.CopyIndex,
			Generation: summary.Generation,
			IsCurrent:  summary.IsCurrent,
			// Only the generation that receives writes can be replaced;
			// replacing a historical one would move nothing.
			Replaceable:        summary.IsCurrent && summary.Status != model.StorageDataSetStatusRetired,
			ProviderID:         summary.ProviderID.String(),
			ProviderIdentity:   providerIdentityFromSnapshot(identities, summary.ProviderID),
			DataSetID:          onChainIDStringPtr(summary.DataSetID),
			ClientDataSetID:    onChainIDStringPtr(summary.ClientDataSetID),
			Status:             string(summary.Status),
			CreatedByContentID: summary.CreatedByContentID,
			LastUsedContentID:  summary.LastUsedContentID,
			CommittedCopies:    summary.CommittedCopies,
			ReadableCopies:     summary.ReadableCopies,
			PhysicalBytes:      summary.PhysicalBytes,
			ReferencedVersions: summary.ReferencedVersions,
			CurrentVersions:    summary.CurrentVersions,
			CreatedAt:          summary.CreatedAt.Format(time.RFC3339),
			UpdatedAt:          summary.UpdatedAt.Format(time.RFC3339),
			StorageHealth:      storageHealth,
		})
	}
	return out
}

func (s *Server) dataSetStorageHealthStates(ctx context.Context, localIDs []int64) (map[int64]observability.DataSetObservation, bool) {
	if s.observability == nil || len(localIDs) == 0 {
		return nil, false
	}
	states, err := s.observability.DataSetObservationsByLocalIDs(ctx, localIDs)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("api: failed to enrich bucket data set storage health", "error", err)
		}
		return nil, true
	}
	return states, false
}

func (s *Server) dataSetStorageHealthInfo(observation observability.DataSetObservation) *dataSetStorageHealthInfo {
	if observation.Facts.LocalDataSetID == 0 {
		return nil
	}
	reasonCodes := make([]observability.ReasonCode, 0, len(observation.Signal.ReasonCodes))
	reasonCodes = append(reasonCodes, observation.Signal.ReasonCodes...)
	var lastCheckedAt string
	if observation.Signal.Freshness.LastCheckedAt != nil {
		lastCheckedAt = observation.Signal.Freshness.LastCheckedAt.Format(time.RFC3339)
	}
	return &dataSetStorageHealthInfo{
		Status:           string(observation.Signal.Status),
		ReasonCodes:      reasonCodes,
		ActivePieceCount: observation.Facts.ActivePieceCount,
		HasActivePieces:  observation.Facts.HasActivePieces,
		LastCheckedAt:    lastCheckedAt,
		LastError:        observation.Signal.LastError,
		Stale:            observation.Signal.Freshness.Stale,
	}
}

func dataSetStorageHealthQueryFailureInfo() *dataSetStorageHealthInfo {
	errText := "storage health query failed"
	return &dataSetStorageHealthInfo{
		Status:      string(observability.StatusUnknown),
		ReasonCodes: []observability.ReasonCode{},
		LastError:   &errText,
	}
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
	VersionID string                  `json:"version_id"`
	State     string                  `json:"state"`
	Status    string                  `json:"status"`
	Progress  *uploadProgressResponse `json:"progress,omitempty"`
	Message   *string                 `json:"message,omitempty"`
	UpdatedAt string                  `json:"updated_at"`
}

type objectProvenanceResponse struct {
	VersionID       string                         `json:"version_id"`
	State           string                         `json:"state"`
	Status          string                         `json:"status"`
	Progress        *uploadProgressResponse        `json:"progress,omitempty"`
	PieceCID        *string                        `json:"piece_cid,omitempty"`
	RequestedCopies int                            `json:"requested_copies"`
	SuccessCopies   int                            `json:"success_copies"`
	CopyHealth      copyHealthSummaryResponse      `json:"copy_health"`
	Copies          []objectProvenanceCopyResponse `json:"copies"`
	UpdatedAt       string                         `json:"updated_at"`
}

type objectProvenanceCopyResponse struct {
	CopyIndex        int                       `json:"copy_index"`
	Status           string                    `json:"status"`
	Health           copyHealthInfo            `json:"health"`
	ProviderID       *string                   `json:"provider_id,omitempty"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
	DataSetID        *string                   `json:"data_set_id,omitempty"`
	PieceID          *string                   `json:"piece_id,omitempty"`
	TransferMethod   string                    `json:"transfer_method"`
	RetrievalURL     *string                   `json:"retrieval_url,omitempty"`
	IsNewDataSet     bool                      `json:"is_new_data_set"`
	AttentionCode    *string                   `json:"attention_code,omitempty"`
	AttentionAt      *string                   `json:"attention_at,omitempty"`
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

// objectAdminStatusWithUpload derives the operator-facing status. It no longer
// takes a separate upload status: that value described the same pipeline as
// state, which is itself derived from the copy rows, so the two could only ever
// agree or be wrong.
func objectAdminStatusWithUpload(state model.ObjectState, inCache, inFilecoin bool) string {
	if state == model.ObjectStateFailed {
		return objectAdminStatusWarning
	}
	if !inCache && !inFilecoin {
		return objectAdminStatusUnavailable
	}
	switch state {
	case model.ObjectStateCached, model.ObjectStateUploading:
		return objectAdminStatusUploading
	case model.ObjectStateCommitting, model.ObjectStateReplicating:
		return objectAdminStatusSyncing
	case model.ObjectStateStored:
		return objectAdminStatusSuccess
	default:
		return objectAdminStatusUnavailable
	}
}

type objectAdminUploadInfo struct {
	Message  *string
	Progress *uploadProgressResponse
}

func (s *Server) objectAdminStorageContent(ctx context.Context, version model.ObjectVersion) (*model.StorageContent, error) {
	if s.repos.Contents == nil {
		return nil, nil
	}
	if version.ContentID != nil {
		return s.repos.Contents.GetByID(ctx, *version.ContentID)
	}
	return nil, nil
}

func (s *Server) objectAdminUploadInfo(ctx context.Context, version model.ObjectVersion) (objectAdminUploadInfo, error) {
	upload, err := s.objectAdminStorageContent(ctx, version)
	if err != nil || upload == nil {
		return objectAdminUploadInfo{}, err
	}
	ingress, err := s.repos.Contents.GetIngressCopy(ctx, upload.ID)
	if err != nil {
		return objectAdminUploadInfo{}, err
	}
	return objectAdminUploadInfo{
		Message:  uploadStatusMessage(upload),
		Progress: uploadProgressResponseFromUpload(ingress),
	}, nil
}

func (s *Server) objectAdminUploadInfos(ctx context.Context, versions []model.ObjectVersion) (map[string]objectAdminUploadInfo, error) {
	infos := make(map[string]objectAdminUploadInfo, len(versions))
	if s.repos.Contents == nil || len(versions) == 0 {
		return infos, nil
	}
	contentIDSet := make(map[int64]struct{})
	for _, version := range versions {
		if version.IsDeleteMarker {
			continue
		}
		if version.ContentID != nil {
			contentIDSet[*version.ContentID] = struct{}{}
		}
	}
	contentIDs := make([]int64, 0, len(contentIDSet))
	for contentID := range contentIDSet {
		contentIDs = append(contentIDs, contentID)
	}
	uploadsByID, err := s.repos.Contents.GetByIDs(ctx, contentIDs)
	if err != nil {
		return nil, err
	}
	for _, version := range versions {
		if version.ContentID == nil {
			continue
		}
		upload, ok := uploadsByID[*version.ContentID]
		if !ok {
			continue
		}
		ingress, err := s.repos.Contents.GetIngressCopy(ctx, upload.ID)
		if err != nil {
			return nil, err
		}
		infos[version.VersionID] = objectAdminUploadInfo{
			Message:  uploadStatusMessage(&upload),
			Progress: uploadProgressResponseFromUpload(ingress),
		}
	}
	return infos, nil
}

func uploadStatusMessage(upload *model.StorageContent) *string {
	if upload == nil {
		return nil
	}
	if upload.ErrorMessage != nil && *upload.ErrorMessage != "" {
		return upload.ErrorMessage
	}
	return nil
}

// uploadProgressResponseFromUpload reads the ingress copy, not the content:
// progress belongs to the transfer that produced it.
func uploadProgressResponseFromUpload(upload *model.StorageCopy) *uploadProgressResponse {
	if upload == nil || upload.ProgressUpdatedAt == nil || upload.IngressStoreAttempt <= 0 {
		return nil
	}
	uploaded := max(upload.IngressBytesTransferred, 0)
	total := max(upload.ContentSize, 0)
	if uploaded > total {
		uploaded = total
	}
	percent := model.UploadProgressPercent(uploaded, total)
	return &uploadProgressResponse{
		Scope:         "ingress_store",
		Attempt:       upload.IngressStoreAttempt,
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

type objectDeleteMarkerResponse struct {
	Key                   string `json:"key"`
	DeleteMarkerVersionID string `json:"delete_marker_version_id"`
	DeletedAt             string `json:"deleted_at"`
}

type deletedObjectListItem struct {
	Key                   string `json:"key"`
	DeleteMarkerVersionID string `json:"delete_marker_version_id"`
	DeletedAt             string `json:"deleted_at"`
	RestoreVersionID      string `json:"restore_version_id"`
	RestoreSize           int64  `json:"restore_size"`
	RestoreContentType    string `json:"restore_content_type"`
	RestoreETag           string `json:"restore_etag"`
}

type deletedObjectListResponse struct {
	Objects    []deletedObjectListItem `json:"objects"`
	HasMore    bool                    `json:"has_more"`
	NextMarker string                  `json:"next_marker,omitempty"`
}

type restoreObjectRequest struct {
	Key                   string `json:"key"`
	DeleteMarkerVersionID string `json:"delete_marker_version_id"`
}

type restoreObjectResponse struct {
	Key               string `json:"key"`
	RestoredVersionID string `json:"restored_version_id"`
}

type permanentDeleteObjectRequest struct {
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
}

type permanentDeleteObjectResponse struct {
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
	// CacheRelease reports what happened to the cached bytes: released when this
	// deletion removed the last reference, retained when another version still
	// names them, failed when the local file could not be removed.
	CacheRelease         string `json:"cache_release"`
	StorageCleanupTaskID *int64 `json:"storage_cleanup_task_id,omitempty"`
}

type permanentDeleteDeletedObjectRequest struct {
	Key                   string `json:"key"`
	DeleteMarkerVersionID string `json:"delete_marker_version_id"`
}

type permanentDeleteDeletedObjectResponse struct {
	Key                     string  `json:"key"`
	DeleteMarkerVersionID   string  `json:"delete_marker_version_id"`
	DataVersionsDeleted     int     `json:"data_versions_deleted"`
	DeleteMarkersDeleted    int     `json:"delete_markers_deleted"`
	CacheCleanupFailedCount int     `json:"cache_cleanup_failed_count"`
	StorageCleanupTaskIDs   []int64 `json:"storage_cleanup_task_ids"`
}

type objectDeletionListItem struct {
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
	DeletedAt string `json:"deleted_at"`
}

type objectDeletionListResponse struct {
	Deletions []objectDeletionListItem `json:"deletions"`
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
			Status:           objectAdminStatusWithUpload(o.State, o.InCache, o.InFilecoin),
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

func (s *Server) handleAPIDeleteBucketObject(w http.ResponseWriter, r *http.Request) {
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

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for object delete", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	marker, err := s.repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, key, model.NewVersionID())
	if err != nil {
		s.logger.Error("api: failed to delete bucket object", "error", err, "bucket", bucketName, "key", key)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, objectDeleteMarkerResponse{
		Key:                   marker.Key,
		DeleteMarkerVersionID: marker.VersionID,
		DeletedAt:             marker.CreatedAt.Format(time.RFC3339),
	})
}

func (s *Server) handleAPIBucketDeletedObjects(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for deleted object list", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
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

	markers, err := s.repos.Objects.ListRecoverableDeleteMarkers(ctx, bucket.ID, prefix, after, limit+1)
	if err != nil {
		s.logger.Error("api: failed to list deleted bucket objects", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	hasMore := len(markers) > limit
	if hasMore {
		markers = markers[:limit]
	}
	items := make([]deletedObjectListItem, 0, len(markers))
	for _, marker := range markers {
		items = append(items, deletedObjectListItem{
			Key:                   marker.Marker.Key,
			DeleteMarkerVersionID: marker.Marker.VersionID,
			DeletedAt:             marker.Marker.CreatedAt.Format(time.RFC3339),
			RestoreVersionID:      marker.RestoreVersion.VersionID,
			RestoreSize:           marker.RestoreVersion.Size,
			RestoreContentType:    marker.RestoreVersion.ContentType,
			RestoreETag:           marker.RestoreVersion.ETag,
		})
	}
	resp := deletedObjectListResponse{
		Objects: items,
		HasMore: hasMore,
	}
	if hasMore && len(items) > 0 {
		resp.NextMarker = items[len(items)-1].Key
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAPIPermanentDeleteBucketObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	var req permanentDeleteObjectRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	key := req.Key
	versionID := strings.TrimSpace(req.VersionID)
	if key == "" || versionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key and version_id are required"})
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for permanent delete", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	var result repository.DeleteObjectVersionResult
	err = s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		var deleteErr error
		result, deleteErr = txRepos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: bucket.ID, Key: key, VersionID: versionID,
		})
		if deleteErr != nil {
			return deleteErr
		}
		return s.bindStorageCleanupTask(ctx, txRepos, result.StorageCleanup)
	})
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "object version not found"})
		case errors.Is(err, repository.ErrPermanentDeleteStorageBusy):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "Storage work for this version is still in progress or awaiting Filecoin confirmation. Check the related task, then try again."})
		case errors.Is(err, repository.ErrConflict):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "object version cannot be permanently deleted"})
		default:
			s.logger.Error("api: failed to permanently delete object version", "error", err, "bucket", bucketName, "key", key, "versionID", versionID)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		}
		return
	}

	released := s.releaseContentCache(ctx, bucket.Name, result.ContentID, result.ContentUnreferenced)
	status := "released"
	if !released {
		status = "failed"
	} else if !result.ContentUnreferenced {
		status = "retained"
	}
	writeJSON(w, http.StatusOK, permanentDeleteObjectResponse{
		Key:                  key,
		VersionID:            versionID,
		CacheRelease:         status,
		StorageCleanupTaskID: cleanupReservationTaskID(result.StorageCleanup),
	})
}

func (s *Server) handleAPIPermanentDeleteDeletedBucketObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	var req permanentDeleteDeletedObjectRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	key := req.Key
	deleteMarkerVersionID := strings.TrimSpace(req.DeleteMarkerVersionID)
	if key == "" || deleteMarkerVersionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key and delete_marker_version_id are required"})
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for deleted object permanent delete", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	var result repository.DeleteDeletedObjectResult
	err = s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		var deleteErr error
		result, deleteErr = txRepos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
			BucketID: bucket.ID, Key: key, DeleteMarkerVersionID: deleteMarkerVersionID,
		})
		if deleteErr != nil {
			return deleteErr
		}
		for i := range result.StorageCleanups {
			if err := s.bindStorageCleanupTask(ctx, txRepos, &result.StorageCleanups[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "deleted object not found"})
		case errors.Is(err, repository.ErrPermanentDeleteStorageBusy):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "Storage work for one or more versions is still in progress or awaiting Filecoin confirmation. Check the related tasks, then try again."})
		case errors.Is(err, repository.ErrConflict):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "deleted object cannot be permanently deleted"})
		default:
			s.logger.Error("api: failed to permanently delete deleted object", "error", err, "bucket", bucketName, "key", key, "marker", deleteMarkerVersionID)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		}
		return
	}

	cacheCleanupFailedCount := s.recordDeletedObjectPermanentDeleteCacheCleanup(ctx, bucket.Name, result.DeletedVersions)
	storageCleanupTaskIDs := make([]int64, 0, len(result.StorageCleanups))
	for i := range result.StorageCleanups {
		if result.StorageCleanups[i].TaskID != nil {
			storageCleanupTaskIDs = append(storageCleanupTaskIDs, *result.StorageCleanups[i].TaskID)
		}
	}
	writeJSON(w, http.StatusOK, permanentDeleteDeletedObjectResponse{
		Key:                     result.Key,
		DeleteMarkerVersionID:   result.DeleteMarkerVersionID,
		DataVersionsDeleted:     result.DataVersionsDeleted,
		DeleteMarkersDeleted:    result.DeleteMarkersDeleted,
		CacheCleanupFailedCount: cacheCleanupFailedCount,
		StorageCleanupTaskIDs:   storageCleanupTaskIDs,
	})
}

func (s *Server) bindStorageCleanupTask(ctx context.Context, repos *repository.Repositories, cleanup *repository.StorageCleanupReservation) error {
	if cleanup == nil || cleanup.TaskID != nil {
		return nil
	}
	taskRow, _, err := s.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type:           model.TaskTypeStorageCleanup,
		IdempotencyKey: storagecleanup.TaskKey(cleanup.ContentID, cleanup.Generation),
		Input:          storagecleanup.Input{ContentID: cleanup.ContentID, Generation: cleanup.Generation},
		SubjectType:    "storage_content",
		SubjectKey:     strconv.FormatInt(cleanup.ContentID, 10),
	})
	if err != nil {
		return err
	}
	if err := repos.StorageCleanup.BindTask(ctx, cleanup.ContentID, cleanup.Generation, taskRow.ID); err != nil {
		return err
	}
	cleanup.TaskID = &taskRow.ID
	return nil
}

func cleanupReservationTaskID(cleanup *repository.StorageCleanupReservation) *int64 {
	if cleanup == nil {
		return nil
	}
	return cleanup.TaskID
}

func (s *Server) handleAPIBucketObjectDeletions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for object deletions", "error", err, "name", bucketName)
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
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	var deletions []model.ObjectDeletion
	q := s.db.NewSelect().
		Model(&deletions).
		Where("bucket_id = ?", bucket.ID).
		OrderExpr("deleted_at DESC, id DESC").
		Limit(limit).
		Offset(offset)
	if key := r.URL.Query().Get("key"); key != "" {
		q = q.Where("key = ?", key)
	}
	if err := q.Scan(ctx); err != nil {
		s.logger.Error("api: failed to list object deletions", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	items := make([]objectDeletionListItem, 0, len(deletions))
	for _, deletion := range deletions {
		items = append(items, objectDeletionListItem{
			Key:       deletion.Key,
			VersionID: deletion.VersionID,
			DeletedAt: deletion.DeletedAt.Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, objectDeletionListResponse{Deletions: items})
}

// releaseContentCache frees the cached bytes of a content payload, but only
// after the deletion that removed its last live reference. Residency is
// content-addressed, so bytes still named by another version must survive.
func (s *Server) releaseContentCache(ctx context.Context, bucketName string, contentID *int64, unreferenced bool) bool {
	if contentID == nil || !unreferenced {
		return true
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), permanentDeleteCacheCleanupTimeout)
	defer cancelCleanup()
	return objectdeletion.ReleaseContentCache(
		cleanupCtx,
		s.cache,
		s.cacheGate,
		s.cacheAccessTracker,
		s.repos.Objects,
		s.logger,
		bucketName,
		*contentID,
	)
}

func (s *Server) recordDeletedObjectPermanentDeleteCacheCleanup(ctx context.Context, bucketName string, versions []repository.DeletedObjectVersionSnapshot) int {
	var failed atomic.Int32
	var group errgroup.Group
	group.SetLimit(permanentDeleteCacheCleanupConcurrency)
	for _, version := range versions {
		group.Go(func() error {
			if !s.releaseContentCache(ctx, bucketName, version.ContentID, version.ContentUnreferenced) {
				failed.Add(1)
			}
			return nil
		})
	}
	_ = group.Wait()
	return int(failed.Load())
}

func (s *Server) handleAPIRestoreBucketObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	if !bucketNameRe.MatchString(bucketName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}

	var req restoreObjectRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	key := req.Key
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "object key is required"})
		return
	}
	deleteMarkerVersionID := strings.TrimSpace(req.DeleteMarkerVersionID)
	if deleteMarkerVersionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "delete_marker_version_id is required"})
		return
	}

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket for object restore", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	restored, err := s.repos.Objects.RestoreCurrentDeleteMarkerStack(ctx, bucket.ID, key, deleteMarkerVersionID)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrConflict):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "delete marker is no longer current"})
		case errors.Is(err, repository.ErrNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "restorable object not found"})
		default:
			s.logger.Error("api: failed to restore bucket object", "error", err, "bucket", bucketName, "key", key, "marker", deleteMarkerVersionID)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		}
		return
	}
	writeJSON(w, http.StatusOK, restoreObjectResponse{
		Key:               restored.Key,
		RestoredVersionID: restored.VersionID,
	})
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
	VersionID      string                  `json:"version_id"`
	Key            string                  `json:"key"`
	Size           int64                   `json:"size"`
	State          string                  `json:"state"`
	Status         string                  `json:"status"`
	IsDeleteMarker bool                    `json:"is_delete_marker"`
	Progress       *uploadProgressResponse `json:"progress,omitempty"`
	Location       objectLocation          `json:"location"`
	ContentType    string                  `json:"content_type"`
	ETag           string                  `json:"etag"`
	PieceCID       *string                 `json:"piece_cid,omitempty"`
	CreatedAt      string                  `json:"created_at"`
	UpdatedAt      string                  `json:"updated_at"`
	IsCurrent      bool                    `json:"is_current"`
}

type objectVersionListResponse struct {
	Versions          []objectVersionListItem `json:"versions"`
	HasMore           bool                    `json:"has_more"`
	CurrentVersionID  string                  `json:"current_version_id,omitempty"`
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

	// The stage a failure happened at is no longer recorded: position is derived
	// from the copies, and the copy's own error is what an operator can act on.
	writeJSON(w, http.StatusOK, objectStatusDetailResponse{
		VersionID: version.VersionID,
		State:     string(version.State),
		Status:    objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin),
		Progress:  uploadInfo.Progress,
		Message:   uploadInfo.Message,
		UpdatedAt: version.UpdatedAt.Format(time.RFC3339),
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
		VersionID:  version.VersionID,
		State:      string(version.State),
		Status:     objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin),
		CopyHealth: emptyCopyHealthSummary(),
		Copies:     make([]objectProvenanceCopyResponse, 0),
		UpdatedAt:  version.UpdatedAt.Format(time.RFC3339),
	}

	upload, err := s.objectAdminStorageContent(ctx, *version)
	if err != nil {
		s.logger.Error("api: failed to load object provenance upload", "error", err, "bucket", bucketName, "versionID", versionID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if upload == nil {
		resp.CopyHealth = noUploadCopyHealthSummary()
		writeJSON(w, http.StatusOK, resp)
		return
	}

	provenance, err := s.repos.Contents.GetUploadProvenance(ctx, upload.ID)
	if err != nil {
		s.logger.Error("api: failed to load object provenance", "error", err, "bucket", bucketName, "versionID", versionID, "contentID", upload.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if provenance == nil {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	readableCopies, err := s.repos.Contents.ListReadableCommittedCopies(ctx, upload.ID)
	if err != nil {
		s.logger.Error("api: failed to count readable provenance copies", "error", err, "bucket", bucketName, "versionID", versionID, "contentID", upload.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	resp.Progress = uploadProgressResponseFromUpload(provenance.IngressCopy)
	resp.Status = objectAdminStatusWithUpload(version.State, version.InCache, version.InFilecoin)
	resp.PieceCID = provenance.Upload.PieceCID
	resp.RequestedCopies = provenance.Upload.RequestedCopies
	resp.SuccessCopies = len(readableCopies)
	resp.UpdatedAt = provenance.Upload.UpdatedAt.Format(time.RFC3339)
	providerIDs := make([]idtypes.OnChainID, 0, len(provenance.Copies))
	for _, copyRow := range provenance.Copies {
		providerIDs = append(providerIDs, copyRow.ProviderID)
	}
	providerIdentities := s.providerIdentities(providerIDs)
	copyFacts := provenanceCopyHealthFacts(bucket.ID, version.VersionID, provenance.Upload, provenance.Copies)
	copyObservations, copyHealthFailed := s.copyHealthDataSetObservations(ctx, copyHealthLocalDataSetIDs(copyFacts))
	copyHealthInterval := s.copyHealthRefreshInterval()
	resp.CopyHealth = copyHealthSummaryForBucket(copyHealthSummariesByBucket(copyFacts, copyObservations, copyHealthFailed, copyHealthInterval), bucket.ID, false)
	copyHealthByIndex := make(map[int]copyHealthInfo, len(provenance.Copies))
	copyHealthNow := time.Now().UTC()
	for _, fact := range copyFacts {
		if fact.CopyIndex == nil {
			continue
		}
		copyHealthByIndex[*fact.CopyIndex] = copyHealthInfoFromSignal(copyHealthSignalFromFact(fact, copyObservations, copyHealthFailed, copyHealthInterval, copyHealthNow))
	}
	for _, copyRow := range provenance.Copies {
		providerID := copyRow.ProviderID
		var attentionAt *string
		if copyRow.CommitAttentionAt != nil {
			value := copyRow.CommitAttentionAt.Format(time.RFC3339)
			attentionAt = &value
		}
		resp.Copies = append(resp.Copies, objectProvenanceCopyResponse{
			CopyIndex:        copyRow.CopyIndex,
			Status:           string(copyRow.Status),
			Health:           copyHealthByIndex[copyRow.CopyIndex],
			ProviderID:       onChainIDStringPtr(&providerID),
			ProviderIdentity: providerIdentityFromSnapshotPtr(providerIdentities, &providerID),
			DataSetID:        onChainIDStringPtr(copyRow.DataSetID),
			PieceID:          onChainIDStringPtr(copyRow.PieceID),
			TransferMethod:   string(copyRow.TransferMethod),
			RetrievalURL:     copyRow.RetrievalURL,
			IsNewDataSet:     copyRow.IsNewDataSet,
			AttentionCode:    copyRow.CommitAttentionCode,
			AttentionAt:      attentionAt,
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
	current, err := s.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, key)
	if err != nil {
		s.logger.Error("api: failed to get current object version", "error", err, "bucket", bucketName, "key", key)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	currentVersionID := ""
	if len(versions) > 0 {
		if current != nil {
			currentVersionID = current.VersionID
		} else {
			for _, version := range versions {
				if version.IsCurrent {
					currentVersionID = version.VersionID
					break
				}
			}
			if currentVersionID == "" {
				s.logger.Warn("api: object version history has no observable current version; omitting restore token", "bucket", bucketName, "key", key)
			}
		}
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
			VersionID:      v.VersionID,
			Key:            v.Key,
			Size:           v.Size,
			State:          string(v.State),
			Status:         objectAdminStatusWithUpload(v.State, v.InCache, v.InFilecoin),
			IsDeleteMarker: v.IsDeleteMarker,
			Progress:       uploadInfo.Progress,
			Location:       objectLocation{Cache: v.InCache, Filecoin: v.InFilecoin},
			ContentType:    v.ContentType,
			ETag:           v.ETag,
			PieceCID:       v.PieceCID,
			CreatedAt:      v.CreatedAt.Format(time.RFC3339),
			UpdatedAt:      v.UpdatedAt.Format(time.RFC3339),
			IsCurrent:      v.IsCurrent,
		})
	}

	resp := objectVersionListResponse{
		Versions:         items,
		HasMore:          hasMore,
		CurrentVersionID: currentVersionID,
	}
	if hasMore && len(items) > 0 {
		resp.NextVersionMarker = items[len(items)-1].VersionID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAPIDownloadObject(w http.ResponseWriter, r *http.Request) {
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
	if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {
		s.logger.Warn("api: failed to clear object download write deadline", "error", err, "bucket", bucketName, "key", key)
	}

	reader := s.objectReader
	if reader == nil {
		reader = objectreader.New(
			s.repos,
			s.cache,
			s.objectStorage,
			s.cacheGate,
			s.cacheAccessTracker,
			s.logger,
		)
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
		case errors.Is(err, objectreader.ErrMethodNotAllowed):
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
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
