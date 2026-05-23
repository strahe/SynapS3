package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
	"github.com/versity/versitygw/auth"
)

// BucketRepository defines persistence operations for Bucket entities.
type BucketRepository interface {
	Create(ctx context.Context, bucket *model.Bucket) error
	GetByName(ctx context.Context, name string) (*model.Bucket, error)
	GetByID(ctx context.Context, id int64) (*model.Bucket, error)
	ListActive(ctx context.Context) ([]model.Bucket, error)
	// List returns all buckets regardless of status.
	List(ctx context.Context) ([]model.Bucket, error)
	// ListACLs returns the minimal bucket fields needed to derive ACL owners.
	ListACLs(ctx context.Context) ([]BucketACLSnapshot, error)
	// CountByStatus returns bucket counts grouped by status.
	CountByStatus(ctx context.Context) ([]BucketStatusCount, error)
	SoftDelete(ctx context.Context, id int64) error
	// UpdateStatus atomically transitions bucket status using CAS.
	UpdateStatus(ctx context.Context, id int64, from, to model.BucketStatus) error
	// SetACL stores the bucket ACL JSON blob used by VersityGW access control.
	SetACL(ctx context.Context, name string, acl []byte) error
	// SetOwnerAndACL stores both the authoritative owner and compatible ACL.
	SetOwnerAndACL(ctx context.Context, name string, ownerAccessKey *string, acl []byte) error
	// SetDefaultCopies stores the bucket copy policy override. Nil means inherit.
	SetDefaultCopies(ctx context.Context, name string, copies *int) error
	// CountByOwner returns bucket count for the authoritative owner access key.
	CountByOwner(ctx context.Context, ownerAccessKey string) (int, error)
	// AggregateCountsByOwner returns bucket counts grouped by authoritative owner access key.
	AggregateCountsByOwner(ctx context.Context) (map[string]int, error)
	// HardDelete permanently removes a bucket row.
	HardDelete(ctx context.Context, id int64) error
	// CountStorageDataSets returns provider-scoped data set count.
	CountStorageDataSets(ctx context.Context) (int, error)
}

// S3AccountRepository defines persistence operations for S3 IAM accounts.
type S3AccountRepository interface {
	Create(ctx context.Context, account *model.S3Account) error
	GetByAccessKey(ctx context.Context, accessKey string) (*model.S3Account, error)
	GetRoot(ctx context.Context) (*model.S3Account, error)
	ListNonRoot(ctx context.Context) ([]model.S3Account, error)
	Update(ctx context.Context, accessKey string, update S3AccountUpdate) error
	Delete(ctx context.Context, accessKey string) error
	LockByAccessKey(ctx context.Context, accessKey string) (*model.S3Account, error)
}

// S3AccountUpdate holds mutable S3 account fields.
type S3AccountUpdate struct {
	SecretKey *string
	Role      auth.Role
}

// ObjectVersionWriteResult reports whether a write created a new version or
// reused the current version.
type ObjectVersionWriteResult struct {
	ObjectID  int64
	VersionID string
	ETag      string
	Created   bool
}

// ObjectVersionListItem represents one object version row in listing queries.
type ObjectVersionListItem struct {
	model.ObjectVersion `bun:",extend"`
}

// RecoverableDeleteMarker pairs a current delete marker with the data version
// that would become current if the marker stack is restored.
type RecoverableDeleteMarker struct {
	Marker         model.ObjectVersion
	RestoreVersion model.ObjectVersion
}

// ObjectVersionRef identifies a version and its current object row.
type ObjectVersionRef struct {
	ObjectID  int64  `bun:"object_id"`
	VersionID string `bun:"version_id"`
}

type DeleteObjectVersionInput struct {
	BucketID                 int64
	Key                      string
	VersionID                string
	StorageCleanupMaxRetries *int
}

type DeleteObjectVersionResult struct {
	DeletionID           int64
	CacheKey             string
	StorageUploadID      *int64
	StorageCleanupTaskID *int64
}

type DeleteDeletedObjectInput struct {
	BucketID                 int64
	Key                      string
	DeleteMarkerVersionID    string
	StorageCleanupMaxRetries *int
}

type DeletedObjectVersionSnapshot struct {
	VersionID string
	CacheKey  string
}

type DeleteDeletedObjectResult struct {
	Key                   string
	DeleteMarkerVersionID string
	DataVersionsDeleted   int
	DeleteMarkersDeleted  int
	DeletedVersions       []DeletedObjectVersionSnapshot
	StorageCleanupTaskIDs []int64
}

// ObjectRepository defines persistence operations for object identities and versions.
type ObjectRepository interface {
	CreateVersionAndSetCurrent(ctx context.Context, version *model.ObjectVersion) (objectID int64, err error)
	CreateVersionAndSetCurrentIfChanged(ctx context.Context, version *model.ObjectVersion) (ObjectVersionWriteResult, error)
	CreateDeleteMarkerAndSetCurrent(ctx context.Context, bucketID int64, key string, versionID string) (*model.ObjectVersion, error)
	DeleteMarkerVersion(ctx context.Context, bucketID int64, key string, versionID string) error
	DeleteObjectVersionPermanently(ctx context.Context, input DeleteObjectVersionInput) (DeleteObjectVersionResult, error)
	DeleteDeletedObjectPermanently(ctx context.Context, input DeleteDeletedObjectInput) (DeleteDeletedObjectResult, error)
	UpdateObjectDeletionCacheCleanup(ctx context.Context, versionID string, status model.CacheCleanupStatus, cacheError string) error
	// RestoreCurrentDeleteMarkerStack is the admin trash restore path: it removes
	// the current delete marker stack until the latest data version becomes current,
	// unlike S3 versioned delete which removes only one specified delete marker.
	RestoreCurrentDeleteMarkerStack(ctx context.Context, bucketID int64, key string, currentMarkerVersionID string) (*model.ObjectVersion, error)
	GetObjectByID(ctx context.Context, id int64) (*model.Object, error)
	GetObjectByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error)
	GetCurrentVersionByObjectID(ctx context.Context, objectID int64) (*model.ObjectVersion, error)
	GetCurrentVersionByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.ObjectVersion, error)
	GetVersionByID(ctx context.Context, versionID string) (*model.ObjectVersion, error)
	GetVersionByBucketKeyAndID(ctx context.Context, bucketID int64, key string, versionID string) (*model.ObjectVersion, error)
	FindReusableStoredVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error)
	FindReusableReplicatingVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error)
	FindReusableActiveUploadVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error)
	ListCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.ObjectVersion, error)
	ListCurrentVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix string, fromKey string, maxKeys int) ([]model.ObjectVersion, error)
	ListVersionsByBucket(ctx context.Context, bucketID int64, prefix string, keyMarker string, versionIDMarker string, maxKeys int) ([]ObjectVersionListItem, error)
	ListVersionsByKey(ctx context.Context, bucketID int64, key string, afterVersionID string, maxKeys int) ([]ObjectVersionListItem, error)
	ListRecoverableDeleteMarkers(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]RecoverableDeleteMarker, error)
	UpdateVersionState(ctx context.Context, versionID string, from, to model.ObjectState) error
	UpdateVersionStateToFailed(ctx context.Context, versionID string, from model.ObjectState, lastError string) error
	SetVersionCachePresence(ctx context.Context, versionID string, inCache bool) error
	SetVersionStorageUploadAndTransition(ctx context.Context, versionID string, storageUploadID int64, from, to model.ObjectState) error
	FailUploadingContentFollowers(ctx context.Context, bucketID int64, size int64, checksum string, leaderVersionID string, lastError string) ([]ObjectVersionRef, error)
	ListVersionsByState(ctx context.Context, state model.ObjectState, limit int) ([]model.ObjectVersion, error)
	ListVersionsByStateAfter(ctx context.Context, state model.ObjectState, afterUpdatedAt time.Time, afterVersionID string, limit int) ([]model.ObjectVersion, error)
	ResetStaleVersionStates(ctx context.Context, fromState, toState model.ObjectState, staleBefore time.Time) (int, error)
	// CountByState returns object counts grouped by state.
	CountByState(ctx context.Context) ([]ObjectStateCount, error)
	CountOverviewAttention(ctx context.Context) (ObjectAttentionCount, error)
	// TotalSize returns the sum of current object sizes in bytes.
	TotalSize(ctx context.Context) (int64, error)
	// CountByBucket returns the number of current objects in a bucket.
	CountByBucket(ctx context.Context, bucketID int64) (int64, error)
	// TotalSizeByBucket returns the sum of current object sizes in a bucket.
	TotalSizeByBucket(ctx context.Context, bucketID int64) (int64, error)
	// BucketStats returns object count and total size for a single bucket in a single query.
	BucketStats(ctx context.Context, bucketID int64) (BucketObjectStats, error)
	// AggregateByBucket returns object count and total size for all buckets in a single query.
	AggregateByBucket(ctx context.Context) (map[int64]BucketObjectStats, error)
}

type StartObjectUploadAttemptInput struct {
	BucketID        int64
	SourceTaskID    int64
	SourceVersionID string
	ContentSize     int64
	Checksum        string
	RequestedCopies int
}

type AppendUploadFailureInput struct {
	UploadID       int64
	CopyIndex      int
	ProviderID     *types.OnChainID
	TransferMethod string
	Stage          string
	ErrorMessage   string
	Explicit       bool
}

type RecordIngressStoreProgressInput struct {
	UploadID      int64
	Attempt       int
	BytesUploaded int64
}

type StorageUploadProvenance struct {
	Upload   model.StorageUpload
	Copies   []model.StorageUploadCopy
	Failures []model.StorageUploadFailure
}

type StorageDataSetSummary struct {
	ID                 int64                      `bun:"id"`
	BucketID           int64                      `bun:"bucket_id"`
	BucketName         string                     `bun:"bucket_name"`
	CopyIndex          int                        `bun:"copy_index"`
	ProviderID         types.OnChainID            `bun:"provider_id"`
	DataSetID          *types.OnChainID           `bun:"data_set_id"`
	ClientDataSetID    *types.OnChainID           `bun:"client_data_set_id"`
	Status             model.StorageDataSetStatus `bun:"status"`
	CreatedByUploadID  *int64                     `bun:"created_by_upload_id"`
	LastUsedUploadID   *int64                     `bun:"last_used_upload_id"`
	CommittedCopies    int64                      `bun:"committed_copies"`
	ReadableCopies     int64                      `bun:"readable_copies"`
	PhysicalBytes      int64                      `bun:"physical_bytes"`
	ReferencedVersions int64                      `bun:"referenced_versions"`
	CurrentVersions    int64                      `bun:"current_versions"`
	CreatedAt          time.Time                  `bun:"created_at"`
	UpdatedAt          time.Time                  `bun:"updated_at"`
}

type ReadableStorageCopy struct {
	UploadID       int64           `bun:"upload_id"`
	PieceCID       string          `bun:"piece_cid"`
	CopyIndex      int             `bun:"copy_index"`
	ProviderID     types.OnChainID `bun:"provider_id"`
	DataSetID      types.OnChainID `bun:"data_set_id"`
	PieceID        types.OnChainID `bun:"piece_id"`
	TransferMethod string          `bun:"transfer_method"`
	RetrievalURL   string          `bun:"retrieval_url"`
}

type CurrentObjectCopyHealthSummary struct {
	BucketID                int64      `bun:"bucket_id"`
	Status                  string     `bun:"status"`
	TotalObjects            int        `bun:"total_objects"`
	UnhealthyObjects        int        `bun:"unhealthy_objects"`
	RequestedCopies         int        `bun:"requested_copies"`
	ReadableCopies          int        `bun:"readable_copies"`
	PendingCopies           int        `bun:"pending_copies"`
	FailedCopies            int        `bun:"failed_copies"`
	UnknownCopies           int        `bun:"unknown_copies"`
	CopyUnderReplicated     bool       `bun:"copy_under_replicated"`
	CopyPending             bool       `bun:"copy_pending"`
	CopyCommitting          bool       `bun:"copy_committing"`
	CopyFailed              bool       `bun:"copy_failed"`
	CopyMissingProvider     bool       `bun:"copy_missing_provider"`
	CopyMissingDataSet      bool       `bun:"copy_missing_data_set"`
	CopyMissingPiece        bool       `bun:"copy_missing_piece"`
	CopyMissingRetrievalURL bool       `bun:"copy_missing_retrieval_url"`
	CopyObservationMissing  bool       `bun:"copy_observation_missing"`
	LastCheckedAt           *time.Time `bun:"last_checked_at"`
}

type StorageCleanupRepository interface {
	ListCopiesForTask(ctx context.Context, taskID int64) ([]model.StorageCleanupCopy, error)
	MarkCopyRemoved(ctx context.Context, id int64) error
	MarkCopyDeleteScheduled(ctx context.Context, id int64, txHash string) error
	MarkCopyUnsupported(ctx context.Context, id int64, message string) error
	UploadHasObjectReferences(ctx context.Context, uploadID int64) (bool, error)
	TaskHasObjectReferences(ctx context.Context, taskID int64, uploadID int64) (bool, error)
	DeleteUploadProvenanceIfUnreferenced(ctx context.Context, uploadID int64) error
}

type EnsureDataSetBindingInput struct {
	BucketID          int64
	ProviderID        types.OnChainID
	CopyIndex         int
	CreatedByUploadID int64
}

type MarkDataSetCreatingInput struct {
	ID              int64
	UploadID        int64
	TransactionID   string
	StatusURL       string
	ClientDataSetID *types.OnChainID
}

type MarkDataSetReadyInput struct {
	ID              int64
	UploadID        int64
	DataSetID       types.OnChainID
	ClientDataSetID *types.OnChainID
}

type UploadCopyBindingInput struct {
	StorageDataSetID int64
	CopyIndex        int
	TransferMethod   model.StorageCopyTransferMethod
	ProviderID       types.OnChainID
}

type MarkUploadCopyPieceReadyInput struct {
	UploadID     int64
	CopyIndex    int
	PieceCID     string
	PieceID      *types.OnChainID
	RetrievalURL string
}

type MarkUploadCopyCommittingInput struct {
	UploadID            int64
	CopyIndex           int
	CommitExtraDataHex  string
	CommitTransactionID string
}

type MarkUploadCopyCommittedInput struct {
	UploadID            int64
	CopyIndex           int
	PieceCID            string
	PieceID             *types.OnChainID
	RetrievalURL        string
	CommitExtraDataHex  string
	CommitTransactionID string
}

type BindReadableUploadInput struct {
	UploadID    int64
	BucketID    int64
	ContentSize int64
	Checksum    string
}

type BindReadableUploadForVersionInput struct {
	UploadID    int64
	BucketID    int64
	ContentSize int64
	Checksum    string
	VersionID   string
}

type FinalizeUploadInput struct {
	UploadID int64
}

type StorageUploadRepository interface {
	StartObjectUploadAttempt(ctx context.Context, input StartObjectUploadAttemptInput) (*model.StorageUpload, error)
	FindActiveUploadBySourceVersion(ctx context.Context, versionID string) (*model.StorageUpload, error)
	FindLatestUploadBySourceVersion(ctx context.Context, versionID string) (*model.StorageUpload, error)
	FindLatestUploadsBySourceVersions(ctx context.Context, versionIDs []string) (map[string]model.StorageUpload, error)
	SetAcceptError(ctx context.Context, uploadID int64, message string) error
	GetByID(ctx context.Context, uploadID int64) (*model.StorageUpload, error)
	GetByIDs(ctx context.Context, uploadIDs []int64) (map[int64]model.StorageUpload, error)
	BeginIngressStoreProgress(ctx context.Context, uploadID int64) (*model.StorageUpload, error)
	RecordIngressStoreProgress(ctx context.Context, input RecordIngressStoreProgressInput) (*model.StorageUpload, error)
	GetUploadProvenance(ctx context.Context, uploadID int64) (*StorageUploadProvenance, error)
	AppendUploadFailure(ctx context.Context, input AppendUploadFailureInput) error
	ListCopies(ctx context.Context, uploadID int64) ([]model.StorageUploadCopy, error)
	ListReadableCommittedCopies(ctx context.Context, uploadID int64) ([]ReadableStorageCopy, error)
	ListCurrentObjectCopyHealthSummaries(ctx context.Context, bucketID int64, staleBefore time.Time) ([]CurrentObjectCopyHealthSummary, error)
	ListDataSetBindings(ctx context.Context, bucketID int64) ([]model.StorageDataSet, error)
	ListDataSetSummaries(ctx context.Context, bucketID int64) ([]StorageDataSetSummary, error)
	GetDataSetBindingByID(ctx context.Context, id int64) (*model.StorageDataSet, error)
	GetDataSetBindingByCopyIndex(ctx context.Context, bucketID int64, copyIndex int) (*model.StorageDataSet, error)
	EnsureDataSetBinding(ctx context.Context, input EnsureDataSetBindingInput) (*model.StorageDataSet, error)
	MarkDataSetCreating(ctx context.Context, input MarkDataSetCreatingInput) error
	MarkDataSetReady(ctx context.Context, input MarkDataSetReadyInput) error
	MarkDataSetDraining(ctx context.Context, id int64, lastError string) error
	MarkDataSetFailed(ctx context.Context, id int64, lastError string) error
	MarkDataSetUnavailable(ctx context.Context, id int64, lastError string) error
	DiscardFailedDataSetCandidate(ctx context.Context, uploadID int64, copyIndex int, storageDataSetID int64) error
	CreateUploadCopiesForBindings(ctx context.Context, uploadID int64, copies []UploadCopyBindingInput) error
	GetUploadCopy(ctx context.Context, uploadID int64, copyIndex int) (*model.StorageUploadCopy, error)
	MarkUploadCopyPieceReady(ctx context.Context, input MarkUploadCopyPieceReadyInput) error
	MarkUploadCopyCommitting(ctx context.Context, input MarkUploadCopyCommittingInput) error
	MarkUploadCopyCommitted(ctx context.Context, input MarkUploadCopyCommittedInput) error
	MarkUploadCopyFailed(ctx context.Context, uploadID int64, copyIndex int, lastError string) error
	BindReadableUploadForContent(ctx context.Context, input BindReadableUploadInput) ([]ObjectVersionRef, error)
	BindReadableUploadForVersion(ctx context.Context, input BindReadableUploadForVersionInput) ([]ObjectVersionRef, error)
	FinalizeUploadIfTargetCopiesMet(ctx context.Context, input FinalizeUploadInput) (bool, []ObjectVersionRef, error)
}

// BucketObjectStats holds aggregate object metrics for a single bucket.
type BucketObjectStats struct {
	Count     int64 `bun:"count"`
	TotalSize int64 `bun:"total_size"`
}

// BucketACLSnapshot holds the minimal bucket fields needed for ACL owner scans.
type BucketACLSnapshot struct {
	Name   string             `bun:"name"`
	Status model.BucketStatus `bun:"status"`
	ACL    []byte             `bun:"acl"`
}

// TaskRepository defines persistence operations for Task entities.
type TaskRepository interface {
	Create(ctx context.Context, task *model.Task) error
	GetByID(ctx context.Context, id int64) (*model.Task, error)

	// ClaimReady atomically claims one ready task of the given type by
	// transitioning it to running and setting a lease. Returns nil if no task is available.
	ClaimReady(ctx context.Context, taskType model.TaskType, leaseDuration time.Duration) (*model.Task, error)
	// RenewLease extends the same running task claim.
	RenewLease(ctx context.Context, task *model.Task, leaseDuration time.Duration) error
	// Complete marks the same running task claim as completed.
	Complete(ctx context.Context, task *model.Task) error
	// CompleteWithMessage marks the same running task claim as completed with a retained status message.
	CompleteWithMessage(ctx context.Context, task *model.Task, message string) error
	// FailRunning marks the same running task claim as non-retryably failed.
	FailRunning(ctx context.Context, task *model.Task, lastError string) error
	// ScheduleRetryRunning records a retryable failure for the same running claim
	// and returns the resulting task status.
	ScheduleRetryRunning(ctx context.Context, task *model.Task, lastError string, backoff time.Duration) (model.TaskStatus, error)
	// WaitRunning records a non-error wait and releases the running task until scheduled_at.
	WaitRunning(ctx context.Context, task *model.Task, reason model.TaskWaitReason, message string, delay time.Duration) error
	// ReleaseRunning releases the same running task claim back to queued without recording an error.
	ReleaseRunning(ctx context.Context, task *model.Task) error
	// ReleaseExpiredLeases resets running tasks whose lease has expired back to queued.
	ReleaseExpiredLeases(ctx context.Context) (int, error)
	// MarkRunningExhausted marks the same running task claim as exhausted.
	MarkRunningExhausted(ctx context.Context, task *model.Task, lastError string) error
	// ListExhausted returns exhausted tasks, ordered by most recent first.
	ListExhausted(ctx context.Context, limit int) ([]model.Task, error)
	// RetryExhausted resets an exhausted task back to queued for manual retry.
	RetryExhausted(ctx context.Context, taskID int64) error
	// CountByStatus returns task counts grouped by type and status.
	CountByStatus(ctx context.Context) ([]TaskStatusCount, error)
	CountOverviewActivePipeline(ctx context.Context) ([]TaskPipelineCount, error)
	// CountActiveObjectTasksByBucket returns active object tasks
	// whose referenced current object belongs to the given bucket.
	CountActiveObjectTasksByBucket(ctx context.Context, bucketID int64) (int64, error)
	// CountActiveBucketTasksByBucketID returns the number of active tasks
	// that directly reference the given bucket (ref_type=bucket, ref_id=bucketID).
	CountActiveBucketTasksByBucketID(ctx context.Context, bucketID int64) (int64, error)
	// CompleteByRef marks all active tasks matching the given ref as completed.
	CompleteByRef(ctx context.Context, refType string, refID int64, taskType model.TaskType) error
	// List returns tasks with optional filters, paginated by offset/limit.
	// Returns the matching tasks and the total count (for pagination).
	List(ctx context.Context, taskType string, stage string, status string, limit, offset int) ([]model.Task, int, error)
}

type WalletOperationRepository interface {
	CreateOrGet(ctx context.Context, input CreateWalletOperationInput) (*model.WalletOperation, bool, error)
	GetByID(ctx context.Context, id int64) (*model.WalletOperation, error)
	ClaimPending(ctx context.Context, leaseDuration time.Duration) (*model.WalletOperation, error)
	MarkSubmitted(ctx context.Context, id int64, txHash string) error
	MarkConfirmed(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, id int64, lastError string) error
	MarkExpiredRunningUnknown(ctx context.Context) ([]model.WalletOperation, error)
	ListSubmitted(ctx context.Context, limit int) ([]model.WalletOperation, error)
	ListRecent(ctx context.Context, limit int) ([]model.WalletOperation, error)
}

type ObservabilityRepository interface {
	ReplaceProviderStates(ctx context.Context, checkedAt time.Time, states []observability.ProviderState) error
	ListProviderStates(ctx context.Context, opts observability.ListOptions) (observability.ProviderStatePage, error)
	ReplaceDataSetStates(ctx context.Context, checkedAt time.Time, states []observability.DataSetState) error
	ListDataSetStates(ctx context.Context, opts observability.ListOptions) (observability.DataSetStatePage, error)
	GetDataSetStatesByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]observability.DataSetState, error)
}

type CreateWalletOperationInput struct {
	Type            model.WalletOperationType
	ClientRequestID string
	Amount          string
}

// TaskStatusCount holds a task count grouped by type and status.
type TaskStatusCount struct {
	Type   string `bun:"type"`
	Status string `bun:"status"`
	Count  int64  `bun:"count"`
}

type TaskPipelineCount struct {
	Pipeline string `bun:"pipeline"`
	Status   string `bun:"status"`
	Count    int64  `bun:"count"`
}

// ObjectStateCount holds an object count grouped by state.
type ObjectStateCount struct {
	State string `bun:"state"`
	Count int64  `bun:"count"`
}

type ObjectAttentionCount struct {
	NeedsAttention int64 `bun:"needs_attention"`
	Unavailable    int64 `bun:"unavailable"`
}

// BucketStatusCount holds a bucket count grouped by status.
type BucketStatusCount struct {
	Status string `bun:"status"`
	Count  int64  `bun:"count"`
}

// MultipartUploadRepository defines persistence operations for multipart upload entities.
type MultipartUploadRepository interface {
	Create(ctx context.Context, upload *model.MultipartUpload) error
	GetByUploadID(ctx context.Context, uploadID string) (*model.MultipartUpload, error)
	ListByBucket(ctx context.Context, bucketID int64, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]model.MultipartUpload, error)
	// CountActiveByBucket returns initiated/completing multipart uploads for the given bucket.
	CountActiveByBucket(ctx context.Context, bucketID int64) (int64, error)
	// SetStatus atomically transitions status using CAS (compare-and-swap) to prevent races.
	SetStatus(ctx context.Context, uploadID string, from, to model.MultipartStatus) error
	Delete(ctx context.Context, uploadID string) error

	// Part operations
	CreatePart(ctx context.Context, part *model.MultipartPart) error
	GetParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]model.MultipartPart, error)
	GetPartsByNumbers(ctx context.Context, uploadID string, numbers []int) ([]model.MultipartPart, error)
	DeleteParts(ctx context.Context, uploadID string) error
}
