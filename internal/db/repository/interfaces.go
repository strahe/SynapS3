package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/types"
	"github.com/versity/versitygw/auth"
)

// BucketRepository defines persistence operations for Bucket entities.
type BucketRepository interface {
	Create(ctx context.Context, bucket *model.Bucket) error
	GetByName(ctx context.Context, name string) (*model.Bucket, error)
	GetByID(ctx context.Context, id int64) (*model.Bucket, error)
	GetNamesByIDs(ctx context.Context, ids []int64) (map[int64]string, error)
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
	// UpdateCopyPolicy locks and updates the independently optional bucket policy fields.
	UpdateCopyPolicy(ctx context.Context, input UpdateBucketCopyPolicyInput) (*model.Bucket, error)
	// SetDefaultCopies stores the bucket target override. Nil means inherit.
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

// UpdateBucketCopyPolicyInput distinguishes omitted fields from explicit nulls.
type UpdateBucketCopyPolicyInput struct {
	Name                    string
	SetDefaultCopies        bool
	DefaultCopies           *int
	SetMinimumDurableCopies bool
	MinimumDurableCopies    *int
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
	// CreateRestoredVersionAndSetCurrent creates a new current data version only
	// when the selected source still exists and the expected version is current.
	CreateRestoredVersionAndSetCurrent(ctx context.Context, version *model.ObjectVersion, sourceVersionID, expectedCurrentVersionID string) (objectID int64, err error)
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
	// RecordVersionCacheAccess advances LRU recency without changing whether
	// the local cache file is present.
	RecordVersionCacheAccess(ctx context.Context, versionID string, accessedAt time.Time) error
	// RecordVersionCacheCommit marks a newly committed local cache file present
	// and initializes or advances its LRU recency.
	RecordVersionCacheCommit(ctx context.Context, versionID string, accessedAt time.Time) error
	SetVersionStorageUploadAndTransition(ctx context.Context, versionID string, storageUploadID int64, from, to model.ObjectState) error
	FailUploadingContentFollowers(ctx context.Context, bucketID int64, size int64, checksum string, leaderVersionID string, lastError string) ([]ObjectVersionRef, error)
	ListVersionsByState(ctx context.Context, state model.ObjectState, limit int) ([]model.ObjectVersion, error)
	ListVersionsByStateAfter(ctx context.Context, state model.ObjectState, afterUpdatedAt time.Time, afterVersionID string, limit int) ([]model.ObjectVersion, error)
	ResetStaleVersionStates(ctx context.Context, fromState, toState model.ObjectState, staleBefore time.Time) (int, error)
	// CountByState returns object counts grouped by state.
	CountByState(ctx context.Context) ([]ObjectStateCount, error)
	// AggregateByState returns object counts and sizes grouped by state.
	AggregateByState(ctx context.Context) ([]ObjectStateAggregate, error)
	CountOverviewAttention(ctx context.Context) (ObjectAttentionCount, error)
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
	Generation         int                        `bun:"generation"`
	IsCurrent          bool                       `bun:"is_current"`
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

type BucketStorageHealthSummary struct {
	BucketID                   int64 `bun:"bucket_id"`
	AbnormalDataSets           int   `bun:"abnormal_data_sets"`
	AffectedVersionsCapped     int
	AffectedVersionsCap        int
	AffectedVersionsExceedsCap bool
	LocalStatusNotReady        bool `bun:"local_status_not_ready"`
	ObservationMissing         bool `bun:"observation_missing"`
	ObservationStale           bool `bun:"observation_stale"`
	ObservationUnavailable     bool `bun:"observation_unavailable"`
	ObservationDegraded        bool `bun:"observation_degraded"`
	ObservationUnknown         bool `bun:"observation_unknown"`
	ReasonCodes                []observability.ReasonCode
	LastCheckedAt              *time.Time `bun:"last_checked_at"`
}

type BucketStorageHealthAffectedVersionsInput struct {
	BucketID        int64
	LocalDataSetID  int64
	Prefix          string
	Key             string
	KeyMarker       string
	VersionIDMarker string
	CreatedAtMarker time.Time
	StaleBefore     time.Time
	Limit           int
}

type BucketStorageHealthAffectedVersionPage struct {
	Versions            []BucketStorageHealthAffectedVersion
	HasMore             bool
	NextKeyMarker       string
	NextVersionIDMarker string
	NextCreatedAtMarker time.Time
}

type BucketStorageHealthAffectedVersion struct {
	Version                  model.ObjectVersion
	RiskDataSets             []BucketStorageHealthRiskDataSet
	ReadableAlternativeCount int
}

type BucketStorageHealthRiskDataSet struct {
	LocalDataSetID     int64                      `bun:"local_data_set_id"`
	BucketID           int64                      `bun:"bucket_id"`
	CopyIndex          int                        `bun:"copy_index"`
	ProviderID         types.OnChainID            `bun:"provider_id"`
	DataSetID          *types.OnChainID           `bun:"data_set_id"`
	ClientDataSetID    *types.OnChainID           `bun:"client_data_set_id"`
	LocalStatus        model.StorageDataSetStatus `bun:"local_status"`
	ObservationStatus  *observability.Status      `bun:"observation_status"`
	ObservationMissing bool                       `bun:"observation_missing"`
	ObservationStale   bool                       `bun:"observation_stale"`
	ReasonCodes        []observability.ReasonCode `bun:"reason_codes"`
	LastCheckedAt      *time.Time                 `bun:"last_checked_at"`
	LastError          *string                    `bun:"last_error"`
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

// StorageUploadCopyID names the exact copy row to write. A task that was
// queued before the current data set generation took over must still land on
// the generation it actually stored to, so addressing is separate from the
// eligibility guards below.
//
// RequireEligibleCopy makes the write refuse a failed copy, a deleted object,
// or a copy that no longer matches, instead of reporting no rows. Coordinators
// that own one specific copy set it; ordinary upload stages stay idempotent.
type MarkUploadCopyPieceReadyInput struct {
	StorageUploadCopyID int64
	RequireEligibleCopy bool
	UploadID            int64
	CopyIndex           int
	PieceCID            string
	PieceID             *types.OnChainID
	RetrievalURL        string
}

type MarkUploadCopyCommittingInput struct {
	StorageUploadCopyID int64
	RequireEligibleCopy bool
	UploadID            int64
	CopyIndex           int
	CommitExtraDataHex  string
	CommitTransactionID string
}

// MarkUploadCopyFailedInput names the copy that failed. Without the id the
// failure resolves through the replica slot, which after an activation is a
// different generation than the one the write was bound to: the failure would
// either land on the replacement's copy or update nothing at all, leaving the
// original stuck mid-transfer and holding retirement open.
type MarkUploadCopyFailedInput struct {
	StorageUploadCopyID int64
	UploadID            int64
	CopyIndex           int
	LastError           string
}

type ResetRejectedUploadCopyCommitInput struct {
	// StorageUploadCopyID names the exact copy whose commit was rejected. Without
	// it the reset resolves through the replica slot, which after an activation
	// points at a different generation than the one that submitted the commit.
	StorageUploadCopyID int64
	UploadID            int64
	CopyIndex           int
	CommitTransactionID string
	LastError           string
}

type MarkUploadCopyCommittedInput struct {
	StorageUploadCopyID int64
	RequireEligibleCopy bool
	UploadID            int64
	CopyIndex           int
	PieceCID            string
	PieceID             *types.OnChainID
	RetrievalURL        string
	CommitExtraDataHex  string
	CommitTransactionID string
}

// AcquireReplicaRepairItemInput identifies the exact repair work already
// parsed and claimed by the upload worker.
type AcquireReplicaRepairItemInput struct {
	TaskID              int64
	TaskClaimedAt       time.Time
	StorageDataSetID    int64
	StorageUploadCopyID int64
	BucketID            int64
}

// AcquireUploadTaskInput identifies one claimed ordinary upload task and its
// optional existing storage upload.
type AcquireUploadTaskInput struct {
	TaskID        int64
	TaskClaimedAt time.Time
	UploadID      int64
	VersionID     string
}

// ReplicaRepairItem is the consistent database snapshot authorized for one
// replica repair attempt.
type ReplicaRepairItem struct {
	DataSet model.StorageDataSet
	Copy    model.StorageUploadCopy
	Upload  model.StorageUpload
	Version model.ObjectVersion
}

// IncompleteReadableUpload identifies one durable upload that still needs
// work to reach its frozen target copy count.
type IncompleteReadableUpload struct {
	Upload  model.StorageUpload
	Version model.ObjectVersion
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
	UploadID                   int64
	EnqueueAfterUploadEviction bool
	EvictionMaxRetries         int
}

// NewFinalizeUploadInput builds the shared upload-finalization contract used
// by every upload completion path.
func NewFinalizeUploadInput(
	uploadID int64,
	enqueueAfterUploadEviction bool,
	evictionMaxRetries int,
) FinalizeUploadInput {
	return FinalizeUploadInput{
		UploadID:                   uploadID,
		EnqueueAfterUploadEviction: enqueueAfterUploadEviction,
		EvictionMaxRetries:         evictionMaxRetries,
	}
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
	CountCurrentGenerationCopySlots(ctx context.Context, uploadID int64) (int, error)
	ListReadableCommittedCopies(ctx context.Context, uploadID int64) ([]ReadableStorageCopy, error)
	HasReadableCommittedCopy(ctx context.Context, uploadID int64) (bool, error)
	ListBucketStorageHealthSummaries(ctx context.Context, bucketID int64, staleBefore time.Time, affectedVersionCap int) ([]BucketStorageHealthSummary, error)
	ListBucketStorageHealthAffectedVersions(ctx context.Context, input BucketStorageHealthAffectedVersionsInput) (BucketStorageHealthAffectedVersionPage, error)
	ListDataSetBindings(ctx context.Context, bucketID int64) ([]model.StorageDataSet, error)
	ListDataSetSummaries(ctx context.Context, bucketID int64) ([]StorageDataSetSummary, error)
	GetDataSetBindingByID(ctx context.Context, id int64) (*model.StorageDataSet, error)
	GetDataSetBindingByCopyIndex(ctx context.Context, bucketID int64, copyIndex int) (*model.StorageDataSet, error)
	EnsureDataSetBinding(ctx context.Context, input EnsureDataSetBindingInput) (*model.StorageDataSet, error)
	MarkDataSetCreating(ctx context.Context, input MarkDataSetCreatingInput) error
	MarkDataSetReady(ctx context.Context, input MarkDataSetReadyInput) error
	RecoverDataSet(ctx context.Context, input MarkDataSetReadyInput) (bool, error)
	MarkDataSetDraining(ctx context.Context, id int64, lastError string) error
	MarkDataSetFailed(ctx context.Context, id int64, lastError string) error
	MarkDataSetUnavailable(ctx context.Context, id int64, lastError string) error
	DiscardFailedDataSetCandidate(ctx context.Context, uploadID int64, copyIndex int, storageDataSetID int64) (bool, error)
	CreateUploadCopiesForBindings(ctx context.Context, uploadID int64, copies []UploadCopyBindingInput) error
	GetUploadCopy(ctx context.Context, uploadID int64, copyIndex int) (*model.StorageUploadCopy, error)
	GetUploadCopyByID(ctx context.Context, id int64) (*model.StorageUploadCopy, error)
	// GetUploadCopyForDataSet addresses one concrete data set generation.
	GetUploadCopyForDataSet(ctx context.Context, uploadID, storageDataSetID int64) (*model.StorageUploadCopy, error)
	AcquireUploadTask(ctx context.Context, input AcquireUploadTaskInput) error
	AcquireReplicaRepairItem(ctx context.Context, input AcquireReplicaRepairItemInput) (*ReplicaRepairItem, error)
	NextIncompleteCopyForDataSet(ctx context.Context, storageDataSetID int64) (*model.StorageUploadCopy, error)
	NextFinalizableCopyForDataSet(ctx context.Context, storageDataSetID int64) (*model.StorageUploadCopy, error)
	ListUnavailableDataSetsWithIncompleteCopies(ctx context.Context, afterID int64, limit int) ([]model.StorageDataSet, error)
	ListIncompleteReadableUploads(ctx context.Context, afterID int64, limit int) ([]IncompleteReadableUpload, error)
	ReassignIngressCopy(ctx context.Context, uploadID int64, unavailableCopyIndex int) (*model.StorageUploadCopy, error)
	MarkUploadCopyPieceReady(ctx context.Context, input MarkUploadCopyPieceReadyInput) error
	MarkUploadCopyCommitting(ctx context.Context, input MarkUploadCopyCommittingInput) error
	ResetRejectedUploadCopyCommit(ctx context.Context, input ResetRejectedUploadCopyCommitInput) error
	MarkUploadCopyCommitted(ctx context.Context, input MarkUploadCopyCommittedInput) error
	MarkUploadCopyFailed(ctx context.Context, input MarkUploadCopyFailedInput) error
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

// StorageReplacementRepository owns operator-approved provider replacement:
// its state machine, the bounded migration cursor, and the retirement safety
// gate. Every state change is a compare-and-set so a superseded or stale caller
// is refused rather than silently applied.
type StorageReplacementRepository interface {
	// Authorize records one confirmed replacement, creates the target data set
	// generation, supersedes any earlier replacement of the same source, and
	// queues the migration coordinator, all in one transaction.
	Authorize(ctx context.Context, input AuthorizeReplacementInput) (*storagereplacement.Replacement, bool, error)
	// Retry resumes failed or cleanup-attention work on the same approved
	// target. Choosing a different provider requires a new authorization.
	Retry(ctx context.Context, input RetryReplacementInput) (*storagereplacement.Replacement, error)

	GetByID(ctx context.Context, id int64) (*storagereplacement.Replacement, error)
	GetByClientRequestID(ctx context.Context, bucketID int64, clientRequestID string) (*storagereplacement.Replacement, error)
	ListForBucket(ctx context.Context, bucketID int64, limit int) ([]storagereplacement.Replacement, error)
	GetActiveForDataSet(ctx context.Context, dataSetID int64) (*storagereplacement.Replacement, error)
	// HeldItemCopyID reports the target copy the coordinator is writing right
	// now, or zero when it holds no item.
	HeldItemCopyID(ctx context.Context, replacementID int64) (int64, error)
	// HasInProgressForDataSet reports whether recovery must leave this data set
	// alone. Terminally failed work does not count, so a stuck slot can still
	// repair in place.
	HasInProgressForDataSet(ctx context.Context, dataSetID int64) (bool, error)
	ListActive(ctx context.Context, afterID int64, limit int) ([]storagereplacement.Replacement, error)
	ListSupersededCleanupCandidates(ctx context.Context, afterID int64, limit int) ([]storagereplacement.Replacement, error)

	// Activate makes the target the write target and marks the source draining
	// in one transaction. It touches a fixed number of rows regardless of how
	// much history the bucket holds.
	Activate(ctx context.Context, replacementID int64) error
	// SeedMigrationBatch inserts one bounded batch of migration work and
	// advances the cursor. done reports that the whole history has been scanned.
	SeedMigrationBatch(ctx context.Context, replacementID int64, limit int) (inserted int, done bool, err error)
	NextExecutableItem(ctx context.Context, replacementID int64) (*storagereplacement.Item, error)
	// AcquireItem re-derives a consistent snapshot and revalidates the worker
	// claim. No provider call may start before it returns.
	AcquireItem(ctx context.Context, input AcquireReplacementItemInput) (*ReplacementItemSnapshot, error)
	AttachTargetCopy(ctx context.Context, input AttachReplacementTargetCopyInput) (*model.StorageUploadCopy, error)
	MarkItemCopied(ctx context.Context, itemID int64) error
	MarkItemWaitingSource(ctx context.Context, itemID int64, lastError string) error

	MarkMigrating(ctx context.Context, replacementID int64) error
	MarkWaiting(ctx context.Context, replacementID int64, reason storagereplacement.WaitReason) error
	MarkFailed(ctx context.Context, replacementID int64, reason *storagereplacement.FailureReason, lastError string) error
	MarkCleanupAttention(ctx context.Context, replacementID int64, lastError string) error
	BeginRetirement(ctx context.Context, replacementID int64) error
	RecordTerminationEpoch(ctx context.Context, input RecordTerminationEpochInput) error
	RecordAbandonedTerminationEpoch(ctx context.Context, input RecordTerminationEpochInput) error
	CompleteAbandonedTargetTermination(ctx context.Context, replacementID int64, observedAt time.Time) error
	// EvaluateRetirementGate reports every blocker by name so the API and UI can
	// explain why a source is still held.
	EvaluateRetirementGate(ctx context.Context, replacementID int64, observedEpoch *int64) (RetirementGate, error)
	// CompleteRetirement re-runs the whole gate inside its own transaction and
	// refuses premature completion even when called outside the worker.
	CompleteRetirement(ctx context.Context, replacementID int64, observedEpoch int64) error

	// CountAbandonedTargetSoleCopies and RetireAbandonedTarget clean up a target
	// a later confirmation replaced. They retire the opposite generation from
	// CompleteRetirement and never change the replacement record.
	CountAbandonedTargetSoleCopies(ctx context.Context, targetDataSetID int64) (int, error)
	RetireAbandonedTarget(ctx context.Context, replacementID int64) error
}

// AuthorizeReplacementInput is one operator confirmation.
type AuthorizeReplacementInput struct {
	BucketID         int64
	SourceDataSetID  int64
	SelectionMode    storagereplacement.SelectionMode
	TargetProviderID types.OnChainID
	ClientRequestID  string
	MaxRetries       int
}

type RetryReplacementInput struct {
	ReplacementID int64
	MaxRetries    int
}

type AcquireReplacementItemInput struct {
	ReplacementID int64
	ItemID        int64
	TaskID        int64
	TaskClaimedAt time.Time
}

// ReplacementItemSnapshot is the consistent view one migration item needs.
type ReplacementItemSnapshot struct {
	Replacement storagereplacement.Replacement
	Item        storagereplacement.Item
	Source      model.StorageDataSet
	Target      model.StorageDataSet
	Upload      model.StorageUpload
	Version     model.ObjectVersion
}

type AttachReplacementTargetCopyInput struct {
	ReplacementID int64
	ItemID        int64
	UploadID      int64
}

type RecordTerminationEpochInput struct {
	ReplacementID int64
	TxHash        string
	Epoch         int64
}

// RetirementGate reports each safety predicate separately so a recoverable
// block can wait while a structural one raises operator attention.
type RetirementGate struct {
	CoverageGaps     int
	SourceWrites     int
	WaitingItems     int
	SlotOwned        bool
	EpochReached     bool
	TerminationEpoch *int64
	// Blockers names the failing predicates in evaluation order.
	Blockers []string
}

// Passed reports whether every predicate is satisfied.
func (g RetirementGate) Passed() bool { return len(g.Blockers) == 0 }

// TaskRepository defines persistence operations for Task entities.
type TaskRepository interface {
	Create(ctx context.Context, task *model.Task) error
	// EnsureRecurring creates a singleton coordinator task or reactivates its
	// completed row with the supplied payload. Active, failed, exhausted, and
	// cancelled rows are left unchanged.
	EnsureRecurring(ctx context.Context, task *model.Task) (bool, error)
	// ResumeCoordinator revives a singleton coordinator on an operator's
	// request, including one that exhausted its retries or failed.
	ResumeCoordinator(ctx context.Context, task *model.Task) (bool, error)
	GetByID(ctx context.Context, id int64) (*model.Task, error)
	GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (*model.Task, error)
	HasActiveByIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error)
	HasEarlierRunningUploadCopyTask(ctx context.Context, claimedTask *model.Task, uploadID int64, copyIndex int) (bool, error)

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
	// LockRunningClaim locks the same running task claim for a cross-repository
	// transaction that must decide whether to continue or complete it.
	LockRunningClaim(ctx context.Context, task *model.Task) error
	// ContinueRunning completes one successful coordinator item by replacing
	// its version reference and payload, then returning the same task row to the queue tail.
	ContinueRunning(ctx context.Context, task *model.Task, refVersionID string, payload map[string]interface{}) error
	// ReleaseRunning releases the same running task claim back to queued without recording an error.
	ReleaseRunning(ctx context.Context, task *model.Task) error
	// CancelRunning marks the same running task claim as cancelled.
	CancelRunning(ctx context.Context, task *model.Task, message string) error
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
	MarkConfirmedWithoutTransaction(ctx context.Context, id int64) error
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

// ObjectStateAggregate holds object count and total size grouped by state.
type ObjectStateAggregate struct {
	State     string `bun:"state"`
	Count     int64  `bun:"count"`
	TotalSize int64  `bun:"total_size"`
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
