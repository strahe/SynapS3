package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagecommit"
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
	// PromoteReadyIfProvisioned marks a provisioning bucket ready once enough current data sets are ready.
	PromoteReadyIfProvisioned(ctx context.Context, id int64, requiredDataSets int) (bool, error)
	// SetACL stores the bucket ACL JSON blob used by VersityGW access control.
	SetACL(ctx context.Context, name string, acl []byte) error
	// SetOwnerAndACL stores both the authoritative owner and compatible ACL.
	SetOwnerAndACL(ctx context.Context, name string, ownerAccessKey *string, acl []byte) error
	// UpdateCopyPolicy locks and updates the independently optional bucket policy fields.
	UpdateCopyPolicy(ctx context.Context, input UpdateBucketCopyPolicyInput) (*model.Bucket, error)
	// SetDefaultCopies stores the bucket replica target. Nil resets it to the
	// caller-supplied configured default.
	SetDefaultCopies(ctx context.Context, name string, copies *int) error
	// ActiveReplicaSlots returns the ascending copy indexes the bucket still
	// accepts new writes on. Decommissioned slots keep their rows for history
	// but never take a new data set.
	ActiveReplicaSlots(ctx context.Context, bucketID int64) ([]int, error)
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
	Name      *string
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
	ContentID *int64 `bun:"content_id"`
}

type DeleteObjectVersionInput struct {
	BucketID  int64
	Key       string
	VersionID string
}

type StorageCleanupReservation struct {
	ContentID  int64
	Generation int64
	TaskID     *int64
}

type DeleteObjectVersionResult struct {
	DeletionID     int64
	ContentID      *int64
	StorageCleanup *StorageCleanupReservation
}

type DeleteDeletedObjectInput struct {
	BucketID              int64
	Key                   string
	DeleteMarkerVersionID string
}

type DeletedObjectVersionSnapshot struct {
	VersionID string
	ContentID *int64
}

type DeleteDeletedObjectResult struct {
	Key                   string
	DeleteMarkerVersionID string
	DataVersionsDeleted   int
	DeleteMarkersDeleted  int
	DeletedVersions       []DeletedObjectVersionSnapshot
	StorageCleanups       []StorageCleanupReservation
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
	// ClearContentCachePresence records that a content payload no longer has
	// cached bytes on this node.
	ClearContentCachePresence(ctx context.Context, contentID int64) error
	// ReleaseContentCacheIfUnreferenced locks the content row, rechecks live
	// references, and invokes release before clearing cache presence in the
	// same transaction. The callback must be idempotent.
	ReleaseContentCacheIfUnreferenced(ctx context.Context, contentID int64, release func() error) (bool, error)
	// ContentIsUnreferenced reports whether any live object version still
	// points at the content.
	ContentIsUnreferenced(ctx context.Context, contentID int64) (bool, error)
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
	ListCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.ObjectVersion, error)
	ListCurrentVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix string, fromKey string, maxKeys int) ([]model.ObjectVersion, error)
	ListVersionsByBucket(ctx context.Context, bucketID int64, prefix string, keyMarker string, versionIDMarker string, maxKeys int) ([]ObjectVersionListItem, error)
	ListVersionsByKey(ctx context.Context, bucketID int64, key string, afterVersionID string, maxKeys int) ([]ObjectVersionListItem, error)
	ListRecoverableDeleteMarkers(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]RecoverableDeleteMarker, error)
	SetVersionCachePresence(ctx context.Context, versionID string, inCache bool) error
	// RecordContentCacheAccess advances LRU recency for one content payload
	// without changing whether its bytes are present.
	RecordContentCacheAccess(ctx context.Context, contentID int64, accessedAt time.Time) error
	// RecordContentCacheCommit marks a newly written cache file present and
	// advances its recency in the same write.
	RecordContentCacheCommit(ctx context.Context, contentID int64, accessedAt time.Time) error
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

type EnsureContentInput struct {
	BucketID        int64
	ContentSize     int64
	Checksum        string
	RequestedCopies int
}

type BeginIngressStoreProgressInput struct {
	CopyID     int64
	Generation int64
	TaskID     int64
	Attempt    int
}

type RecordIngressStoreProgressInput struct {
	CopyID        int64
	Generation    int64
	TaskID        int64
	Attempt       int
	BytesUploaded int64
}

type StorageContentProvenance struct {
	Upload model.StorageContent
	Copies []model.StorageCopy
	// IngressCopy is the copy that performed the ingress transfer, when one
	// exists. Progress belongs to that transfer rather than to the content.
	IngressCopy *model.StorageCopy
}

type StorageDataSetSummary struct {
	ID                 int64                      `bun:"id"`
	BucketID           int64                      `bun:"bucket_id"`
	BucketName         string                     `bun:"bucket_name"`
	CopyIndex          int                        `bun:"copy_index"`
	Generation         int64                      `bun:"generation"`
	IsCurrent          bool                       `bun:"is_current"`
	ProviderID         types.OnChainID            `bun:"provider_id"`
	DataSetID          *types.OnChainID           `bun:"data_set_id"`
	ClientDataSetID    *types.OnChainID           `bun:"client_data_set_id"`
	Status             model.StorageDataSetStatus `bun:"status"`
	CreatedByContentID *int64                     `bun:"created_by_content_id"`
	LastUsedContentID  *int64                     `bun:"last_used_content_id"`
	CommittedCopies    int64                      `bun:"committed_copies"`
	ReadableCopies     int64                      `bun:"readable_copies"`
	PhysicalBytes      int64                      `bun:"physical_bytes"`
	ReferencedVersions int64                      `bun:"referenced_versions"`
	CurrentVersions    int64                      `bun:"current_versions"`
	CreatedAt          time.Time                  `bun:"created_at"`
	UpdatedAt          time.Time                  `bun:"updated_at"`
}

type ReadableStorageCopy struct {
	ContentID      int64           `bun:"content_id"`
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
	BindTask(ctx context.Context, contentID, generation, taskID int64) error
	AuthorizeTask(ctx context.Context, contentID, generation, taskID int64) ([]model.StorageCleanupCopy, error)
	MarkCopyRemoved(ctx context.Context, id int64) error
	MarkCopyDeleteScheduled(ctx context.Context, id int64, txHash string) error
	BeginFailedCopyRetry(ctx context.Context, id int64, oldHash string) error
	MarkCopyFailed(ctx context.Context, id int64, message string) error
	MarkCopyUnsupported(ctx context.Context, id int64, message string) error
	UploadHasObjectReferences(ctx context.Context, contentID int64) (bool, error)
	CleanupHasObjectReferences(ctx context.Context, contentID int64) (bool, error)
	// FinalizeContent deletes a cleaned-up content's current-state rows: its
	// cache record, its copies, and the content row. Ledgers keep theirs.
	FinalizeContent(ctx context.Context, contentID, generation, taskID int64) error
}

type EnsureDataSetBindingInput struct {
	BucketID           int64
	ProviderID         types.OnChainID
	CopyIndex          int
	CreatedByContentID int64
}

type MarkDataSetCreatingInput struct {
	ID              int64
	ContentID       int64
	TransactionID   string
	StatusURL       string
	ClientDataSetID *types.OnChainID
}

type MarkDataSetReadyInput struct {
	ID              int64
	ContentID       int64
	DataSetID       types.OnChainID
	ClientDataSetID *types.OnChainID
}

type BackfillClientDataSetIDInput struct {
	ID              int64
	DataSetID       types.OnChainID
	ClientDataSetID types.OnChainID
}

type UploadCopyBindingInput struct {
	StorageDataSetID int64
	CopyIndex        int
	TransferMethod   model.StorageCopyTransferMethod
	ProviderID       types.OnChainID
}

// ReservePullRequestInput records one provider-side copy request before it is
// sent. AttemptID is the ledger row's identity and SourcePieceCID names the
// piece being fetched, which is not the same as the content's own CID.
type ReservePullRequestInput struct {
	CopyID     int64
	Generation int64
	TaskID     int64
	AttemptID  string
	// On-chain identity is optional at the type level because zero is a legal
	// value: the first piece of a data set is piece 0. Presence is nil-checked.
	SourceProviderID   *types.OnChainID
	SourceDataSetID    *types.OnChainID
	SourcePieceID      *types.OnChainID
	SourcePieceCID     string
	SourceRetrievalURL string
	CommitExtraDataHex string
}

// StorageCopyID names the exact copy row to write. A task that was
// queued before the current data set generation took over must still land on
// the generation it actually stored to, so addressing is separate from the
// eligibility guards below.
//
// RequireEligibleCopy makes the write refuse a failed copy, a deleted object,
// or a copy that no longer matches, instead of reporting no rows. Coordinators
// that own one specific copy set it; ordinary upload stages stay idempotent.
type MarkUploadCopyPieceReadyInput struct {
	StorageCopyID       int64
	RequireEligibleCopy bool
	ContentID           int64
	CopyIndex           int
	PieceCID            string
	PieceID             *types.OnChainID
	RetrievalURL        string
	CommitExtraDataHex  string
	// PullAttemptID resolves the pull ledger row in the same transaction that
	// records the piece. Leaving it empty is how an ingress store settles, since
	// no request was sent to a source provider.
	PullAttemptID string
}

// MarkUploadCopyFailedInput names the copy that failed. Without the id the
// failure resolves through the replica slot, which after an activation is a
// different generation than the one the write was bound to: the failure would
// either land on the replacement's copy or update nothing at all, leaving the
// original stuck mid-transfer and holding retirement open.
type MarkUploadCopyFailedInput struct {
	StorageCopyID int64
	ContentID     int64
	CopyIndex     int
	LastError     string
	// PullAttemptID abandons the unresolved provider pull in the same
	// transaction as the copy failure. Empty means this was not a pull failure.
	PullAttemptID string
}

// MarkUploadCopyCommittedInput requires CommitAttemptID: a committed copy is a
// projection of a confirmed ledger row, and the schema refuses one without it.
type MarkUploadCopyCommittedInput struct {
	StorageCopyID                int64
	RequireEligibleCopy          bool
	ContentID                    int64
	CopyIndex                    int
	PieceCID                     string
	PieceID                      *types.OnChainID
	RetrievalURL                 string
	CommitExtraDataHex           string
	CommitTransactionID          string
	CommitAttemptID              string
	CommitConfirmedTransactionID string
}

type BindReadableUploadInput struct {
	ContentID int64
	BucketID  int64
}

type BindReadableUploadForVersionInput struct {
	ContentID int64
	BucketID  int64
	VersionID string
}

type FinalizeUploadInput struct {
	ContentID int64
}

func NewFinalizeUploadInput(contentID int64) FinalizeUploadInput {
	return FinalizeUploadInput{ContentID: contentID}
}

type StorageContentRepository interface {
	EnsureContent(ctx context.Context, input EnsureContentInput) (*model.StorageContent, error)
	GetByID(ctx context.Context, contentID int64) (*model.StorageContent, error)
	GetByIDs(ctx context.Context, contentIDs []int64) (map[int64]model.StorageContent, error)
	GetIngressCopy(ctx context.Context, contentID int64) (*model.StorageCopy, error)
	// ContentPipelineState derives pipeline position from the copy rows.
	ContentPipelineState(ctx context.Context, contentID int64) (model.ObjectState, error)
	BeginIngressStoreProgress(ctx context.Context, input BeginIngressStoreProgressInput) (*model.StorageCopy, error)
	RecordIngressStoreProgress(ctx context.Context, input RecordIngressStoreProgressInput) (*model.StorageCopy, error)
	GetUploadProvenance(ctx context.Context, contentID int64) (*StorageContentProvenance, error)
	ListCopies(ctx context.Context, contentID int64) ([]model.StorageCopy, error)
	CountCurrentGenerationCopySlots(ctx context.Context, contentID int64) (int, error)
	ListReadableCommittedCopies(ctx context.Context, contentID int64) ([]ReadableStorageCopy, error)
	IsPendingReplacementCopy(ctx context.Context, copyID int64) (bool, error)
	HasReadableCommittedCopy(ctx context.Context, contentID int64) (bool, error)
	ListBucketStorageHealthSummaries(ctx context.Context, bucketID int64, staleBefore time.Time, affectedVersionCap int) ([]BucketStorageHealthSummary, error)
	ListBucketStorageHealthAffectedVersions(ctx context.Context, input BucketStorageHealthAffectedVersionsInput) (BucketStorageHealthAffectedVersionPage, error)
	ListDataSetBindings(ctx context.Context, bucketID int64) ([]model.StorageDataSet, error)
	ListReadyBucketProviderIDs(ctx context.Context) ([]string, error)
	ListDataSetSummaries(ctx context.Context, bucketID int64) ([]StorageDataSetSummary, error)
	GetDataSetBindingByID(ctx context.Context, id int64) (*model.StorageDataSet, error)
	GetDataSetBindingByCopyIndex(ctx context.Context, bucketID int64, copyIndex int) (*model.StorageDataSet, error)
	EnsureDataSetBinding(ctx context.Context, input EnsureDataSetBindingInput) (*model.StorageDataSet, error)
	MarkDataSetCreating(ctx context.Context, input MarkDataSetCreatingInput) error
	// RecordDataSetClientID ties a generation to the client data set ID of its
	// create request before the request is sent. The ID never changes later.
	RecordDataSetClientID(ctx context.Context, id int64, clientDataSetID types.OnChainID) error
	MarkDataSetReady(ctx context.Context, input MarkDataSetReadyInput) error
	BackfillClientDataSetID(ctx context.Context, input BackfillClientDataSetIDInput) error
	MarkDataSetDraining(ctx context.Context, id int64, lastError string) error
	MarkDataSetFailed(ctx context.Context, id int64, lastError string) error
	// RetireRejectedDataSet ends a failed generation whose creation the chain
	// refused, so its provider stops being reserved. The caller must have proof
	// that no data set was created. Reports whether it was retired.
	RetireRejectedDataSet(ctx context.Context, storageDataSetID int64) (bool, error)
	CreateUploadCopiesForBindings(ctx context.Context, contentID int64, copies []UploadCopyBindingInput) error
	GetUploadCopy(ctx context.Context, contentID int64, copyIndex int) (*model.StorageCopy, error)
	GetUploadCopyByID(ctx context.Context, id int64) (*model.StorageCopy, error)
	GetLiveVersionForUpload(ctx context.Context, contentID int64) (*model.ObjectVersion, error)
	ListIncompleteCopiesForDataSet(ctx context.Context, storageDataSetID int64) ([]model.StorageCopy, error)
	BindDataSetEnsureTask(ctx context.Context, dataSetID, taskID int64) error
	AuthorizeDataSetEnsureTask(ctx context.Context, dataSetID, taskID int64) (*model.StorageDataSet, error)
	CompleteDataSetEnsureTask(ctx context.Context, dataSetID, taskID int64) error
	NextCopyWorkGeneration(ctx context.Context, copyID int64) (int64, error)
	BindCopyTask(ctx context.Context, copyID, generation, taskID int64) error
	AuthorizeCopyTask(ctx context.Context, copyID, generation, taskID, claimGeneration int64) (*model.StorageCopy, error)
	ReservePullRequest(ctx context.Context, input ReservePullRequestInput) error
	SetCopyCacheRestore(ctx context.Context, copyID, generation, taskID int64, pullAttemptID string) error
	AbandonMigrationPull(ctx context.Context, copyID, generation, taskID int64, pullAttemptID string) error
	PromotePendingIngress(ctx context.Context, contentID int64) (*model.StorageCopy, error)
	ReopenFailedIngressForPull(ctx context.Context, contentID int64) ([]model.StorageCopy, error)
	ReplaceCopyTask(ctx context.Context, copyID, generation, taskID, nextGeneration, nextTaskID int64) error
	CompleteCopyTask(ctx context.Context, copyID, generation, taskID int64) error
	NextDataSetRetirementGeneration(ctx context.Context, dataSetID int64) (int64, error)
	BindDataSetRetirementTask(ctx context.Context, dataSetID, generation, taskID int64) error
	AuthorizeDataSetRetirementTask(ctx context.Context, dataSetID, generation, taskID int64) (*model.StorageDataSet, error)
	CompleteDataSetRetirementTask(ctx context.Context, dataSetID, generation, taskID int64) error
	// GetUploadCopyForDataSet addresses one concrete data set generation.
	GetUploadCopyForDataSet(ctx context.Context, contentID, storageDataSetID int64) (*model.StorageCopy, error)
	NextFinalizableCopyForDataSet(ctx context.Context, storageDataSetID int64) (*model.StorageCopy, error)
	MarkUploadCopyPieceReady(ctx context.Context, input MarkUploadCopyPieceReadyInput) error
	ReopenFailedUploadCopy(ctx context.Context, copyID int64) error
	ReserveCommitAttempt(ctx context.Context, input storagecommit.ReserveInput) (storagecommit.ReserveResult, error)
	MarkCommitAttempted(ctx context.Context, input storagecommit.AttemptInput) (storagecommit.AttemptResult, error)
	RecordCommitSubmission(ctx context.Context, input storagecommit.EvidenceInput) error
	MarkCommitAttention(ctx context.Context, input storagecommit.AttentionInput) error
	ResetCommitAttempt(ctx context.Context, input storagecommit.ResetInput) error
	ReleaseCommitAttempt(ctx context.Context, input storagecommit.ReleaseInput) error
	ReleaseCommitReservation(ctx context.Context, input storagecommit.ReservationReleaseInput) error
	CountActiveCommitAttemptsForDataSet(ctx context.Context, storageDataSetID int64) (int, error)
	ListCommitAttention(ctx context.Context, limit int) ([]storagecommit.AttentionRecord, error)
	ReleaseCommitAttention(ctx context.Context, input storagecommit.ManualReleaseInput) error
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
	BindTask(ctx context.Context, replacementID, generation, taskID int64) error
	AuthorizeTask(ctx context.Context, replacementID, generation, taskID int64) (*storagereplacement.Replacement, error)
	CompleteTask(ctx context.Context, replacementID, generation, taskID int64) error

	GetByID(ctx context.Context, id int64) (*storagereplacement.Replacement, error)
	GetByClientRequestID(ctx context.Context, bucketID int64, clientRequestID string) (*storagereplacement.Replacement, error)
	ListForBucket(ctx context.Context, bucketID int64, limit int) ([]storagereplacement.Replacement, error)
	GetActiveForDataSet(ctx context.Context, dataSetID int64) (*storagereplacement.Replacement, error)
	// HasInProgressForDataSet reports whether recovery must leave this data set
	// alone. Operator-paused failed or attention work does not count as actively
	// progressing, so a stuck slot can still repair in place; its replacement
	// identity remains reserved until retry, supersession, or completion.
	HasInProgressForDataSet(ctx context.Context, dataSetID int64) (bool, error)
	ListActive(ctx context.Context, afterID int64, limit int) ([]storagereplacement.Replacement, error)

	// Activate makes the target the write target and marks the source draining
	// in one transaction. It touches a fixed number of rows regardless of how
	// much history the bucket holds.
	Activate(ctx context.Context, replacementID int64) error
	SeedMigrationBatch(ctx context.Context, replacementID int64, limit int) (inserted int, done bool, err error)
	NextPendingReplacementItem(ctx context.Context, replacementID int64) (*storagereplacement.Item, error)
	MarkReplacementItemCopied(ctx context.Context, replacementID, itemID, targetCopyID int64) error
	MarkReplacementItemCancelled(ctx context.Context, replacementID, itemID int64) error
	MarkReplacementItemAttention(ctx context.Context, replacementID, itemID int64, lastError string) error
	ReplacementExecution(ctx context.Context, replacementID int64) (storagereplacement.ExecutionSnapshot, error)
	ReplacementProgresses(ctx context.Context, replacementIDs []int64) (map[int64]storagereplacement.ProgressSnapshot, error)
	AcquireItem(ctx context.Context, input AcquireReplacementItemInput) (*ReplacementItemSnapshot, error)
	AttachTargetCopy(ctx context.Context, input AttachReplacementTargetCopyInput) (*model.StorageCopy, error)

	MarkMigrating(ctx context.Context, replacementID int64) error
	MarkWaiting(ctx context.Context, replacementID int64, reason storagereplacement.WaitReason) error
	MarkFailed(ctx context.Context, replacementID int64, reason *storagereplacement.FailureReason, lastError string) error
	MarkCleanupAttention(ctx context.Context, replacementID int64, lastError string) error
	BeginRetirement(ctx context.Context, replacementID int64) error
	RecordTerminationEpoch(ctx context.Context, input RecordTerminationEpochInput) error
	RecordAbandonedTerminationEpoch(ctx context.Context, input RecordTerminationEpochInput) error
	CompleteAbandonedTargetTermination(ctx context.Context, replacementID int64) error
	// EvaluateRetirementGate reports every blocker by name so the API and UI can
	// explain why a source is still held.
	EvaluateRetirementGate(ctx context.Context, replacementID int64, observedEpoch *int64) (RetirementGate, error)
	// CompleteRetirement re-runs the whole gate inside its own transaction and
	// refuses premature completion even when called outside the worker.
	CompleteRetirement(ctx context.Context, replacementID int64, observedEpoch int64) error

	// CountAbandonedTargetSoleCopies counts the copies that only the target a
	// later confirmation replaced still holds. CompleteAbandonedTargetTermination
	// refuses to retire that target while any remain.
	CountAbandonedTargetSoleCopies(ctx context.Context, targetDataSetID int64) (int, error)
}

// AuthorizeReplacementInput is one operator confirmation.
type AuthorizeReplacementInput struct {
	BucketID         int64
	SourceDataSetID  int64
	SelectionMode    storagereplacement.SelectionMode
	TargetProviderID types.OnChainID
	ClientRequestID  string
}

type RetryReplacementInput struct {
	ReplacementID int64
}

type AcquireReplacementItemInput struct {
	ReplacementID int64
	ItemID        int64
}

// ReplacementItemSnapshot is the consistent view one migration item needs.
type ReplacementItemSnapshot struct {
	Replacement storagereplacement.Replacement
	Item        storagereplacement.Item
	Source      model.StorageDataSet
	Target      model.StorageDataSet
	Upload      model.StorageContent
	Version     model.ObjectVersion
}

type AttachReplacementTargetCopyInput struct {
	ReplacementID int64
	ItemID        int64
	ContentID     int64
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
	ActiveAttempts   int
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
	Enqueue(ctx context.Context, task *model.Task) (*model.Task, bool, error)
	GetByID(ctx context.Context, id int64) (*model.Task, error)
	GetByIdentity(ctx context.Context, taskType model.TaskType, idempotencyKey string) (*model.Task, error)
	PreviousStoreCheckpoints(ctx context.Context, copyID, taskID int64) ([]model.Task, error)
	ClaimNext(ctx context.Context, leaseDuration time.Duration) (*model.Task, error)
	RenewLease(ctx context.Context, id, generation int64, leaseDuration time.Duration) (time.Time, error)
	WriteCheckpoint(ctx context.Context, id, generation int64, checkpoint json.RawMessage) error
	ConsumeStoreRetry(ctx context.Context, id, generation int64) error
	ValidateClaim(ctx context.Context, id, generation int64) error
	Settle(ctx context.Context, id, generation int64, transition TaskTransition) error
	ShortenLease(ctx context.Context, id, generation int64, duration time.Duration) error
	WakePending(ctx context.Context, ids []int64) (int, error)
	RequestCancellation(ctx context.Context, id int64, reason string) error
	RetryFailed(ctx context.Context, id int64) error
	ReactivateTerminal(ctx context.Context, id int64) error
	AcknowledgeFailed(ctx context.Context, id int64, retention time.Duration) error
	// AcknowledgeFailedMatching dismisses every unacknowledged failure the filter
	// covers and reports how many it dismissed.
	AcknowledgeFailedMatching(ctx context.Context, filter TaskAcknowledgeFilter, retention time.Duration) (int, error)
	// CountFailedMatching reports how many failures the same filter covers, so a
	// bulk dismissal can be previewed before it is confirmed.
	CountFailedMatching(ctx context.Context, filter TaskAcknowledgeFilter) (int, error)
	DeleteRetained(ctx context.Context, now time.Time, limit int) (int, error)
	List(ctx context.Context, filter TaskListFilter) (TaskPage, error)
	CountByStatus(ctx context.Context) ([]TaskStatusCount, error)
	CountByPresentationStatus(ctx context.Context) ([]TaskStatusCount, error)
	CountUnacknowledgedFailed(ctx context.Context) (int64, error)
	CountOverviewActivePipeline(ctx context.Context) ([]TaskPipelineCount, error)
}

type TaskTransition struct {
	Status         model.TaskStatus
	ResumeMode     model.TaskResumeMode
	AvailableAt    time.Time
	WaitReason     *string
	FailureReason  *string
	LastError      *string
	StatusMessage  *string
	IncrementRetry bool
	RetentionUntil *time.Time
}

// TaskAcknowledgeFilter selects the failures one bulk dismissal covers. The
// cutoff is what the operator saw: failures recorded after it stay visible.
type TaskAcknowledgeFilter struct {
	Type         model.TaskType
	FailedBefore time.Time
}

type TaskListFilter struct {
	Type                       model.TaskType
	Status                     model.TaskStatus
	Acknowledged               *bool
	BeforeID                   int64
	Limit                      int
	HideHealthyRecurringSystem bool
}

type TaskPage struct {
	Tasks        []model.Task
	NextBeforeID int64
}

type WalletOperationRepository interface {
	CreateOrGet(ctx context.Context, input CreateWalletOperationInput) (*model.WalletOperation, bool, error)
	GetByID(ctx context.Context, id int64) (*model.WalletOperation, error)
	BindTask(ctx context.Context, id, taskID int64) error
	MarkBroadcastAttempted(ctx context.Context, id, taskID int64) error
	MarkSubmitted(ctx context.Context, id, taskID int64, txHash string) error
	MarkConfirmed(ctx context.Context, id, taskID int64, txHash string) error
	MarkConfirmedWithoutTransaction(ctx context.Context, id, taskID int64) error
	MarkFailed(ctx context.Context, id, taskID int64, lastError string) error
	MarkUnknown(ctx context.Context, id, taskID int64, lastError string) error
	ListRecent(ctx context.Context, limit int) ([]model.WalletOperation, error)
}

type ObservabilityRepository interface {
	OverviewStorageStates(ctx context.Context) ([]model.StorageDataSet, []observability.ProviderState, []observability.DataSetState, *time.Time, *time.Time, error)
	ReplaceProviderStates(ctx context.Context, checkedAt time.Time, states []observability.ProviderState) error
	ListProviderStates(ctx context.Context, opts observability.ListOptions) (observability.ProviderStatePage, error)
	ReplaceDataSetStates(ctx context.Context, checkedAt time.Time, states []observability.DataSetState) error
	ListDataSetStates(ctx context.Context, opts observability.ListOptions) (observability.DataSetStatePage, error)
	GetDataSetStatesByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]observability.DataSetState, error)
}

type ProviderUploadSpeedRepository interface {
	Begin(context.Context, string, string, int64) error
	BeginIfAbsent(context.Context, string, string, int64) error
	Finish(context.Context, string, int64, providerbenchmark.State, int64, int64, string) error
	FailActiveTask(context.Context, int64, string) error
	Get(context.Context, string) (*providerbenchmark.Result, error)
	ListByProviderIDs(context.Context, []string) (map[string]providerbenchmark.Result, error)
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
	ListByBucket(ctx context.Context, bucketID int64, prefix, keyMarker, contentIDMarker string, maxUploads int) ([]model.MultipartUpload, error)
	// CountActiveByBucket returns initiated/completing multipart uploads for the given bucket.
	CountActiveByBucket(ctx context.Context, bucketID int64) (int64, error)
	// SetStatus atomically transitions status using CAS (compare-and-swap) to prevent races.
	SetStatus(ctx context.Context, contentID string, from, to model.MultipartStatus) error
	Delete(ctx context.Context, contentID string) error

	// Part operations
	CreatePart(ctx context.Context, part *model.MultipartPart) error
	GetParts(ctx context.Context, contentID string, partNumberMarker, maxParts int) ([]model.MultipartPart, error)
	GetPartsByNumbers(ctx context.Context, contentID string, numbers []int) ([]model.MultipartPart, error)
	DeleteParts(ctx context.Context, contentID string) error
}
