package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	idtypes "github.com/strahe/synaps3/internal/types"
)

// providerReplacementSelector resolves the provider an automatic replacement
// should use. It is injected so the admin API stays testable without a live
// storage service.
type providerReplacementSelector interface {
	// ListReplacementProviders returns the approved, active providers a
	// replacement may use, in a stable order. Which of them a given replica can
	// actually take is decided by replacementProviderCandidates.
	ListReplacementProviders(ctx context.Context) ([]idtypes.OnChainID, error)
}

// WithProviderReplacement enables the provider replacement endpoints. Without it
// they report that the storage service is unavailable.
func (s *Server) WithProviderReplacement(selector providerReplacementSelector) *Server {
	s.replacementSelector = selector
	return s
}

type startReplacementRequest struct {
	Mode            string `json:"mode"`
	ProviderID      string `json:"provider_id"`
	ClientRequestID string `json:"client_request_id"`
}

// providerReplacementResponse is the operator-facing view of one replacement.
// Counts are deliberately named for what they measure: a confirmation talks
// about referenced versions, progress talks about stored content.
type providerReplacementResponse struct {
	ID            int64  `json:"id"`
	BucketName    string `json:"bucket_name"`
	CopyIndex     int    `json:"copy_index"`
	Status        string `json:"status"`
	WaitReason    string `json:"wait_reason,omitempty"`
	WaitMessage   string `json:"wait_message,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	SelectionMode string `json:"selection_mode"`

	Source replacementDataSetResponse `json:"source"`
	Target replacementDataSetResponse `json:"target"`

	ItemsTotal       int                   `json:"items_total"`
	ItemsCopied      int                   `json:"items_copied"`
	Progress         *taskProgressResponse `json:"progress,omitempty"`
	LastError        *string               `json:"last_error"`
	TerminationEpoch *int64                `json:"termination_epoch"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type replacementDataSetResponse struct {
	ID               int64                     `json:"id"`
	Generation       int                       `json:"generation"`
	IsCurrent        bool                      `json:"is_current"`
	Status           string                    `json:"status"`
	ProviderID       string                    `json:"provider_id"`
	DataSetID        *string                   `json:"data_set_id"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
}

// handleAPIStartDataSetReplacement is the only entry point that authorizes a
// provider replacement. One confirmation covers the new paid service, the
// topology switch, the migration, and retirement of the old service once the
// safety gate passes.
func (s *Server) handleAPIStartDataSetReplacement(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !bucketNameRe.MatchString(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	dataSetID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || dataSetID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid data set id"})
		return
	}
	var req startReplacementRequest
	if !decodeBucketStrictJSON(w, r, &req) {
		return
	}
	req.ClientRequestID = strings.TrimSpace(req.ClientRequestID)
	if req.ClientRequestID == "" || len(req.ClientRequestID) > 128 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "client_request_id is required"})
		return
	}

	ctx := r.Context()
	bucket, source, ok := s.replacementSubject(w, ctx, name, dataSetID)
	if !ok {
		return
	}

	mode := storagereplacement.SelectionMode(req.Mode)
	if !mode.Valid() {
		s.writeReplacementError(w, storagereplacement.ErrInvalidTarget, name)
		return
	}
	existing, err := s.repos.Replacements.GetByClientRequestID(ctx, bucket.ID, req.ClientRequestID)
	if err != nil {
		s.writeReplacementError(w, err, name)
		return
	}
	if existing != nil {
		if !replacementReplayMatches(existing, source.ID, mode, req.ProviderID) {
			s.writeReplacementError(w, storagereplacement.ErrIdempotencyConflict, name)
			return
		}
		response, responseErr := s.providerReplacementResponse(ctx, bucket.Name, existing)
		if responseErr != nil {
			s.logger.Error("api: failed to build replacement replay response", "error", responseErr, "replacementID", existing.ID)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		writeJSON(w, http.StatusOK, response)
		return
	}
	targetProvider, err := s.resolveReplacementProvider(ctx, bucket, source, mode, req.ProviderID)
	if err != nil {
		s.writeReplacementError(w, err, name)
		return
	}

	row, created, err := s.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    mode,
		TargetProviderID: targetProvider,
		ClientRequestID:  req.ClientRequestID,
		MaxRetries:       s.uploadMaxRetries,
	})
	if err != nil {
		s.writeReplacementError(w, err, name)
		return
	}
	response, err := s.providerReplacementResponse(ctx, bucket.Name, row)
	if err != nil {
		s.logger.Error("api: failed to build replacement response", "error", err, "replacementID", row.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, response)
}

func replacementReplayMatches(
	row *storagereplacement.Replacement,
	sourceDataSetID int64,
	mode storagereplacement.SelectionMode,
	requested string,
) bool {
	if row.SourceDataSetID != sourceDataSetID || row.SelectionMode != mode {
		return false
	}
	if mode == storagereplacement.SelectionModeAutomatic {
		return strings.TrimSpace(requested) == ""
	}
	providerID, err := idtypes.ParseOnChainID("provider_id", requested)
	return err == nil && row.RequestedProviderID != nil && row.RequestedProviderID.Equal(providerID)
}

// resolveReplacementProvider turns the operator's choice into one provider.
// Automatic selection excludes every provider the bucket has used, including
// retired generations; a supplied Provider ID is used exactly as approved.
func (s *Server) resolveReplacementProvider(
	ctx context.Context,
	bucket *model.Bucket,
	source *model.StorageDataSet,
	mode storagereplacement.SelectionMode,
	requested string,
) (idtypes.OnChainID, error) {
	switch mode {
	case storagereplacement.SelectionModeManual:
		providerID, err := idtypes.ParseOnChainID("provider_id", requested)
		if err != nil || providerID.IsZero() {
			return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
		}
		if providerID.Equal(source.ProviderID) {
			return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
		}
		candidates, err := s.replacementProviderCandidates(ctx, bucket, source)
		if err != nil {
			return idtypes.OnChainID{}, err
		}
		for _, candidate := range candidates {
			if !candidate.ProviderID.Equal(providerID) {
				continue
			}
			if candidate.Eligible {
				return providerID, nil
			}
			if candidate.IneligibleReason == providerIneligibleServesBucket {
				return idtypes.OnChainID{}, storagereplacement.ErrTargetInUse
			}
			return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
		}
		return idtypes.OnChainID{}, storagereplacement.ErrTargetUnavailable
	case storagereplacement.SelectionModeAutomatic:
		if requested != "" {
			return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
		}
		candidates, err := s.replacementProviderCandidates(ctx, bucket, source)
		if err != nil {
			return idtypes.OnChainID{}, err
		}
		providerID, ok := firstAutomaticChoice(candidates)
		if !ok {
			return idtypes.OnChainID{}, storagereplacement.ErrNoEligibleProvider
		}
		return providerID, nil
	default:
		return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
	}
}

// replacementProviderCandidates lists every approved provider together with
// whether this replica can move to it. Both the automatic choice and the
// dashboard's provider list read it, so neither can offer what Authorize
// refuses.
func (s *Server) replacementProviderCandidates(
	ctx context.Context,
	bucket *model.Bucket,
	source *model.StorageDataSet,
) ([]replacementProviderCandidate, error) {
	if s.replacementSelector == nil {
		return nil, errReplacementUnavailable
	}
	providers, err := s.replacementSelector.ListReplacementProviders(ctx)
	if err != nil {
		return nil, err
	}
	bindings, err := s.repos.Uploads.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	return replacementProviderCandidates(providers, bindings, source), nil
}

var errReplacementUnavailable = errors.New("storage service is unavailable")

// handleAPIRetryStorageReplacement resumes work an operator owns. Choosing a
// different provider needs a new confirmation, so this endpoint never changes
// the approved target.
func (s *Server) handleAPIRetryStorageReplacement(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid replacement id"})
		return
	}
	ctx := r.Context()
	row, err := s.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{
		ReplacementID:  id,
		MaxRetries:     s.uploadMaxRetries,
		ItemMaxRetries: s.providerReplacementMaxRetries,
	})
	if err != nil {
		s.writeReplacementError(w, err, "")
		return
	}
	bucket, err := s.repos.Buckets.GetByID(ctx, row.BucketID)
	if err != nil || bucket == nil {
		s.logger.Error("api: failed to load bucket for replacement retry", "error", err, "bucketID", row.BucketID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	response, err := s.providerReplacementResponse(ctx, bucket.Name, row)
	if err != nil {
		s.logger.Error("api: failed to build replacement response", "error", err, "replacementID", row.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) writeReplacementError(w http.ResponseWriter, err error, bucketName string) {
	code := storagereplacement.Code(err)
	switch {
	case errors.Is(err, errReplacementUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage service is unavailable"})
	case errors.Is(err, storagereplacement.ErrInvalidTarget):
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "choose a different provider for this replica",
			"code":  code,
		})
	case errors.Is(err, repository.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid replacement request"})
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	case errors.Is(err, storagereplacement.ErrActiveReplacement):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "this replica is already being replaced",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrTargetInUse):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "that provider already stores a replica of this bucket",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrNoEligibleProvider):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "no unused storage provider is available right now",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrTargetUnavailable):
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "that provider is not currently available for replacement",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrIdempotencyConflict):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "client_request_id already belongs to a different replacement request",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrSourceNotCurrent):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "this replica no longer receives writes, so replacing it would change nothing",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrSuperseded):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "a newer replacement has taken over this replica",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrNotRetryable):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "this replacement is still progressing on its own",
			"code":  code,
		})
	case errors.Is(err, storagereplacement.ErrTaskRunning):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "replacement work is still running; try again shortly",
			"code":  code,
		})
	case errors.Is(err, repository.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "replacement state changed; refresh and try again"})
	default:
		s.logger.Error("api: provider replacement failed", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
	}
}

func (s *Server) providerReplacementResponse(ctx context.Context, bucketName string, row *storagereplacement.Replacement) (providerReplacementResponse, error) {
	progresses, err := s.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		return providerReplacementResponse{}, err
	}
	progress, ok := progresses[row.ID]
	if !ok {
		return providerReplacementResponse{}, fmt.Errorf("provider replacement %d progress: %w", row.ID, repository.ErrNotFound)
	}
	return s.providerReplacementResponseWithProgress(ctx, bucketName, row, progress)
}

func (s *Server) providerReplacementResponseWithProgress(
	ctx context.Context,
	bucketName string,
	row *storagereplacement.Replacement,
	progress storagereplacement.ProgressSnapshot,
) (providerReplacementResponse, error) {
	source, err := s.repos.Uploads.GetDataSetBindingByID(ctx, row.SourceDataSetID)
	if err != nil {
		return providerReplacementResponse{}, err
	}
	target, err := s.repos.Uploads.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return providerReplacementResponse{}, err
	}
	identities := s.providerIdentities(replacementProviderIDs(source, target))
	response := providerReplacementResponse{
		ID:               row.ID,
		BucketName:       bucketName,
		CopyIndex:        row.CopyIndex,
		Status:           string(row.Status),
		SelectionMode:    string(row.SelectionMode),
		Source:           replacementDataSetView(source, identities),
		Target:           replacementDataSetView(target, identities),
		ItemsTotal:       row.ItemsTotal,
		ItemsCopied:      row.ItemsCopied,
		Progress:         taskProgressFromReplacement(progress),
		LastError:        row.LastError,
		TerminationEpoch: row.TerminationEpoch,
		CreatedAt:        row.CreatedAt,
		UpdatedAt:        row.UpdatedAt,
	}
	if row.WaitReason != nil {
		response.WaitReason = string(*row.WaitReason)
		response.WaitMessage = row.WaitReason.Message()
	}
	if row.FailureReason != nil {
		response.FailureReason = string(*row.FailureReason)
	}
	return response, nil
}

func replacementProviderIDs(sets ...*model.StorageDataSet) []idtypes.OnChainID {
	ids := make([]idtypes.OnChainID, 0, len(sets))
	for _, set := range sets {
		if set != nil {
			ids = append(ids, set.ProviderID)
		}
	}
	return ids
}

func replacementDataSetView(set *model.StorageDataSet, identities map[string]*providerIdentityResponse) replacementDataSetResponse {
	if set == nil {
		return replacementDataSetResponse{}
	}
	view := replacementDataSetResponse{
		ID:         set.ID,
		Generation: set.Generation,
		IsCurrent:  set.IsCurrent,
		Status:     string(set.Status),
		ProviderID: set.ProviderID.String(),
		DataSetID:  onChainIDStringPtr(set.DataSetID),
	}
	if identity, ok := identities[set.ProviderID.String()]; ok {
		view.ProviderIdentity = identity
	}
	return view
}

// bucketReplacementResponses returns the bucket's whole replacement history,
// newest first, so the dashboard can show what happened as well as what is
// happening.
func (s *Server) bucketReplacementResponses(ctx context.Context, bucketName string, bucketID int64) []providerReplacementResponse {
	rows, err := s.repos.Replacements.ListForBucket(ctx, bucketID, 50)
	if err != nil {
		s.logger.Error("api: failed to list provider replacements", "error", err, "bucketID", bucketID)
		return nil
	}
	replacementIDs := make([]int64, 0, len(rows))
	for i := range rows {
		replacementIDs = append(replacementIDs, rows[i].ID)
	}
	progresses, err := s.repos.Replacements.ReplacementProgresses(ctx, replacementIDs)
	if err != nil {
		s.logger.Error("api: failed to load provider replacement progress", "error", err, "bucketID", bucketID)
		return nil
	}
	out := make([]providerReplacementResponse, 0, len(rows))
	for i := range rows {
		progress, ok := progresses[rows[i].ID]
		if !ok {
			continue
		}
		response, err := s.providerReplacementResponseWithProgress(ctx, bucketName, &rows[i], progress)
		if err != nil {
			s.logger.Error("api: failed to build replacement response", "error", err, "replacementID", rows[i].ID)
			continue
		}
		out = append(out, response)
	}
	return out
}

// replacementSubject resolves the bucket and the replica a replacement acts on,
// writing the response itself when either is missing.
func (s *Server) replacementSubject(
	w http.ResponseWriter,
	ctx context.Context,
	name string,
	dataSetID int64,
) (*model.Bucket, *model.StorageDataSet, bool) {
	bucket, err := s.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		s.logger.Error("api: failed to load bucket for replacement", "error", err, "bucket", name)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return nil, nil, false
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return nil, nil, false
	}
	source, err := s.repos.Uploads.GetDataSetBindingByID(ctx, dataSetID)
	if err != nil {
		s.logger.Error("api: failed to load data set for replacement", "error", err, "dataSetID", dataSetID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return nil, nil, false
	}
	if source == nil || source.BucketID != bucket.ID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "data set not found"})
		return nil, nil, false
	}
	return bucket, source, true
}

// replacementProviderResponse is one row of the provider chooser. Ineligible
// providers are included with the reason, so an operator can see why the
// provider they were looking for cannot take this replica.
type replacementProviderResponse struct {
	ProviderID       string                    `json:"provider_id"`
	Eligible         bool                      `json:"eligible"`
	IneligibleReason string                    `json:"ineligible_reason,omitempty"`
	PreviouslyUsed   bool                      `json:"previously_used"`
	ProviderIdentity *providerIdentityResponse `json:"provider_identity,omitempty"`
}

// handleAPIListDataSetReplacementProviders lists the providers this replica can
// move to. It reports the same eligibility the confirmation enforces, so the
// chooser never offers something the confirmation would reject.
func (s *Server) handleAPIListDataSetReplacementProviders(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !bucketNameRe.MatchString(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bucket name"})
		return
	}
	dataSetID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || dataSetID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid data set id"})
		return
	}
	ctx := r.Context()
	bucket, source, ok := s.replacementSubject(w, ctx, name, dataSetID)
	if !ok {
		return
	}
	candidates, err := s.replacementProviderCandidates(ctx, bucket, source)
	if err != nil {
		s.writeReplacementError(w, err, name)
		return
	}

	providerIDs := make([]idtypes.OnChainID, 0, len(candidates))
	for _, candidate := range candidates {
		providerIDs = append(providerIDs, candidate.ProviderID)
	}
	identities := s.providerIdentities(providerIDs)

	providers := make([]replacementProviderResponse, 0, len(candidates))
	for _, candidate := range candidates {
		providers = append(providers, replacementProviderResponse{
			ProviderID:       candidate.ProviderID.String(),
			Eligible:         candidate.Eligible,
			IneligibleReason: candidate.IneligibleReason,
			PreviouslyUsed:   candidate.PreviouslyUsed,
			ProviderIdentity: providerIdentityFromSnapshot(identities, candidate.ProviderID),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"providers": providers})
}
