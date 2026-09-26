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
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagereplacement"
	taskengine "github.com/strahe/synaps3/internal/task"
	idtypes "github.com/strahe/synaps3/internal/types"
)

// providerReplacementSelector resolves the provider an automatic replacement
// should use. It is injected so the admin API stays testable without a live
// storage service.
type providerReplacementSelector interface {
	ListReplacementProviderObservations(context.Context, *idtypes.OnChainID) ([]observability.ProviderObservation, error)
}

// WithProviderReplacement enables the provider replacement endpoints. Without it
// they report that the storage service is unavailable.
func (s *Server) WithProviderReplacement(selector providerReplacementSelector) *Server {
	s.replacementSelector = selector
	return s
}

type startReplacementRequest struct {
	Mode                 string `json:"mode"`
	ProviderID           string `json:"provider_id"`
	ClientRequestID      string `json:"client_request_id"`
	PriceListFingerprint string `json:"price_list_fingerprint"`
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

	ItemsTotal       int                          `json:"items_total"`
	ItemsCopied      int                          `json:"items_copied"`
	Progress         *replacementProgressResponse `json:"progress,omitempty"`
	LastError        *string                      `json:"last_error"`
	TerminationEpoch *int64                       `json:"termination_epoch"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type replacementProgressResponse struct {
	Scope               string  `json:"scope"`
	Phase               string  `json:"phase"`
	SeedingComplete     bool    `json:"seeding_complete"`
	ItemsTotal          int     `json:"items_total"`
	ItemsProcessed      int     `json:"items_processed"`
	ItemsCopied         int     `json:"items_copied"`
	ItemsNoLongerNeeded int     `json:"items_no_longer_needed"`
	ItemsPending        int     `json:"items_pending"`
	ItemsActive         int     `json:"items_active"`
	ItemsAttention      int     `json:"items_attention"`
	ItemsRetrying       int     `json:"items_retrying"`
	ItemsWaitingSource  int     `json:"items_waiting_source"`
	ItemsFailed         int     `json:"items_failed"`
	Percent             *int    `json:"percent,omitempty"`
	NextRetryAt         *string `json:"next_retry_at,omitempty"`
}

type replacementDataSetResponse struct {
	ID               int64                     `json:"id"`
	Generation       int64                     `json:"generation"`
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
	req.PriceListFingerprint = strings.TrimSpace(req.PriceListFingerprint)
	if req.ClientRequestID == "" || len(req.ClientRequestID) > 128 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "client_request_id is required"})
		return
	}
	if len(req.PriceListFingerprint) != 64 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "price list review is required"})
		return
	}

	ctx := r.Context()
	bucket, ok := s.replacementBucket(w, ctx, name)
	if !ok {
		return
	}

	mode := storagereplacement.SelectionMode(req.Mode)
	if !mode.Valid() {
		s.writeReplacementError(w, storagereplacement.ErrInvalidTarget, name)
		return
	}
	if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
		return
	}
	source, sourceErr := s.repos.Contents.GetDataSetBindingByID(ctx, dataSetID)
	if sourceErr != nil || source == nil || source.BucketID != bucket.ID {
		if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
			return
		}
		if sourceErr != nil {
			s.logger.Error("api: failed to load data set for replacement", "error", sourceErr, "dataSetID", dataSetID)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		} else {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "data set not found"})
		}
		return
	}
	price, priceErr := s.currentWarmStoragePrice(ctx)
	if priceErr != nil {
		if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "price list unavailable", "code": "price_list_unavailable"})
		return
	}
	if !price.SupportedToken {
		if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
			return
		}
		writeJSON(w, http.StatusConflict, map[string]string{"error": "unsupported price token", "code": "unsupported_price_token"})
		return
	}
	if price.Fingerprint != req.PriceListFingerprint {
		if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
			return
		}
		writeJSON(w, http.StatusConflict, map[string]string{"error": "price list changed", "code": "price_list_changed"})
		return
	}
	targetProvider, err := s.resolveReplacementProvider(ctx, bucket, source, mode, req.ProviderID)
	if err != nil {
		if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
			return
		}
		s.writeReplacementError(w, err, name)
		return
	}

	if s.taskService == nil {
		if s.writeReplacementReplay(w, ctx, bucket, dataSetID, req, mode) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	var row *storagereplacement.Replacement
	created := false
	err = s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		var authorizeErr error
		row, created, authorizeErr = txRepos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
			BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: mode,
			TargetProviderID: targetProvider, ClientRequestID: req.ClientRequestID,
			PriceListFingerprint: req.PriceListFingerprint,
		})
		if authorizeErr != nil || !created {
			return authorizeErr
		}
		taskRow, _, enqueueErr := s.taskService.EnqueueInTransaction(ctx, txRepos, taskengine.EnqueueRequest{
			Type:           model.TaskTypeProviderReplacementCoordinate,
			IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration),
			Input:          storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration},
			SubjectType:    "storage_replacement", SubjectKey: strconv.FormatInt(row.ID, 10),
		})
		if enqueueErr != nil {
			return enqueueErr
		}
		if bindErr := txRepos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, taskRow.ID); bindErr != nil {
			return bindErr
		}
		row.TaskID = &taskRow.ID
		return nil
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

func (s *Server) writeReplacementReplay(w http.ResponseWriter, ctx context.Context, bucket *model.Bucket, dataSetID int64, req startReplacementRequest, mode storagereplacement.SelectionMode) bool {
	existing, err := s.repos.Replacements.GetByClientRequestID(ctx, bucket.ID, req.ClientRequestID)
	if err != nil {
		s.writeReplacementError(w, err, bucket.Name)
		return true
	}
	if existing == nil {
		return false
	}
	if !replacementReplayMatches(existing, dataSetID, mode, req.ProviderID, req.PriceListFingerprint) {
		s.writeReplacementError(w, storagereplacement.ErrIdempotencyConflict, bucket.Name)
		return true
	}
	response, err := s.providerReplacementResponse(ctx, bucket.Name, existing)
	if err != nil {
		s.logger.Error("api: failed to build replacement replay response", "error", err, "replacementID", existing.ID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return true
	}
	writeJSON(w, http.StatusOK, response)
	return true
}

func replacementReplayMatches(
	row *storagereplacement.Replacement,
	sourceDataSetID int64,
	mode storagereplacement.SelectionMode,
	requested string,
	priceListFingerprint string,
) bool {
	if row.SourceDataSetID != sourceDataSetID || row.SelectionMode != mode || row.PriceListFingerprint != priceListFingerprint {
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
// retired generations, and confirms FWSS approval on chain.
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
		candidates, err := s.replacementProviderCandidatesFor(ctx, bucket, source, &providerID)
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
			return idtypes.OnChainID{}, storagereplacement.ErrTargetUnavailable
		}
		return idtypes.OnChainID{}, storagereplacement.ErrTargetUnavailable
	case storagereplacement.SelectionModeAutomatic:
		if s.warmStorageMarket == nil {
			return idtypes.OnChainID{}, errApprovalCheckUnavailable
		}
		approvalCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if requested != "" {
			return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
		}
		candidates, err := s.replacementProviderCandidates(ctx, bucket, source)
		if err != nil {
			return idtypes.OnChainID{}, err
		}
		for _, candidate := range candidates {
			if !candidate.Eligible || candidate.PreviouslyUsed {
				continue
			}
			if !candidate.ApprovedFresh || candidate.Profile == nil {
				continue
			}
			if !candidate.Profile.Approved {
				continue
			}
			approved, checkErr := s.warmStorageMarket.IsProviderApproved(approvalCtx, candidate.ProviderID.SDK())
			if checkErr != nil {
				return idtypes.OnChainID{}, fmt.Errorf("%w: %v", errApprovalCheckUnavailable, checkErr)
			}
			if approved {
				return candidate.ProviderID, nil
			}
		}
		return idtypes.OnChainID{}, storagereplacement.ErrNoEligibleProvider
	default:
		return idtypes.OnChainID{}, storagereplacement.ErrInvalidTarget
	}
}

// replacementProviderCandidates lists every observed provider together with
// whether this replica can move to it. Automatic selection applies the
// separate approval requirement after the shared availability checks.
func (s *Server) replacementProviderCandidates(
	ctx context.Context,
	bucket *model.Bucket,
	source *model.StorageDataSet,
) ([]replacementProviderCandidate, error) {
	return s.replacementProviderCandidatesFor(ctx, bucket, source, nil)
}

func (s *Server) replacementProviderCandidatesFor(
	ctx context.Context,
	bucket *model.Bucket,
	source *model.StorageDataSet,
	requested *idtypes.OnChainID,
) ([]replacementProviderCandidate, error) {
	if s.replacementSelector == nil || s.observability == nil {
		return nil, errReplacementUnavailable
	}
	observations, err := s.replacementSelector.ListReplacementProviderObservations(ctx, requested)
	if err != nil {
		return nil, err
	}
	providers := make([]idtypes.OnChainID, 0, len(observations))
	for _, item := range observations {
		providers = append(providers, item.Facts.ProviderID)
	}
	bindings, err := s.repos.Contents.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	candidates := replacementProviderCandidates(providers, bindings, source)
	profiles, err := s.repos.Observability.ProviderProfiles(ctx, providers)
	if err != nil {
		return nil, err
	}
	observed := make(map[string]observability.ProviderObservation, len(observations))
	for _, item := range observations {
		observed[item.Facts.ProviderID.String()] = item
	}
	now := time.Now().UTC()
	for i := range candidates {
		item := observed[candidates[i].ProviderID.String()]
		candidates[i].Observation = &item
		profile, hasProfile := profiles[candidates[i].ProviderID.String()]
		if hasProfile {
			copy := profile
			candidates[i].Profile = &copy
			candidates[i].ApprovedFresh = profile.ApprovedCheckedAt != nil &&
				!profile.ApprovedCheckedAt.IsZero() && now.Sub(*profile.ApprovedCheckedAt) <= 2*s.observability.RefreshInterval()
		}
		if !candidates[i].Eligible {
			continue
		}
		profileURLChanged := hasProfile && item.Facts.ServiceURL != nil && profile.ServiceURL != *item.Facts.ServiceURL
		switch {
		case hasProfile && !profile.Active:
			candidates[i].Eligible = false
			candidates[i].IneligibleReason = "provider_unavailable"
		case item.Signal.Freshness.Stale:
			candidates[i].Eligible = false
			candidates[i].IneligibleReason = "observation_stale"
		case item.Signal.Status != observability.StatusAvailable || item.Facts.Active == nil || !*item.Facts.Active || item.Facts.HasPDP == nil || !*item.Facts.HasPDP || item.Facts.ServiceURL == nil || *item.Facts.ServiceURL == "":
			candidates[i].Eligible = false
			candidates[i].IneligibleReason = "provider_unavailable"
		case !hasProfile:
			candidates[i].Eligible = false
			candidates[i].IneligibleReason = "profile_missing"
		case profileURLChanged:
			candidates[i].Eligible = false
			candidates[i].IneligibleReason = "profile_url_changed"
		}
	}
	return candidates, nil
}

var (
	errReplacementUnavailable   = errors.New("storage service is unavailable")
	errApprovalCheckUnavailable = errors.New("provider approval check is unavailable")
)

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
	if s.taskService == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "task service unavailable"})
		return
	}
	var row *storagereplacement.Replacement
	err = s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		var retryErr error
		row, retryErr = txRepos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: id})
		if retryErr != nil {
			return retryErr
		}
		taskRow, _, enqueueErr := s.taskService.EnqueueInTransaction(ctx, txRepos, taskengine.EnqueueRequest{
			Type:           model.TaskTypeProviderReplacementCoordinate,
			IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration),
			Input:          storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration},
			SubjectType:    "storage_replacement", SubjectKey: strconv.FormatInt(row.ID, 10),
		})
		if enqueueErr != nil {
			return enqueueErr
		}
		if bindErr := txRepos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, taskRow.ID); bindErr != nil {
			return bindErr
		}
		row.TaskID = &taskRow.ID
		return nil
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
	case errors.Is(err, errApprovalCheckUnavailable):
		s.logger.Warn("api: provider approval check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Could not confirm provider approval. Try again.", "code": "approval_check_unavailable"})
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
	case errors.Is(err, storagereplacement.ErrTargetCreating):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "the earlier replacement of this replica is still setting up its storage service",
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
	source, err := s.repos.Contents.GetDataSetBindingByID(ctx, row.SourceDataSetID)
	if err != nil {
		return providerReplacementResponse{}, err
	}
	target, err := s.repos.Contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return providerReplacementResponse{}, err
	}
	identities := s.providerIdentities(ctx, replacementProviderIDs(source, target))
	response := providerReplacementResponse{
		ID:            row.ID,
		BucketName:    bucketName,
		CopyIndex:     row.CopyIndex,
		Status:        string(row.Status),
		SelectionMode: string(row.SelectionMode),
		Source:        replacementDataSetView(source, identities),
		Target:        replacementDataSetView(target, identities),
		ItemsTotal:    row.ItemsTotal,
		ItemsCopied:   row.ItemsCopied,
		Progress: &replacementProgressResponse{
			Scope: "provider_replacement", Phase: string(progress.Phase), SeedingComplete: progress.SeedingComplete,
			ItemsTotal: progress.ItemsTotal, ItemsProcessed: progress.ItemsProcessed,
			ItemsCopied: progress.ItemsCopied, ItemsNoLongerNeeded: progress.ItemsNoLongerNeeded,
			ItemsPending: progress.ItemsPending, ItemsActive: progress.ItemsActive,
			ItemsAttention: progress.ItemsAttention, ItemsRetrying: progress.ItemsRetrying,
			ItemsWaitingSource: progress.ItemsWaitingSource, ItemsFailed: progress.ItemsFailed,
			Percent: progress.Percent,
		},
		LastError:        row.LastError,
		TerminationEpoch: row.TerminationEpoch,
		CreatedAt:        row.CreatedAt,
		UpdatedAt:        row.UpdatedAt,
	}
	if progress.NextRetryAt != nil {
		value := progress.NextRetryAt.Format(time.RFC3339)
		response.Progress.NextRetryAt = &value
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
	bucket, ok := s.replacementBucket(w, ctx, name)
	if !ok {
		return nil, nil, false
	}
	source, ok := s.replacementDataSet(w, ctx, bucket, dataSetID)
	return bucket, source, ok
}

func (s *Server) replacementBucket(w http.ResponseWriter, ctx context.Context, name string) (*model.Bucket, bool) {
	bucket, err := s.repos.Buckets.GetByName(ctx, name)
	if err != nil {
		s.logger.Error("api: failed to load bucket for replacement", "error", err, "bucket", name)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return nil, false
	}
	if bucket == nil || !bucket.Status.IsAdminVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return nil, false
	}
	return bucket, true
}

func (s *Server) replacementDataSet(w http.ResponseWriter, ctx context.Context, bucket *model.Bucket, dataSetID int64) (*model.StorageDataSet, bool) {
	source, err := s.repos.Contents.GetDataSetBindingByID(ctx, dataSetID)
	if err != nil {
		s.logger.Error("api: failed to load data set for replacement", "error", err, "dataSetID", dataSetID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return nil, false
	}
	if source == nil || source.BucketID != bucket.ID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "data set not found"})
		return nil, false
	}
	return source, true
}

// replacementProviderResponse is one row of the provider chooser. Ineligible
// providers are included with the reason, so an operator can see why the
// provider they were looking for cannot take this replica.
type replacementProviderResponse struct {
	ProviderID        string                             `json:"provider_id"`
	ManualSelectable  bool                               `json:"manual_selectable"`
	ManualBlockReason string                             `json:"manual_block_reason,omitempty"`
	ApprovedFresh     bool                               `json:"approved_fresh"`
	PreviouslyUsed    bool                               `json:"previously_used"`
	ProviderIdentity  *providerIdentityResponse          `json:"provider_identity,omitempty"`
	ProviderProfile   *observability.ProviderProfile     `json:"provider_profile,omitempty"`
	Observation       *observability.ProviderObservation `json:"observation,omitempty"`
	UploadSpeedTest   *providerUploadSpeedView           `json:"upload_speed_test,omitempty"`
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
	profiles := make(map[string]observability.ProviderProfile, len(candidates))
	for _, candidate := range candidates {
		if candidate.Profile != nil {
			profiles[candidate.ProviderID.String()] = *candidate.Profile
		}
	}
	identities := s.providerIdentitiesFromProfiles(profiles)
	keys := make([]string, 0, len(providerIDs))
	for _, id := range providerIDs {
		keys = append(keys, id.String())
	}
	speedTests, err := s.repos.ProviderUploadSpeed.ListByProviderIDs(ctx, keys)
	if err != nil {
		s.writeReplacementError(w, err, name)
		return
	}

	providers := make([]replacementProviderResponse, 0, len(candidates))
	for _, candidate := range candidates {
		view := replacementProviderResponse{
			ProviderID:        candidate.ProviderID.String(),
			ManualSelectable:  candidate.Eligible,
			ManualBlockReason: candidate.IneligibleReason,
			ApprovedFresh:     candidate.ApprovedFresh,
			PreviouslyUsed:    candidate.PreviouslyUsed,
			ProviderIdentity:  providerIdentityFromSnapshot(identities, candidate.ProviderID),
			Observation:       candidate.Observation,
			ProviderProfile:   candidate.Profile,
		}
		if row, ok := speedTests[candidate.ProviderID.String()]; ok {
			var observedServiceURL *string
			if candidate.Observation != nil {
				observedServiceURL = candidate.Observation.Facts.ServiceURL
			}
			view.UploadSpeedTest = uploadSpeedView(row, view.ProviderProfile, observedServiceURL)
		}
		providers = append(providers, view)
	}
	writeJSON(w, http.StatusOK, map[string]any{"providers": providers})
}
