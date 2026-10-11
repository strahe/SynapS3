package binding

import (
	"context"
	"errors"
	"sort"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

type SelectedBinding struct {
	Admission *providerselect.Admission
	CopyIndex int
	Target    synapse.StorageTarget
}

type Plan struct {
	Admission *providerselect.Admission
	CopyIndex int
	Provider  idtypes.OnChainID
	DataSet   *storage.DataSetRef
}

type SelectorDependencies struct {
	Repositories       *repository.Repositories
	Storage            synapse.StorageClient
	Observability      *observability.Service
	AnchorProviderTier providerselect.Tier
	Resolver           *Resolver
}

type Selector struct{ deps SelectorDependencies }

func NewSelector(deps SelectorDependencies) (*Selector, error) {
	if deps.Repositories == nil || deps.Resolver == nil {
		return nil, errors.New("binding selector requires repositories and resolver")
	}
	if deps.AnchorProviderTier == "" {
		deps.AnchorProviderTier = providerselect.TierApproved
	}
	if !deps.AnchorProviderTier.Valid() {
		return nil, errors.New("invalid provider tier")
	}
	return &Selector{deps: deps}, nil
}

func (h *Selector) SelectBucketBindings(ctx context.Context, bucket *model.Bucket, targetCount int) ([]SelectedBinding, error) {
	if bucket == nil {
		return nil, errors.New("bucket is missing")
	}
	if h.deps.Storage == nil {
		return nil, errors.New("storage client is unavailable")
	}
	bindings, err := h.deps.Repositories.Contents.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	targetCount = model.ClampStorageCopies(targetCount)
	selected := make([]SelectedBinding, 0, targetCount)
	excluded := make(map[string]providerselect.ExclusionReason)
	for _, binding := range bindings {
		if binding.Status != model.StorageDataSetStatusRetired {
			excluded[binding.ProviderID.String()] = providerselect.ExclusionBucketInUse
		}
	}
	usedIndexes := make(map[int]struct{}, len(bindings))
	for i := range bindings {
		binding := &bindings[i]
		if !binding.IsCurrent {
			continue
		}
		usedIndexes[binding.CopyIndex] = struct{}{}
		if binding.CopyIndex >= targetCount || len(selected) >= targetCount || (binding.Status != model.StorageDataSetStatusReady && binding.Status != model.StorageDataSetStatusPending && binding.Status != model.StorageDataSetStatusCreating) {
			continue
		}
		if binding.DataSetID == nil {
			stopped, err := h.deps.Repositories.Contents.DataSetCreationStopped(ctx, binding.ID)
			if err != nil {
				return nil, err
			}
			if stopped {
				continue
			}
		}
		target, err := h.deps.Resolver.OpenBindingTarget(ctx, bucket.Name, binding)
		if err != nil {
			return nil, h.bindingContextError(ctx, bucket.ID, binding.ProviderID, targetCount, excluded, err)
		}
		selected = append(selected, SelectedBinding{CopyIndex: binding.CopyIndex, Target: target})
	}
	missing := targetCount - len(selected)
	if missing <= 0 {
		sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
		return selected, nil
	}
	slots, err := h.deps.Repositories.Buckets.ActiveReplicaSlots(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	indexes := make([]int, 0, missing)
	for _, copyIndex := range slots {
		if len(indexes) >= missing {
			break
		}
		if _, exists := usedIndexes[copyIndex]; !exists {
			indexes = append(indexes, copyIndex)
		}
	}
	if len(indexes) == 0 {
		sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
		return selected, nil
	}
	missing = min(missing, len(indexes))
	if h.deps.Observability == nil {
		return nil, errors.New("observability service is unavailable")
	}
	inventory, hasTrusted, err := h.selectionInventory(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	refreshed := false
	var openErr error
	var contextErr error
	failedCandidates := make(map[string]providerselect.Candidate)
	finalize := func(selectionErr *providerselect.SelectionError) *providerselect.SelectionError {
		selectionErr.Diagnostics.RequestedCount = targetCount
		selectionErr.Diagnostics.EligibleCount += len(selected)
		if openErr != nil {
			selectionErr.Cause = errors.Join(selectionErr.Cause, openErr)
		}
		if contextErr != nil {
			selectionErr.Kind = providerselect.SelectionProviderContext
			selectionErr.Cause = contextErr
		}
		return selectionErr
	}
	for i := 0; i < missing; {
		candidates, err := providerselect.Select(inventory, bucket.ProviderSelectionStrategy, missing-i, excluded, hasTrusted)
		if err == nil && len(candidates) == 0 {
			err = &providerselect.SelectionError{
				Kind:        providerselect.SelectionTemporary,
				Diagnostics: providerselect.Diagnose(inventory, missing-i, excluded, hasTrusted),
			}
		}
		if err != nil {
			var selectionErr *providerselect.SelectionError
			if errors.As(err, &selectionErr) && selectionErr.Kind == providerselect.SelectionTemporary &&
				(selectionErr.Diagnostics.NeedsRefresh() || len(inventory.Candidates) == 0) && !refreshed {
				refreshed = true
				if refreshErr := h.deps.Observability.RefreshProviderStates(ctx); refreshErr != nil {
					selectionErr.Cause = refreshErr
				} else {
					inventory, hasTrusted, err = h.selectionInventory(ctx, bucket.ID)
					if err != nil {
						selectionErr.Cause = err
						return nil, finalize(selectionErr)
					}
					for id, candidate := range failedCandidates {
						found := false
						for j, current := range inventory.Candidates {
							if current.ID.String() == id {
								inventory.Candidates[j] = candidate
								found = true
								break
							}
						}
						if !found {
							inventory.Candidates = append(inventory.Candidates, candidate)
						}
					}
					for _, binding := range selected {
						hasTrusted = hasTrusted || inventory.Admission.Trusted(idtypes.OnChainIDFromSDK(binding.Target.ProviderID()))
					}
					continue
				}
			}
			if selectionErr != nil {
				return nil, finalize(selectionErr)
			}
			return nil, err
		}
		candidate := candidates[0]
		target, openTargetErr := h.deps.Storage.OpenProviderTarget(ctx, candidate.ID.SDK(), storage.NewProviderContextOptions{DataSetMetadata: map[string]string{"bucket": bucket.Name}})
		if openTargetErr == nil && target == nil {
			openTargetErr = errors.New("provider returned no context")
		}
		if openTargetErr != nil {
			openErr = errors.Join(openErr, openTargetErr)
			if !synapse.IsProviderUnavailable(openTargetErr) {
				contextErr = errors.Join(contextErr, openTargetErr)
			}
			failedCandidates[candidate.ID.String()] = candidate
			excluded[candidate.ID.String()] = providerselect.ExclusionContextOpenFailed
			continue
		}
		excluded[candidate.ID.String()] = providerselect.ExclusionSelected
		selected = append(selected, SelectedBinding{CopyIndex: indexes[i], Target: target, Admission: &inventory.Admission})
		hasTrusted = hasTrusted || inventory.Admission.Trusted(candidate.ID)
		i++
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
	return selected, nil
}

func (h *Selector) selectionInventory(ctx context.Context, bucketID int64) (providerselect.Inventory, bool, error) {
	inventory, err := h.deps.Observability.SelectionInventory(ctx, h.deps.AnchorProviderTier)
	if err != nil {
		return inventory, false, err
	}
	if err := h.deps.Repositories.EnrichProviderCandidates(ctx, &inventory); err != nil {
		return inventory, false, err
	}
	_, hasTrusted, err := h.deps.Repositories.ProviderSelectionState(ctx, bucketID, 0, inventory.Admission)
	return inventory, hasTrusted, err
}

func (h *Selector) bindingContextError(ctx context.Context, bucketID int64, providerID idtypes.OnChainID, count int, excluded map[string]providerselect.ExclusionReason, cause error) error {
	inventory := providerselect.Inventory{Admission: providerselect.Admission{Tier: h.deps.AnchorProviderTier}}
	hasTrusted := false
	if h.deps.Observability != nil {
		if current, trusted, err := h.selectionInventory(ctx, bucketID); err == nil {
			inventory, hasTrusted = current, trusted
		}
	}
	excluded[providerID.String()] = providerselect.ExclusionContextOpenFailed
	found := false
	for _, candidate := range inventory.Candidates {
		found = found || candidate.ID.Equal(providerID)
	}
	if !found {
		inventory.Candidates = append(inventory.Candidates, providerselect.Candidate{ID: providerID})
	}
	diagnostics := providerselect.Diagnose(inventory, count, excluded, hasTrusted)
	return &providerselect.SelectionError{Kind: providerselect.SelectionProviderContext, Cause: cause, Diagnostics: diagnostics}
}
