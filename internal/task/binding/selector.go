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
	excluded := make(map[string]bool)
	usedIndexes := make(map[int]struct{}, len(bindings))
	for i := range bindings {
		binding := &bindings[i]
		if binding.Status != model.StorageDataSetStatusRetired {
			excluded[binding.ProviderID.String()] = true
		}
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
			return nil, err
		}
		selected = append(selected, SelectedBinding{CopyIndex: binding.CopyIndex, Target: target})
	}
	missing := targetCount - len(selected)
	if missing <= 0 {
		sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
		return selected, nil
	}
	// Free positions come from the bucket's own slot rows, not from the global
	// maximum: a data set can only exist on a slot the bucket opened, and the
	// foreign key would reject anything else.
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
	if missing > len(indexes) {
		missing = len(indexes)
	}
	if h.deps.Observability == nil {
		return nil, errors.New("observability service is unavailable")
	}
	in, err := h.deps.Observability.SelectionInventory(ctx, h.deps.AnchorProviderTier)
	if err != nil {
		return nil, err
	}
	if err := h.deps.Repositories.EnrichProviderCandidates(ctx, &in); err != nil {
		return nil, err
	}
	_, hasTrusted, err := h.deps.Repositories.ProviderSelectionState(ctx, bucket.ID, 0, in.Admission)
	if err != nil {
		return nil, err
	}
	var openErr error
	for i := 0; i < missing; {
		candidates, err := providerselect.Select(in, bucket.ProviderSelectionStrategy, 1, excluded, hasTrusted)
		if err != nil {
			if openErr != nil {
				return nil, openErr
			}
			return nil, err
		}
		if len(candidates) == 0 {
			break
		}
		candidate := candidates[0]
		excluded[candidate.ID.String()] = true
		target, err := h.deps.Storage.OpenProviderTarget(ctx, candidate.ID.SDK(), storage.NewProviderContextOptions{DataSetMetadata: map[string]string{"bucket": bucket.Name}})
		if err != nil {
			openErr = err
			continue
		}
		if target == nil {
			openErr = errors.New("provider returned no context")
			continue
		}
		selected = append(selected, SelectedBinding{CopyIndex: indexes[i], Target: target, Admission: &in.Admission})
		hasTrusted = hasTrusted || in.Admission.Trusted(candidate.ID)
		i++
	}
	if len(selected) < targetCount && openErr != nil {
		return nil, openErr
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].CopyIndex < selected[j].CopyIndex })
	return selected, nil
}
