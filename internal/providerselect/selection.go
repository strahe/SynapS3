// Package providerselect defines provider admission and ranking without I/O.
package providerselect

import (
	"errors"
	"slices"

	"github.com/strahe/synaps3/internal/types"
)

type Tier string

const (
	TierApproved Tier = "approved"
	TierEndorsed Tier = "endorsed"
	TierNone     Tier = "none"
)

func (t Tier) Valid() bool {
	//exhaustive:enforce
	switch t {
	case TierApproved, TierEndorsed, TierNone:
		return true
	default:
		return false
	}
}

type Strategy string

const (
	StrategyDistribution Strategy = "distribution"
	StrategySpeed        Strategy = "speed"
)

func (s Strategy) Valid() bool {
	//exhaustive:enforce
	switch s {
	case StrategyDistribution, StrategySpeed:
		return true
	default:
		return false
	}
}

type Candidate struct {
	ID             types.OnChainID
	Active         bool
	HasPDP         bool
	HasProfile     bool
	Healthy        bool
	Fresh          bool
	ServiceURL     string
	ProfileURL     string
	Load           int
	BytesPerSecond int64
	TieBreak       uint64
}

func (c Candidate) IneligibleReason() string {
	switch {
	case !c.HasProfile && c.Fresh && c.Healthy:
		return "profile_missing"
	case !c.Fresh:
		return "observation_stale"
	case c.ID.IsZero() || !c.Active || !c.HasPDP || !c.Healthy || c.ServiceURL == "":
		return "provider_unavailable"
	case !c.HasProfile:
		return "profile_missing"
	case c.ServiceURL != c.ProfileURL:
		return "profile_url_changed"
	default:
		return ""
	}
}

// Admission is selection-time evidence, never a persisted provider policy.
type Admission struct {
	Tier       Tier
	TrustedIDs []types.OnChainID
}

func (a Admission) Trusted(id types.OnChainID) bool {
	return a.Tier == TierNone || slices.ContainsFunc(a.TrustedIDs, id.Equal)
}

func (a Admission) Satisfied(remainingTrusted bool, targets ...types.OnChainID) bool {
	return a.Tier == TierNone || remainingTrusted || slices.ContainsFunc(targets, a.Trusted)
}

type Inventory struct {
	Candidates []Candidate
	Admission  Admission
}

var (
	ErrNoTrustedProvider    = errors.New("no eligible provider meets the required provider tier")
	ErrInventoryUnavailable = errors.New("provider selection inventory unavailable")
)

// Select reserves a trusted candidate first when the remaining set needs one.
// Random values are supplied by the caller; they only break exact ranking ties.
func Select(in Inventory, strategy Strategy, count int, excluded map[string]bool, hasTrusted bool) ([]Candidate, error) {
	if !in.Admission.Tier.Valid() || !strategy.Valid() {
		return nil, ErrInventoryUnavailable
	}
	eligible := make([]Candidate, 0, len(in.Candidates))
	for _, c := range in.Candidates {
		if c.IneligibleReason() == "" && !excluded[c.ID.String()] {
			eligible = append(eligible, c)
		}
	}
	slices.SortFunc(eligible, func(a, b Candidate) int {
		if strategy == StrategyDistribution {
			if n := compare(a.Load, b.Load); n != 0 {
				return n
			}
		}
		if n := compare(b.BytesPerSecond, a.BytesPerSecond); n != 0 {
			return n
		}
		if n := compare(a.Load, b.Load); n != 0 {
			return n
		}
		return compare(a.TieBreak, b.TieBreak)
	})
	selected := make([]Candidate, 0, max(count, 0))
	if count <= 0 {
		return selected, nil
	}
	if !in.Admission.Satisfied(hasTrusted) {
		i := slices.IndexFunc(eligible, func(c Candidate) bool { return in.Admission.Trusted(c.ID) })
		if i < 0 {
			return nil, ErrNoTrustedProvider
		}
		selected = append(selected, eligible[i])
		eligible = slices.Delete(eligible, i, i+1)
	}
	for _, c := range eligible {
		if len(selected) == count {
			break
		}
		selected = append(selected, c)
	}
	return selected, nil
}

func compare[T ~int | ~int64 | ~uint64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
