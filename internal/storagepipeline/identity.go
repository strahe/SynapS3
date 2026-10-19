package storagepipeline

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/storage"
)

var (
	ErrExecutionIdentityMissing = errors.New("storage request has no complete signing identity")
	ErrExecutionIdentityChanged = errors.New("storage wallet, network, or contract differs from the recorded request")
)

// ExecutionIdentity binds a storage request to its signing domain and PDP state.
type ExecutionIdentity struct {
	Identity    *storage.ContextIdentity `json:"identity,omitempty"`
	PDPVerifier *common.Address          `json:"pdp_verifier,omitempty"`
}

func NewExecutionIdentity(identity storage.ContextIdentity, verifier common.Address) ExecutionIdentity {
	return ExecutionIdentity{Identity: &identity, PDPVerifier: &verifier}
}

func (identity ExecutionIdentity) Complete() bool {
	return identity.Identity != nil && identity.Identity.Payer != (common.Address{}) && identity.Identity.ChainID.IsValid() && identity.Identity.RecordKeeper != (common.Address{}) && identity.PDPVerifier != nil && *identity.PDPVerifier != (common.Address{})
}

func (identity ExecutionIdentity) Match(other ExecutionIdentity) error {
	if !identity.Complete() || !other.Complete() {
		return ErrExecutionIdentityMissing
	}
	if *identity.Identity != *other.Identity || *identity.PDPVerifier != *other.PDPVerifier {
		return ErrExecutionIdentityChanged
	}
	return nil
}

func (identity ExecutionIdentity) Present() bool {
	return identity.Identity != nil || identity.PDPVerifier != nil
}

func ExecutionIdentityBlocked(reason string) bool {
	switch reason {
	case "storage_identity_missing", "storage_identity_changed", "dataset_identity_changed", "dataset_identity_mismatch", "termination_identity_changed", "store_identity_mismatch", "store_identity_unverified", "commit_member_recovery_blocked":
		return true
	default:
		return false
	}
}

func ExecutionIdentityFailure(err error) string {
	if errors.Is(err, ErrExecutionIdentityMissing) {
		return "storage_identity_missing"
	}
	return "storage_identity_changed"
}

func IsExecutionIdentityError(err error) bool {
	return errors.Is(err, ErrExecutionIdentityMissing) || errors.Is(err, ErrExecutionIdentityChanged)
}
