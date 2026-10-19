package walletoperation

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/storage"
)

var ErrIdentityChanged = errors.New("wallet request belongs to another wallet, network, or payment contract")

type ExecutionIdentity struct {
	Identity *storage.ContextIdentity `json:"identity,omitempty"`
	Payments *common.Address          `json:"payments,omitempty"`
	USDFC    *common.Address          `json:"usdfc,omitempty"`
}

func NewExecutionIdentity(identity storage.ContextIdentity, payments, usdfc common.Address) ExecutionIdentity {
	return ExecutionIdentity{Identity: &identity, Payments: &payments, USDFC: &usdfc}
}

func (identity ExecutionIdentity) Complete() bool {
	return identity.Identity != nil && identity.Identity.Payer != (common.Address{}) && identity.Identity.ChainID.IsValid() && identity.Identity.RecordKeeper != (common.Address{}) && identity.Payments != nil && *identity.Payments != (common.Address{}) && identity.USDFC != nil && *identity.USDFC != (common.Address{})
}

func (identity ExecutionIdentity) Match(other ExecutionIdentity) error {
	if !identity.Complete() || !other.Complete() || *identity.Identity != *other.Identity || *identity.Payments != *other.Payments || *identity.USDFC != *other.USDFC {
		return ErrIdentityChanged
	}
	return nil
}
