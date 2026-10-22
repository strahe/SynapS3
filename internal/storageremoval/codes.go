package storageremoval

import "fmt"

type AttentionCode string

const (
	AttentionIdentityMismatch    AttentionCode = "identity_mismatch"
	AttentionPayerUnderfunded    AttentionCode = "payer_underfunded"
	AttentionProviderUnreachable AttentionCode = "provider_unreachable"
	AttentionConfirmationTimeout AttentionCode = "confirmation_timeout"
	AttentionTransactionReverted AttentionCode = "transaction_reverted"
	AttentionRemovalStalled      AttentionCode = "removal_stalled"
	AttentionCleanupStalled      AttentionCode = "cleanup_stalled"
)

func (c AttentionCode) Valid() bool {
	//exhaustive:enforce
	switch c {
	case AttentionIdentityMismatch, AttentionPayerUnderfunded, AttentionProviderUnreachable, AttentionConfirmationTimeout, AttentionTransactionReverted, AttentionRemovalStalled, AttentionCleanupStalled:
		return true
	default:
		return false
	}
}

func (c AttentionCode) IsStop() bool {
	//exhaustive:enforce
	switch c {
	case AttentionIdentityMismatch, AttentionPayerUnderfunded, AttentionProviderUnreachable, AttentionTransactionReverted:
		return true
	case AttentionConfirmationTimeout, AttentionRemovalStalled, AttentionCleanupStalled:
		return false
	default:
		return true
	}
}

func ParseAttentionCode(value string) (AttentionCode, error) {
	code := AttentionCode(value)
	if !code.Valid() {
		return "", fmt.Errorf("unknown removal attention code %q", value)
	}
	return code, nil
}
