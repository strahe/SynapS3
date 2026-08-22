package storagereplacement

// WaitReason explains a recoverable pause so an operator can tell "nothing is
// wrong yet" from "something needs me". It is only meaningful while the status
// is StatusWaiting.
type WaitReason string

const (
	// WaitReasonReadableSource means no replica and no cached copy can supply
	// the content this item needs.
	WaitReasonReadableSource WaitReason = "readable_source"
	// WaitReasonTarget means the approved target provider is unreachable.
	WaitReasonTarget WaitReason = "target"
	// WaitReasonTargetCreating means the paid target service has not been
	// created yet.
	WaitReasonTargetCreating WaitReason = "target_creating"
	// WaitReasonTargetWritable means the created service is not writable yet.
	WaitReasonTargetWritable WaitReason = "target_writable"
	// WaitReasonFunding means the wallet cannot yet pay for the target service.
	WaitReasonFunding WaitReason = "funding"
	// WaitReasonProvider means a provider call failed in a way that resolves on
	// its own.
	WaitReasonProvider WaitReason = "provider"
	// WaitReasonTerminationEpoch means the chain has not yet reached the epoch
	// at which the old service ends.
	WaitReasonTerminationEpoch WaitReason = "termination_epoch"
	// WaitReasonSourceWrites means a write to the old provider is still in
	// flight, so it cannot be retired.
	WaitReasonSourceWrites WaitReason = "source_writes"
	// WaitReasonCoverage means some retained version is not yet readable on the
	// new provider.
	WaitReasonCoverage WaitReason = "coverage"
)

// Valid reports whether the value is a known wait reason.
func (r WaitReason) Valid() bool {
	switch r {
	case WaitReasonReadableSource, WaitReasonTarget, WaitReasonTargetCreating, WaitReasonTargetWritable,
		WaitReasonFunding, WaitReasonProvider,
		WaitReasonTerminationEpoch, WaitReasonSourceWrites, WaitReasonCoverage:
		return true
	default:
		return false
	}
}

// Message is the operator-facing explanation of the pause. It says what is
// being waited on, never an internal state name.
func (r WaitReason) Message() string {
	switch r {
	case WaitReasonReadableSource:
		return "Waiting for a readable copy of some stored content"
	case WaitReasonTarget:
		return "Waiting for the replacement provider to become reachable"
	case WaitReasonTargetCreating:
		return "Waiting for the replacement storage service to be created"
	case WaitReasonTargetWritable:
		return "Waiting for the replacement storage service to become writable"
	case WaitReasonFunding:
		return "Waiting for wallet funds to cover the replacement service"
	case WaitReasonProvider:
		return "Waiting for the storage provider to respond"
	case WaitReasonTerminationEpoch:
		return "Waiting for the old service to reach its end of term"
	case WaitReasonSourceWrites:
		return "Waiting for in-flight writes to the old provider to finish"
	case WaitReasonCoverage:
		return "Waiting for every retained version to be readable on the new provider"
	default:
		return "Waiting for a dependency"
	}
}
