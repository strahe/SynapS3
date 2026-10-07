package storagereplacement

// FailureReason identifies a permanent failure whose recovery action differs
// from retrying the same approved target.
type FailureReason string

const (
	FailureReasonTargetInUse    FailureReason = "target_in_use"
	FailureReasonTargetRejected FailureReason = "target_rejected"
)

// Valid reports whether the value is a known permanent failure reason.
func (r FailureReason) Valid() bool {
	//exhaustive:enforce
	switch r {
	case FailureReasonTargetInUse, FailureReasonTargetRejected:
		return true
	default:
		return false
	}
}
