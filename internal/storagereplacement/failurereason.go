package storagereplacement

// FailureReason identifies a permanent failure whose recovery action differs
// from retrying the same approved target.
type FailureReason string

const (
	FailureReasonTargetInUse FailureReason = "target_in_use"
)

// Valid reports whether the value is a known permanent failure reason.
func (r FailureReason) Valid() bool {
	return r == FailureReasonTargetInUse
}
