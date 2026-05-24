package observability

// AppendReasonCode appends a non-empty reason code if it is not already present.
func AppendReasonCode(reasons []ReasonCode, reason ReasonCode) []ReasonCode {
	if reason == "" {
		return reasons
	}
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}
