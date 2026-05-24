package observability

import "testing"

func TestAppendReasonCodeDeduplicatesReasons(t *testing.T) {
	reasons := []ReasonCode{ReasonChainLookupFailed}

	reasons = AppendReasonCode(reasons, "")
	reasons = AppendReasonCode(reasons, ReasonChainLookupFailed)
	reasons = AppendReasonCode(reasons, ReasonLocalStatusNotReady)

	if len(reasons) != 2 ||
		reasons[0] != ReasonChainLookupFailed ||
		reasons[1] != ReasonLocalStatusNotReady {
		t.Fatalf("reasons = %#v, want existing reason plus new non-empty reason", reasons)
	}
}
