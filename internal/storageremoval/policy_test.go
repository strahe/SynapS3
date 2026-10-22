package storageremoval

import (
	"slices"
	"testing"
	"time"
)

func TestNextSendSet(t *testing.T) {
	got, err := NextSendSet([]int64{1, 2, 3}, []int64{3, 1, 3})
	if err != nil || !slices.Equal(got, []int64{1, 3}) {
		t.Fatalf("subset = %v, %v", got, err)
	}
	if _, err := NextSendSet([]int64{1, 3}, []int64{2, 3}); err == nil {
		t.Fatal("exposed authorization expanded")
	}
}

func TestFailedReceiptRequiresCoherentEvidence(t *testing.T) {
	for _, test := range []struct {
		pieces []ReceiptPiece
		stable bool
		want   FailedReceiptClass
	}{
		{nil, true, FailedReceiptInsufficient},
		{[]ReceiptPiece{{BlockNumber: 99}}, true, FailedReceiptInsufficient},
		{[]ReceiptPiece{{BlockNumber: 100}}, false, FailedReceiptInsufficient},
		{[]ReceiptPiece{{Queued: true, BlockNumber: 100}, {BlockNumber: 100}}, true, FailedReceiptCollision},
		{[]ReceiptPiece{{Removed: true, BlockNumber: 100}}, true, FailedReceiptCollision},
		{[]ReceiptPiece{{BlockNumber: 100}}, true, FailedReceiptReverted},
	} {
		if got := ClassifyFailedReceipt(test.pieces, 100, test.stable); got != test.want {
			t.Fatalf("class = %v, want %v", got, test.want)
		}
	}
}

func TestRetryObservation(t *testing.T) {
	first := time.Now().Add(-time.Hour)
	retry := time.Now()
	if !ConfirmationTimeoutStart(first, retry).Equal(retry) {
		t.Fatal("successor inherited an expired confirmation deadline")
	}
	if !AutoResendSupported("3.5.0") || AutoResendSupported("3.6.0") {
		t.Fatal("unverified protocol version admitted")
	}
}
