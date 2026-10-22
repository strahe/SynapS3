package storageremoval

import (
	"math/big"
	"slices"
	"testing"
	"time"
)

func TestSendAuthorizationAndQueueSnapshot(t *testing.T) {
	got, err := NextSendSet([]int64{1, 2, 3}, []int64{3, 1, 3})
	if err != nil || !slices.Equal(got, []int64{1, 3}) {
		t.Fatalf("subset = %v, %v", got, err)
	}
	if _, err := NextSendSet([]int64{1, 3}, []int64{2, 3}); err == nil {
		t.Fatal("exposed authorization expanded")
	}
	h, later := uint64(100), uint64(101)
	room := QueueRoom(1980, h, []QueueReservation{{Count: 4}, {Count: 5, QueuedBlock: &h}, {Count: 6, QueuedBlock: &later}})
	if room != 10 {
		t.Fatalf("room = %d, want 10", room)
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

func TestReserveAndRetryObservation(t *testing.T) {
	if got := ReserveTopUp(big.NewInt(10), big.NewInt(7), big.NewInt(4), big.NewInt(20)); got.Int64() != 17 {
		t.Fatalf("top up = %s", got)
	}
	if got := ReserveTopUp(big.NewInt(11), big.NewInt(7), big.NewInt(4), big.NewInt(20)); got.Sign() != 0 {
		t.Fatalf("threshold top up = %s", got)
	}
	first := time.Now().Add(-time.Hour)
	retry := time.Now()
	if !ConfirmationTimeoutStart(first, retry).Equal(retry) {
		t.Fatal("successor inherited an expired confirmation deadline")
	}
	if !AutoResendSupported("3.5.0") || AutoResendSupported("3.6.0") {
		t.Fatal("unverified protocol version admitted")
	}
}
