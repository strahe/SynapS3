package storageremoval

import (
	"errors"
	"math/big"
	"slices"
	"strings"
	"time"
)

const MaxEnqueuedRemovals = 2000

// NextSendSet rejects members outside an exposed authorization's exact set.
// An empty previous set represents a batch without an exposed authorization.
func NextSendSet(previous, candidate []int64) ([]int64, error) {
	result := slices.Compact(slices.Sorted(slices.Values(candidate)))
	for _, id := range result {
		if id <= 0 || len(previous) > 0 && !slices.Contains(previous, id) {
			return nil, errors.New("removal send is not a subset of its authorization")
		}
	}
	return result, nil
}

type QueueReservation struct {
	Count int
	// Nil identifies a submitted batch whose sends may still reach the queue.
	QueuedBlock *uint64
}

func QueueRoom(snapshotLen int, snapshotBlock uint64, reservations []QueueReservation) int {
	room := MaxEnqueuedRemovals - max(0, snapshotLen)
	for _, reservation := range reservations {
		if reservation.QueuedBlock == nil || *reservation.QueuedBlock > snapshotBlock {
			room -= max(0, reservation.Count)
		}
	}
	return max(0, room)
}

type FailedReceiptClass int

const (
	FailedReceiptInsufficient FailedReceiptClass = iota
	FailedReceiptCollision
	FailedReceiptReverted
)

type ReceiptPiece struct {
	Removed     bool
	Queued      bool
	BlockNumber uint64
}

func ClassifyFailedReceipt(pieces []ReceiptPiece, receiptBlock uint64, receiptStable bool) FailedReceiptClass {
	if !receiptStable || len(pieces) == 0 {
		return FailedReceiptInsufficient
	}
	for _, piece := range pieces {
		if piece.BlockNumber < receiptBlock {
			return FailedReceiptInsufficient
		}
	}
	for _, piece := range pieces {
		if piece.Removed || piece.Queued {
			return FailedReceiptCollision
		}
	}
	return FailedReceiptReverted
}

func ConfirmationTimeoutStart(firstAttempted, workStarted time.Time) time.Time {
	if workStarted.After(firstAttempted) {
		return workStarted
	}
	return firstAttempted
}

func RemovalStalledAfter(period, window time.Duration) time.Duration   { return 2*period + window }
func CleanupStalledAt(endAt time.Time, period time.Duration) time.Time { return endAt.Add(2 * period) }

// ReserveTopUp returns the additional reserve required by the rail's threshold.
func ReserveTopUp(reserve, pending, threshold, target *big.Int) *big.Int {
	if reserve == nil || pending == nil || threshold == nil || target == nil {
		return new(big.Int)
	}
	minimum := new(big.Int).Add(pending, threshold)
	if reserve.Cmp(minimum) >= 0 {
		return new(big.Int)
	}
	needed := new(big.Int).Sub(new(big.Int).Add(pending, target), reserve)
	if needed.Sign() < 0 {
		return new(big.Int)
	}
	return needed
}

func AutoResendSupported(version string) bool { return strings.TrimSpace(version) == "3.5.0" }
