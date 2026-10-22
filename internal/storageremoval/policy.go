package storageremoval

import (
	"errors"
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

func RemovalStalledAfter(period, window time.Duration) time.Duration { return 2*period + window }

func AutoResendSupported(version string) bool { return strings.TrimSpace(version) == "3.5.0" }
