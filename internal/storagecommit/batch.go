package storagecommit

import (
	"math/big"
	"time"

	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
)

// SealInput is what decides whether a collecting request is signed now.
type SealInput struct {
	Members int
	// OldestJoinedAt is when the longest-waiting member became ready.
	OldestJoinedAt time.Time
	Now            time.Time
	MaxPieces      int
	MaxWait        time.Duration
	// CapacityFree reports that the data set has room for another submitted
	// request.
	CapacityFree bool
	// ReadyWaiting reports that a signed request of the data set is waiting
	// for that room already.
	ReadyWaiting bool
	// TransfersInFlight reports that more copies of the data set may join soon.
	TransfersInFlight bool
	// Draining means the data set takes no new copies.
	Draining bool
}

// ShouldSeal reports whether a collecting request should be signed now and,
// when it should not, how long it may wait before asking again. A zero wait
// means wait to be woken.
func ShouldSeal(in SealInput) (bool, time.Duration) {
	switch {
	case in.Members <= 0:
		return false, 0
	case in.Members >= in.MaxPieces, in.Draining:
		return true, 0
	case !in.CapacityFree || in.ReadyWaiting:
		// A request signed now could not be sent before the copies arriving
		// meanwhile, so it keeps collecting until there is room.
		return false, 0
	case !in.TransfersInFlight:
		return true, 0
	}
	if waited := in.Now.Sub(in.OldestJoinedAt); waited < in.MaxWait {
		return false, in.MaxWait - waited
	}
	return true, 0
}

// MaxPieces is how many pieces one request to the data set may carry.
// Data sets below the deployment's compact cutoff keep the legacy piece limit;
// every request also stays within the add-pieces message size, which the
// configured maximum is validated against.
func MaxPieces(dataSetID idtypes.OnChainID, legacyPieceStorageIDLimit uint64, configured int) int {
	if legacyPieceStorageIDLimit != 0 &&
		dataSetID.SDK().Big().Cmp(new(big.Int).SetUint64(legacyPieceStorageIDLimit)) < 0 {
		return min(configured, pdp.MaxLegacyAddPiecesBatchSize)
	}
	return configured
}
