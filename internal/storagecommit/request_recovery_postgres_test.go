//go:build postgres

package storagecommit_test

import (
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/testutil"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestReleasedRequestRecoveryPostgres(t *testing.T) {
	c := seedUnacknowledgedAttemptWithDB(t, testutil.NewTestPostgresDB(t))
	releaseLegacyRequest(t, c)
	insertReleasedRequest(t, c, "same-request-refused", strings.ToUpper(testutil.CommitExtraDataHex(unacknowledgedNonce)), storagecommit.ReleaseProviderRejected)
	c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
	result, err := c.advance(t, c.attemptedAt.Add(time.Hour), storagecommit.AdvanceInput{})
	if err != nil || result.State != storagecommit.AdvanceConfirmed {
		t.Fatalf("historical PostgreSQL recovery = %#v err=%v", result, err)
	}
	settleNonceResult(t, c, result)
}
