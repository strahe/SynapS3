package synapse

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synapse-go/payments"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestWalletSubmissionRetainsTransactionEvidenceAndError(t *testing.T) {
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{Nonce: 4, To: new(common.Address), Data: []byte{0xab, 0xcd, 0xef}})
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("connection closed")
	echoedCause := fmt.Errorf("RPC rejected 0X%X", raw)
	encoded := hex.EncodeToString(raw)
	mixedCase := strings.ToUpper(encoded[:len(encoded)/2]) + encoded[len(encoded)/2:]
	unicodeCause := errors.New("İ: 0x" + encoded)
	invalidUTF8Cause := errors.New("\xff\xfe bad bytes: 0x" + encoded)
	repeatedCause := fmt.Errorf("RPC rejected 0x%s; %s", mixedCase, encoded)
	conflictingHash := common.HexToHash("0x123")
	for _, tc := range []struct {
		name         string
		result       *sdktypes.WriteResult
		err          error
		wantHash     string
		wantRaw      bool
		wantErr      bool
		wantCause    error
		wantMessage  string
		wantConflict bool
	}{
		{name: "accepted", result: &sdktypes.WriteResult{Hash: tx.Hash()}, wantHash: tx.Hash().Hex()},
		{name: "broadcast uncertain", err: fmt.Errorf("SDK write: %w", &sdktypes.BroadcastError{Transaction: tx, Cause: cause}), wantHash: tx.Hash().Hex(), wantRaw: true, wantErr: true, wantCause: cause},
		{name: "remote echoes signed bytes", err: &sdktypes.BroadcastError{Transaction: tx, Cause: echoedCause}, wantHash: tx.Hash().Hex(), wantRaw: true, wantErr: true, wantCause: echoedCause},
		{name: "Unicode prefix", err: &sdktypes.BroadcastError{Transaction: tx, Cause: unicodeCause}, wantHash: tx.Hash().Hex(), wantRaw: true, wantErr: true, wantCause: unicodeCause, wantMessage: "İ: [signed transaction]"},
		{name: "invalid UTF-8 prefix", err: &sdktypes.BroadcastError{Transaction: tx, Cause: invalidUTF8Cause}, wantHash: tx.Hash().Hex(), wantRaw: true, wantErr: true, wantCause: invalidUTF8Cause, wantMessage: "\xff\xfe bad bytes: [signed transaction]"},
		{name: "repeated mixed-case bytes", err: &sdktypes.BroadcastError{Transaction: tx, Cause: repeatedCause}, wantHash: tx.Hash().Hex(), wantRaw: true, wantErr: true, wantCause: repeatedCause, wantMessage: "RPC rejected [signed transaction]; [signed transaction]"},
		{name: "hash with receipt error", result: &sdktypes.WriteResult{Hash: tx.Hash()}, err: cause, wantHash: tx.Hash().Hex(), wantErr: true, wantCause: cause},
		{name: "preflight error", err: cause, wantErr: true, wantCause: cause},
		{name: "approval already satisfied", err: payments.ErrNothingToFund, wantErr: true, wantCause: payments.ErrNothingToFund},
		{name: "missing result", wantErr: true},
		{name: "missing hash", result: &sdktypes.WriteResult{}, wantErr: true},
		{name: "conflicting evidence", result: &sdktypes.WriteResult{Hash: conflictingHash}, err: &sdktypes.BroadcastError{Transaction: tx, Cause: echoedCause}, wantErr: true, wantCause: echoedCause, wantMessage: fmt.Sprintf("result hash %s, signed transaction hash %s", conflictingHash.Hex(), tx.Hash().Hex()), wantConflict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := WalletSubmissionFromWriteResult(tc.result, tc.err, "wallet write")
			if (err != nil) != tc.wantErr || got.TxHash != tc.wantHash || (len(got.SignedTransaction) > 0) != tc.wantRaw {
				t.Fatalf("submission = hash %q, raw %t, error %v", got.TxHash, len(got.SignedTransaction) > 0, err)
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Fatalf("error %v lost cause %v", err, tc.wantCause)
			}
			if tc.wantConflict && !errors.Is(err, ErrWalletReplayInvalidEvidence) {
				t.Fatalf("error %v lost evidence conflict", err)
			}
			if tc.wantMessage != "" && (err == nil || !strings.Contains(err.Error(), tc.wantMessage)) {
				t.Fatalf("error %v does not preserve message %q", err, tc.wantMessage)
			}
			if tc.wantRaw && !bytes.Equal(got.SignedTransaction, raw) {
				t.Fatal("signed transaction bytes changed")
			}
			if err != nil && strings.Contains(strings.ToLower(err.Error()), hex.EncodeToString(raw)) {
				t.Fatal("error exposes signed transaction bytes")
			}
		})
	}
}
