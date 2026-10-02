package testutil

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
)

// CommitStorageCopy drives one copy to committed the way production does: a
// commit attempt is recorded in the ledger first, and the copy then projects
// that confirmed row. A committed copy without confirmed evidence is refused by
// the schema, so tests cannot shortcut this.
func CommitStorageCopy(
	t *testing.T,
	db bun.IDB,
	repos *repository.Repositories,
	input repository.MarkUploadCopyCommittedInput,
) {
	t.Helper()
	ctx := context.Background()
	if input.StorageCopyID == 0 {
		copyRow := new(model.StorageCopy)
		if err := db.NewSelect().
			Model(copyRow).
			Where("content_id = ? AND copy_index = ?", input.ContentID, input.CopyIndex).
			Scan(ctx); err != nil {
			t.Fatalf("loading copy for content %d slot %d: %v", input.ContentID, input.CopyIndex, err)
		}
		input.StorageCopyID = copyRow.ID
	}
	copyRow := new(model.StorageCopy)
	if err := db.NewSelect().Model(copyRow).Where("id = ?", input.StorageCopyID).Scan(ctx); err != nil {
		t.Fatalf("loading copy %d: %v", input.StorageCopyID, err)
	}
	if input.CommitExtraDataHex == "" {
		input.CommitExtraDataHex = "abcd"
	}
	if input.CommitTransactionID == "" {
		input.CommitTransactionID = fmt.Sprintf("tx-%d", input.StorageCopyID)
	}
	if input.CommitConfirmedTransactionID == "" {
		input.CommitConfirmedTransactionID = input.CommitTransactionID
	}
	if input.CommitAttemptID == "" {
		input.CommitAttemptID = fmt.Sprintf("attempt-%d-%d", input.ContentID, input.StorageCopyID)
	}
	now := time.Now()
	statusURL := "https://provider.example/status/" + input.CommitAttemptID
	attempt := &storagecommit.Attempt{
		AttemptID: input.CommitAttemptID, ContentID: copyRow.ContentID,
		StorageDataSetID: copyRow.StorageDataSetID, Status: storagecommit.AttemptStatusAttempted,
		ExtraDataHex: &input.CommitExtraDataHex, TransactionID: &input.CommitTransactionID,
		StatusURL:   &statusURL,
		AttemptedAt: &now, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := db.NewInsert().Model(attempt).Exec(ctx); err != nil {
		t.Fatalf("seeding commit attempt for copy %d: %v", input.StorageCopyID, err)
	}
	// Confirmation lands on a copy the coordinator already moved to committing.
	if _, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusCommitting).
		Set("commit_extra_data_hex = ?", input.CommitExtraDataHex).
		Set("updated_at = ?", now).
		Where("id = ?", input.StorageCopyID).
		Exec(ctx); err != nil {
		t.Fatalf("moving copy %d to committing: %v", input.StorageCopyID, err)
	}
	if err := repos.Contents.MarkUploadCopyCommitted(ctx, input); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
}

var addPiecesExtraDataArguments = func() abi.Arguments {
	var arguments abi.Arguments
	for _, typeName := range []string{"uint256", "string[][]", "string[][]", "bytes"} {
		argumentType, err := abi.NewType(typeName, "", nil)
		if err != nil {
			panic(err)
		}
		arguments = append(arguments, abi.Argument{Type: argumentType})
	}
	return arguments
}()

// CommitExtraData encodes add-pieces extra data signed with nonce, the shape a
// storage commit reads its nonce from.
func CommitExtraData(nonce uint64) []byte {
	extraData, err := addPiecesExtraDataArguments.Pack(new(big.Int).SetUint64(nonce), [][]string{}, [][]string{}, []byte{0x5a})
	if err != nil {
		panic(err)
	}
	return extraData
}

// CommitExtraDataHex is CommitExtraData as the lowercase hex a copy stores.
func CommitExtraDataHex(nonce uint64) string {
	return hex.EncodeToString(CommitExtraData(nonce))
}

// MockCommitNonces is chain nonce state a test sets directly.
type MockCommitNonces struct {
	mu     sync.Mutex
	nonces map[string]synapse.ClientNonceState
	pieces map[string]cid.Cid
	// Err fails every read while set.
	Err   error
	reads int
}

var _ synapse.CommitNonceReader = (*MockCommitNonces)(nil)

// Consume records that the request signed with nonce added pieceCID to the data
// set as pieceID.
func (m *MockCommitNonces) Consume(nonce uint64, dataSetID, pieceID sdktypes.BigInt, pieceCID cid.Cid) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nonces == nil {
		m.nonces = make(map[string]synapse.ClientNonceState)
		m.pieces = make(map[string]cid.Cid)
	}
	next, err := sdktypes.BigIntFromBig(new(big.Int).Add(pieceID.Big(), big.NewInt(1)))
	if err != nil {
		panic(err)
	}
	m.nonces[new(big.Int).SetUint64(nonce).String()] = synapse.ClientNonceState{Consumed: true, DataSetID: dataSetID.Copy(), NextPieceID: next}
	m.pieces[dataSetID.String()+"/"+pieceID.String()] = pieceCID
}

// ConsumeExtraData is Consume for the nonce signed into extraData.
func (m *MockCommitNonces) ConsumeExtraData(extraData []byte, dataSetID, pieceID sdktypes.BigInt, pieceCID cid.Cid) {
	nonce, err := storagecommit.ExtraDataNonce(extraData)
	if err != nil {
		panic(err)
	}
	m.Consume(nonce.Big().Uint64(), dataSetID, pieceID, pieceCID)
}

// Reads counts the nonce reads made so far.
func (m *MockCommitNonces) Reads() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reads
}

func (m *MockCommitNonces) ClientNonce(_ context.Context, nonce sdktypes.BigInt) (synapse.ClientNonceState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.Err != nil {
		return synapse.ClientNonceState{}, m.Err
	}
	return m.nonces[nonce.String()], nil
}

func (m *MockCommitNonces) PieceCIDAt(_ context.Context, dataSetID, pieceID sdktypes.BigInt) (cid.Cid, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return cid.Undef, m.Err
	}
	pieceCID, ok := m.pieces[dataSetID.String()+"/"+pieceID.String()]
	if !ok {
		return cid.Undef, nil
	}
	return pieceCID, nil
}
