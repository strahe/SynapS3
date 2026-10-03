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
	idtypes "github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
)

// CommitCopyInput names a copy to commit and the evidence it ends with.
type CommitCopyInput struct {
	StorageCopyID int64
	ContentID     int64
	CopyIndex     int
	PieceCID      string
	PieceID       *idtypes.OnChainID
	RetrievalURL  string
	TransactionID string
}

// CommitStorageCopy commits one copy the way a confirmed registration leaves
// it: a confirmed single-piece request is recorded first, and the copy then
// names it. A committed copy without a confirmed request is refused by the
// schema, so tests cannot shortcut this.
func CommitStorageCopy(t *testing.T, db bun.IDB, _ *repository.Repositories, input CommitCopyInput) {
	t.Helper()
	ctx := context.Background()
	copyRow := new(model.StorageCopy)
	q := db.NewSelect().Model(copyRow)
	if input.StorageCopyID != 0 {
		q = q.Where("id = ?", input.StorageCopyID)
	} else {
		q = q.Where("content_id = ? AND copy_index = ?", input.ContentID, input.CopyIndex)
	}
	if err := q.Scan(ctx); err != nil {
		t.Fatalf("loading copy for content %d slot %d: %v", input.ContentID, input.CopyIndex, err)
	}
	content := new(model.StorageContent)
	if err := db.NewSelect().Model(content).Where("id = ?", copyRow.ContentID).Scan(ctx); err != nil {
		t.Fatalf("loading content %d: %v", copyRow.ContentID, err)
	}
	pieceCID := input.PieceCID
	if pieceCID == "" && content.PieceCID != nil {
		pieceCID = *content.PieceCID
	}
	if pieceCID == "" {
		t.Fatalf("committing copy %d needs a piece CID", copyRow.ID)
	}
	pieceID := idtypes.NewOnChainID(uint64(copyRow.ID))
	if input.PieceID != nil {
		pieceID = *input.PieceID
	} else if copyRow.PieceID != nil {
		pieceID = *copyRow.PieceID
	}
	retrievalURL := input.RetrievalURL
	if retrievalURL == "" && copyRow.RetrievalURL != nil {
		retrievalURL = *copyRow.RetrievalURL
	}
	if retrievalURL == "" {
		retrievalURL = "https://provider.example/piece/" + pieceCID
	}
	transactionID := input.TransactionID
	if transactionID == "" {
		transactionID = fmt.Sprintf("tx-%d", copyRow.ID)
	}
	now := time.Now()
	requestID := fmt.Sprintf("request-%d-%d", copyRow.ContentID, copyRow.ID)
	extra := CommitExtraDataHex(uint64(copyRow.ID))
	statusURL := "https://provider.example/status/" + transactionID
	request := &storagecommit.Request{
		RequestID: requestID, StorageDataSetID: copyRow.StorageDataSetID, Status: storagecommit.RequestStatusConfirmed,
		PieceCount: 1, ExtraDataHex: &extra, SealedAt: &now, FirstSentAt: &now, Sends: 1,
		SubmittedAt: &now, LastSentAt: &now, TransactionID: &transactionID, StatusURL: &statusURL,
		FirstPieceID: &pieceID, ConfirmedTransactionID: &transactionID, ConfirmedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := db.NewInsert().Model(request).Exec(ctx); err != nil {
		t.Fatalf("seeding commit request for copy %d: %v", copyRow.ID, err)
	}
	piece := &storagecommit.RequestPiece{
		RequestID: requestID, Position: 0, ContentID: copyRow.ContentID,
		StorageDataSetID: copyRow.StorageDataSetID, PieceCID: pieceCID, CreatedAt: now,
	}
	if _, err := db.NewInsert().Model(piece).Exec(ctx); err != nil {
		t.Fatalf("seeding commit request piece for copy %d: %v", copyRow.ID, err)
	}
	if _, err := db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("piece_cid = COALESCE(piece_cid, ?)", pieceCID).
		Set("error_message = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", copyRow.ContentID).
		Exec(ctx); err != nil {
		t.Fatalf("recording piece CID for content %d: %v", copyRow.ContentID, err)
	}
	if _, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusCommitted).
		Set("piece_id = ?", &pieceID).
		Set("retrieval_url = ?", retrievalURL).
		Set("commit_request_id = ?", requestID).
		Set("commit_position = 0").
		Set("commit_request_status = ?", string(storagecommit.RequestStatusConfirmed)).
		Set("commit_ready_at = NULL").
		Set("active_task_id = NULL").
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", copyRow.ID).
		Exec(ctx); err != nil {
		t.Fatalf("committing copy %d: %v", copyRow.ID, err)
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

// ConsumeRequest records that the request signed into extraData added
// pieceCIDs, in order, to the data set starting at firstPieceID.
func (m *MockCommitNonces) ConsumeRequest(extraData []byte, dataSetID, firstPieceID sdktypes.BigInt, pieceCIDs []cid.Cid) {
	nonce, err := storagecommit.ExtraDataNonce(extraData)
	if err != nil {
		panic(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nonces == nil {
		m.nonces = make(map[string]synapse.ClientNonceState)
		m.pieces = make(map[string]cid.Cid)
	}
	first := firstPieceID.Big()
	for i, pieceCID := range pieceCIDs {
		id := new(big.Int).Add(first, big.NewInt(int64(i)))
		m.pieces[dataSetID.String()+"/"+id.String()] = pieceCID
	}
	next, err := sdktypes.BigIntFromBig(new(big.Int).Add(first, big.NewInt(int64(len(pieceCIDs)))))
	if err != nil {
		panic(err)
	}
	m.nonces[nonce.String()] = synapse.ClientNonceState{Consumed: true, DataSetID: dataSetID.Copy(), NextPieceID: next}
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
