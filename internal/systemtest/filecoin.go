//go:build systemtest

package systemtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	appTypes "github.com/strahe/synaps3/internal/types"
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

var errInvalidFilecoinSequence = errors.New("memory filecoin: invalid operation sequence")

type memoryDataSet struct {
	id       sdktypes.BigInt
	clientID sdktypes.BigInt
	provider sdktypes.BigInt
	metadata map[string]string
	withCDN  bool
	pieces   map[string]sdktypes.BigInt
}

type memoryPiece struct {
	content           []byte
	storedProviders   map[string]struct{}
	committedDataSets map[string]struct{}
}

// MemoryFilecoin is a concurrent, stateful Filecoin boundary for system tests.
type MemoryFilecoin struct {
	mu sync.RWMutex

	providers         []sdktypes.BigInt
	dataSets          map[string]*memoryDataSet
	pendingDataSets   map[string]sdktypes.BigInt
	submissions       map[string]sdktypes.BigInt
	commitSubmissions map[string]storage.CommitSubmission
	pieces            map[string]*memoryPiece
	nextDataSet       map[string]uint64
	nextPiece         uint64
	// terminated records the epoch at which each data set's service ends, and
	// epoch is the observed chain head. Tests advance the head to prove that
	// retirement waits for the chain rather than for the call returning.
	terminated map[string]int64
	epoch      int64
	// terminationDelay is how far ahead of the chain head a terminated service
	// ends. Zero means the end of term is already reached.
	terminationDelay int64
}

// MemoryFilecoinProviders is how many providers the fake offers. One more than
// a bucket's default replica count, so an approved replacement always has an
// unused provider to move to.
const MemoryFilecoinProviders = 4

// NewMemoryFilecoin creates deterministic active storage providers.
func NewMemoryFilecoin() *MemoryFilecoin {
	return &MemoryFilecoin{
		// The fourth provider is never used by a fresh bucket, so an approved
		// replacement always has somewhere to move to.
		providers: []sdktypes.BigInt{
			sdktypes.NewBigInt(101),
			sdktypes.NewBigInt(102),
			sdktypes.NewBigInt(103),
			sdktypes.NewBigInt(104),
		},
		dataSets:          make(map[string]*memoryDataSet),
		pendingDataSets:   make(map[string]sdktypes.BigInt),
		submissions:       make(map[string]sdktypes.BigInt),
		commitSubmissions: make(map[string]storage.CommitSubmission),
		pieces:            make(map[string]*memoryPiece),
		nextDataSet:       make(map[string]uint64),
		nextPiece:         1,
		terminated:        make(map[string]int64),
		epoch:             1000,
	}
}

// Probe returns stable relative upload durations without leaving the in-memory boundary.
func (m *MemoryFilecoin) Probe(ctx context.Context, serviceURL string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	for index, providerID := range m.providers {
		if serviceURL == fmt.Sprintf("https://provider-%s.system.invalid", providerID.String()) {
			return time.Duration(len(m.providers)-index) * time.Millisecond, nil
		}
	}
	return 0, fmt.Errorf("%w: unknown provider service URL", errInvalidFilecoinSequence)
}

func (m *MemoryFilecoin) PrepareUpload(ctx context.Context, _ uint64, targets []synapse.StorageTarget) (*sdkcosts.MultiContextCosts, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: upload has no contexts", errInvalidFilecoinSequence)
	}
	return &sdkcosts.MultiContextCosts{DepositNeeded: new(big.Int), Ready: true}, nil
}

func (m *MemoryFilecoin) SelectUploadTargets(ctx context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Copies <= 0 {
		return nil, fmt.Errorf("memory filecoin: positive copy count is required")
	}
	excluded := make(map[string]struct{}, len(opts.ExcludeProviderIDs))
	for _, providerID := range opts.ExcludeProviderIDs {
		excluded[providerID.String()] = struct{}{}
	}
	selected := make([]sdktypes.BigInt, 0, opts.Copies)
	for _, providerID := range m.providers {
		if _, skip := excluded[providerID.String()]; skip {
			continue
		}
		selected = append(selected, providerID.Copy())
		if len(selected) == opts.Copies {
			break
		}
	}
	if len(selected) != opts.Copies {
		return nil, fmt.Errorf("memory filecoin: requested %d copies, only %d providers available", opts.Copies, len(selected))
	}
	targets := make([]synapse.StorageTarget, 0, len(selected))
	for _, providerID := range selected {
		ref, err := m.FindMatchingDataSet(ctx, providerID, opts.DataSetMetadata, optionBool(opts.WithCDN))
		if err != nil {
			return nil, err
		}
		if ref != nil {
			targets = append(targets, m.newDataSetTarget(*ref, optionBool(opts.WithCDN)))
		} else {
			targets = append(targets, m.newProviderTarget(providerID, opts.DataSetMetadata, optionBool(opts.WithCDN)))
		}
	}
	return targets, nil
}

func (m *MemoryFilecoin) OpenProviderTarget(ctx context.Context, providerID sdktypes.BigInt, opts storage.NewProviderContextOptions) (synapse.ProviderTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.hasProvider(providerID) {
		return nil, fmt.Errorf("memory filecoin: unknown provider %s", providerID.String())
	}
	return m.newProviderTarget(providerID, opts.DataSetMetadata, optionBool(opts.WithCDN)), nil
}

func (m *MemoryFilecoin) OpenDataSetTarget(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	dataSet := m.dataSets[dataSetID.String()]
	m.mu.RUnlock()
	if dataSet == nil {
		return nil, fmt.Errorf("memory filecoin: unknown dataset %s", dataSetID.String())
	}
	if opts.ProviderID != nil && !dataSet.provider.Equal(*opts.ProviderID) {
		return nil, fmt.Errorf("memory filecoin: dataset %s belongs to provider %s, not %s", dataSetID.String(), dataSet.provider.String(), opts.ProviderID.String())
	}
	ref, err := storage.NewDataSetRef(dataSet.provider, dataSet.id, dataSet.clientID)
	if err != nil {
		return nil, err
	}
	withCDN := dataSet.withCDN
	if opts.WithCDN != nil {
		withCDN = *opts.WithCDN
	}
	return m.newDataSetTarget(ref, withCDN), nil
}

func (m *MemoryFilecoin) OpenCleanupContext(ctx context.Context, dataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.CleanupContext, error) {
	target, err := m.OpenDataSetTarget(ctx, dataSetID, opts)
	if err != nil {
		return nil, err
	}
	return target.(*memoryDataSetTarget), nil
}

func (m *MemoryFilecoin) Download(ctx context.Context, pieceCID cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	piece := m.pieces[pieceCID.String()]
	if piece == nil {
		m.mu.RUnlock()
		return nil, fmt.Errorf("memory filecoin: unknown CID %s", pieceCID)
	}
	if len(piece.committedDataSets) == 0 {
		m.mu.RUnlock()
		return nil, fmt.Errorf("%w: CID %s has not been committed", errInvalidFilecoinSequence, pieceCID)
	}
	content := bytes.Clone(piece.content)
	m.mu.RUnlock()
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (m *MemoryFilecoin) hasProvider(id sdktypes.BigInt) bool {
	return slices.ContainsFunc(m.providers, func(provider sdktypes.BigInt) bool { return provider.Equal(id) })
}

func (m *MemoryFilecoin) FindMatchingDataSet(
	ctx context.Context,
	providerID sdktypes.BigInt,
	metadata map[string]string,
	withCDN bool,
) (*storage.DataSetRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	wantedMetadata := canonicalMemoryDataSetMetadata(metadata, withCDN)
	var best *memoryDataSet
	for _, dataSet := range m.dataSets {
		if !dataSet.provider.Equal(providerID) || dataSet.withCDN != withCDN ||
			!maps.Equal(dataSet.metadata, wantedMetadata) {
			continue
		}
		if _, terminated := m.terminated[dataSet.id.String()]; terminated {
			continue
		}
		if best == nil ||
			(len(dataSet.pieces) > 0 && len(best.pieces) == 0) ||
			((len(dataSet.pieces) > 0) == (len(best.pieces) > 0) && dataSet.id.Cmp(best.id) < 0) {
			best = dataSet
		}
	}
	if best == nil {
		return nil, nil
	}
	ref, err := storage.NewDataSetRef(best.provider, best.id, best.clientID)
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

func cloneMemoryMetadata(metadata map[string]string) map[string]string {
	out := make(map[string]string, len(metadata))
	maps.Copy(out, metadata)
	return out
}

func canonicalMemoryDataSetMetadata(metadata map[string]string, withCDN bool) map[string]string {
	out := cloneMemoryMetadata(metadata)
	out["source"] = "synaps3"
	if withCDN {
		out["withCDN"] = ""
	} else {
		delete(out, "withCDN")
	}
	return out
}

func (m *MemoryFilecoin) newProviderTarget(providerID sdktypes.BigInt, metadata map[string]string, withCDN bool) *memoryProviderTarget {
	return &memoryProviderTarget{
		memoryTarget: memoryTarget{filecoin: m, provider: providerID.Copy(), withCDN: withCDN},
		metadata:     canonicalMemoryDataSetMetadata(metadata, withCDN),
	}
}

func (m *MemoryFilecoin) newDataSetTarget(ref storage.DataSetRef, withCDN bool) *memoryDataSetTarget {
	return &memoryDataSetTarget{
		memoryTarget: memoryTarget{filecoin: m, provider: ref.ProviderID(), withCDN: withCDN},
		ref:          ref,
	}
}

func optionBool(value *bool) bool { return value != nil && *value }

type memoryTarget struct {
	filecoin *MemoryFilecoin
	provider sdktypes.BigInt
	withCDN  bool
}

func (c *memoryTarget) ProviderID() sdktypes.BigInt { return c.provider.Copy() }

func (c *memoryTarget) GetProviderInfo() storage.Provider {
	return storage.Provider{ID: c.ProviderID(), ServiceURL: c.ServiceURL()}
}

func (c *memoryTarget) CDNEnabled() bool { return c.withCDN }

func (c *memoryTarget) ServiceURL() string {
	return fmt.Sprintf("https://provider-%s.system.invalid", c.provider.String())
}

func (c *memoryTarget) PieceURL(pieceCID cid.Cid) string {
	return c.ServiceURL() + "/piece/" + pieceCID.String()
}

type memoryProviderTarget struct {
	memoryTarget
	metadata map[string]string
}

func (c *memoryProviderTarget) DataSetRef() (storage.DataSetRef, bool) {
	return storage.DataSetRef{}, false
}

type memoryDataSetTarget struct {
	memoryTarget
	ref storage.DataSetRef
}

func (c *memoryDataSetTarget) DataSetRef() (storage.DataSetRef, bool) { return c.ref, true }

func (c *memoryDataSetTarget) DataSetID() *sdktypes.BigInt {
	id := c.ref.DataSetID()
	return &id
}

func (c *memoryProviderTarget) CreateDataSet(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m := c.filecoin
	m.mu.Lock()
	providerKey := c.provider.String()
	id, ok := m.pendingDataSets[providerKey]
	if !ok {
		m.nextDataSet[providerKey]++
		providerNumber, _ := c.provider.Uint64()
		id = sdktypes.NewBigInt(providerNumber*1000 + m.nextDataSet[providerKey])
		m.pendingDataSets[providerKey] = id.Copy()
	}
	clientIDValue, _ := id.Uint64()
	clientID := sdktypes.NewBigInt(clientIDValue + 500000)
	if opts != nil && opts.ClientDataSetID != nil {
		clientID = opts.ClientDataSetID.Copy()
	}
	txID := "create-" + id.String()
	m.submissions[txID] = id.Copy()
	dataSet := &memoryDataSet{
		id: id.Copy(), clientID: clientID.Copy(), provider: c.provider.Copy(),
		metadata: cloneMemoryMetadata(c.metadata), withCDN: c.withCDN,
		pieces: make(map[string]sdktypes.BigInt),
	}
	m.dataSets[id.String()] = dataSet
	delete(m.pendingDataSets, providerKey)
	m.mu.Unlock()

	submission := storage.CreateDataSetSubmission{
		ProviderID: c.provider.Copy(), TransactionID: txID,
		StatusURL: c.ServiceURL() + "/status/" + txID, ClientDataSetID: clientID.Copy(),
	}
	if opts != nil && opts.OnSubmitted != nil {
		opts.OnSubmitted(submission)
	}
	ref, err := storage.NewDataSetRef(c.provider, id, clientID)
	if err != nil {
		return nil, err
	}
	return &storage.CreateDataSetResult{TransactionID: txID, DataSet: ref}, nil
}

func (c *memoryProviderTarget) ContextIdentity() storage.ContextIdentity {
	return storage.ContextIdentity{
		Payer:        common.HexToAddress("0x00000000000000000000000000000000000000a1"),
		ChainID:      314159,
		RecordKeeper: common.HexToAddress("0x00000000000000000000000000000000000000b2"),
	}
}

func (c *memoryProviderTarget) FindDataSetByClientDataSetID(ctx context.Context, clientID sdktypes.BigInt) (storage.DataSetRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return storage.DataSetRef{}, false, err
	}
	c.filecoin.mu.RLock()
	defer c.filecoin.mu.RUnlock()
	for _, dataSet := range c.filecoin.dataSets {
		if dataSet.provider.Equal(c.provider) && dataSet.clientID.Equal(clientID) {
			ref, err := storage.NewDataSetRef(c.provider, dataSet.id, dataSet.clientID)
			return ref, err == nil, err
		}
	}
	return storage.DataSetRef{}, false, nil
}

func (c *memoryProviderTarget) WaitForDataSetCreated(ctx context.Context, statusURL string, clientDataSetID sdktypes.BigInt) (*storage.CreateDataSetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	txID, ok := strings.CutPrefix(statusURL, c.ServiceURL()+"/status/")
	if !ok || txID == "" {
		return nil, fmt.Errorf("%w: invalid dataset status URL %q", errInvalidFilecoinSequence, statusURL)
	}
	c.filecoin.mu.RLock()
	id, ok := c.filecoin.submissions[txID]
	dataSet := c.filecoin.dataSets[id.String()]
	c.filecoin.mu.RUnlock()
	if !ok || dataSet == nil || !dataSet.provider.Equal(c.provider) || !dataSet.clientID.Equal(clientDataSetID) {
		return nil, fmt.Errorf("%w: unknown dataset submission %q", errInvalidFilecoinSequence, txID)
	}
	ref, err := storage.NewDataSetRef(c.provider, id, dataSet.clientID)
	if err != nil {
		return nil, err
	}
	return &storage.CreateDataSetResult{TransactionID: txID, DataSet: ref}, nil
}

func (c *memoryDataSetTarget) Store(ctx context.Context, reader io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.DataSetID() == nil {
		return nil, fmt.Errorf("%w: store requires a dataset", errInvalidFilecoinSequence)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	pieceInfo, err := piece.CalculateFromBytes(content)
	if err != nil {
		return nil, fmt.Errorf("memory filecoin: calculating piece identity: %w", err)
	}
	pieceCID := pieceInfo.CIDv2
	if opts != nil && opts.PieceCID.Defined() && !opts.PieceCID.Equals(pieceCID) {
		return nil, fmt.Errorf("memory filecoin: supplied CID does not match content")
	}
	if opts != nil && opts.OnProgress != nil {
		opts.OnProgress(int64(len(content)))
	}
	c.filecoin.mu.Lock()
	piece := c.filecoin.pieces[pieceCID.String()]
	if piece == nil {
		piece = &memoryPiece{
			content:           bytes.Clone(content),
			storedProviders:   make(map[string]struct{}),
			committedDataSets: make(map[string]struct{}),
		}
		c.filecoin.pieces[pieceCID.String()] = piece
	} else if !bytes.Equal(piece.content, content) {
		c.filecoin.mu.Unlock()
		return nil, fmt.Errorf("memory filecoin: CID content mismatch")
	}
	piece.storedProviders[c.provider.String()] = struct{}{}
	c.filecoin.mu.Unlock()
	return &storage.StoreResult{PieceCID: pieceCID, Size: int64(len(content))}, nil
}

func (c *memoryDataSetTarget) PresignForCommit(ctx context.Context, pieces []storage.PieceInput) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.DataSetID() == nil || len(pieces) == 0 {
		return nil, fmt.Errorf("%w: presign requires a dataset and pieces", errInvalidFilecoinSequence)
	}
	c.filecoin.mu.RLock()
	for _, input := range pieces {
		piece := c.filecoin.pieces[input.PieceCID.String()]
		if piece == nil {
			c.filecoin.mu.RUnlock()
			return nil, fmt.Errorf("memory filecoin: unknown CID %s", input.PieceCID)
		}
	}
	c.filecoin.mu.RUnlock()
	return []byte("commit-" + c.provider.String()), nil
}

func (c *memoryDataSetTarget) Pull(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.DataSetID() == nil || len(request.Pieces) == 0 {
		return nil, fmt.Errorf("%w: pull requires a dataset and pieces", errInvalidFilecoinSequence)
	}
	results := make([]storage.PullPieceResult, 0, len(request.Pieces))
	for _, pieceCID := range request.Pieces {
		if request.From != nil && request.From(pieceCID) == "" {
			return nil, fmt.Errorf("memory filecoin: empty source URL for %s", pieceCID)
		}
		c.filecoin.mu.Lock()
		piece := c.filecoin.pieces[pieceCID.String()]
		if piece == nil || len(piece.storedProviders) == 0 {
			c.filecoin.mu.Unlock()
			return nil, fmt.Errorf("memory filecoin: unknown CID %s", pieceCID)
		}
		piece.storedProviders[c.provider.String()] = struct{}{}
		c.filecoin.mu.Unlock()
		if request.OnProgress != nil {
			request.OnProgress(pieceCID, storage.PullStatusComplete)
		}
		results = append(results, storage.PullPieceResult{PieceCID: pieceCID, Status: storage.PullStatusComplete})
	}
	return &storage.PullResult{Status: storage.PullStatusComplete, Pieces: results}, nil
}

func (c *memoryDataSetTarget) SubmitCommit(ctx context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dataSetID := c.DataSetID()
	if dataSetID == nil || len(request.Pieces) == 0 || len(request.ExtraData) == 0 {
		return nil, fmt.Errorf("%w: commit requires a dataset, pieces, and authorization", errInvalidFilecoinSequence)
	}
	c.filecoin.mu.Lock()
	dataSet := c.filecoin.dataSets[dataSetID.String()]
	if dataSet == nil || !dataSet.provider.Equal(c.provider) {
		c.filecoin.mu.Unlock()
		return nil, fmt.Errorf("%w: dataset %s is unavailable", errInvalidFilecoinSequence, dataSetID.String())
	}
	pieceIDs := make([]sdktypes.BigInt, 0, len(request.Pieces))
	for _, input := range request.Pieces {
		piece := c.filecoin.pieces[input.PieceCID.String()]
		if piece == nil {
			c.filecoin.mu.Unlock()
			return nil, fmt.Errorf("memory filecoin: unknown CID %s", input.PieceCID)
		}
		if _, ok := piece.storedProviders[c.provider.String()]; !ok {
			c.filecoin.mu.Unlock()
			return nil, fmt.Errorf("%w: provider %s has not stored %s", errInvalidFilecoinSequence, c.provider.String(), input.PieceCID)
		}
		pieceID, exists := dataSet.pieces[input.PieceCID.String()]
		if !exists {
			pieceID = sdktypes.NewBigInt(c.filecoin.nextPiece)
			c.filecoin.nextPiece++
			dataSet.pieces[input.PieceCID.String()] = pieceID.Copy()
		}
		pieceIDs = append(pieceIDs, pieceID.Copy())
		piece.committedDataSets[dataSetID.String()] = struct{}{}
	}
	txID := fmt.Sprintf("commit-%s-%s", dataSetID.String(), pieceIDs[0].String())
	c.filecoin.mu.Unlock()
	submission := storage.CommitSubmission{
		Kind: storage.CommitKindAddPieces, TransactionID: txID,
		StatusURL:  c.ServiceURL() + "/status/" + txID,
		ProviderID: c.provider.Copy(), DataSet: &c.ref,
		PieceCIDs: append([]cid.Cid(nil), pieceCIDs(request.Pieces)...),
	}
	c.filecoin.mu.Lock()
	c.filecoin.commitSubmissions[submission.StatusURL] = submission
	c.filecoin.mu.Unlock()
	if request.OnSubmitted != nil {
		request.OnSubmitted(submission)
	}
	return &submission, nil
}

func (c *memoryDataSetTarget) GetCommitStatus(ctx context.Context, statusURL string) (*storage.CommitStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.filecoin.mu.RLock()
	submission, found := c.filecoin.commitSubmissions[statusURL]
	c.filecoin.mu.RUnlock()
	dataSetID := c.DataSetID()
	if !found || dataSetID == nil || submission.Kind != storage.CommitKindAddPieces || submission.DataSet == nil ||
		!submission.ProviderID.Equal(c.provider) || !submission.DataSet.DataSetID().Equal(*dataSetID) || len(submission.PieceCIDs) == 0 {
		return nil, fmt.Errorf("%w: commit submission does not match target", errInvalidFilecoinSequence)
	}
	c.filecoin.mu.RLock()
	defer c.filecoin.mu.RUnlock()
	dataSet := c.filecoin.dataSets[dataSetID.String()]
	if dataSet == nil || !dataSet.provider.Equal(c.provider) {
		return nil, fmt.Errorf("%w: dataset %s is unavailable", errInvalidFilecoinSequence, dataSetID.String())
	}
	pieceIDs := make([]sdktypes.BigInt, 0, len(submission.PieceCIDs))
	for _, pieceCID := range submission.PieceCIDs {
		pieceID, ok := dataSet.pieces[pieceCID.String()]
		if !ok {
			return &storage.CommitStatus{
				Kind: storage.CommitKindAddPieces, State: storage.CommitStatePending,
				TransactionID: submission.TransactionID, DataSet: &c.ref,
			}, nil
		}
		pieceIDs = append(pieceIDs, pieceID.Copy())
	}
	expectedTxID := fmt.Sprintf("commit-%s-%s", dataSetID.String(), pieceIDs[0].String())
	if submission.TransactionID != expectedTxID {
		return nil, fmt.Errorf("%w: unknown commit transaction %q", errInvalidFilecoinSequence, submission.TransactionID)
	}
	return &storage.CommitStatus{
		Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed,
		TransactionID: submission.TransactionID, DataSet: &c.ref, PieceIDs: pieceIDs,
	}, nil
}

func pieceCIDs(pieces []storage.PieceInput) []cid.Cid {
	out := make([]cid.Cid, len(pieces))
	for i := range pieces {
		out[i] = pieces[i].PieceCID
	}
	return out
}

func (c *memoryDataSetTarget) DeletePieceByID(ctx context.Context, pieceID sdktypes.BigInt) (*sdktypes.WriteResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dataSetID := c.DataSetID()
	if dataSetID == nil {
		return nil, fmt.Errorf("%w: delete requires a dataset", errInvalidFilecoinSequence)
	}
	c.filecoin.mu.Lock()
	defer c.filecoin.mu.Unlock()
	dataSet := c.filecoin.dataSets[dataSetID.String()]
	if dataSet == nil {
		return nil, fmt.Errorf("memory filecoin: unknown dataset %s", dataSetID.String())
	}
	for pieceCID, storedID := range dataSet.pieces {
		if storedID.Equal(pieceID) {
			delete(dataSet.pieces, pieceCID)
			if piece := c.filecoin.pieces[pieceCID]; piece != nil {
				delete(piece.committedDataSets, dataSetID.String())
			}
			return &sdktypes.WriteResult{Hash: common.HexToHash(pieceID.String())}, nil
		}
	}
	return nil, fmt.Errorf("memory filecoin: unknown piece ID %s", pieceID.String())
}

func (c *memoryDataSetTarget) PieceStatus(ctx context.Context, pieceCID cid.Cid) (*storage.PieceStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dataSetID := c.DataSetID()
	if dataSetID == nil {
		return nil, fmt.Errorf("%w: status requires a dataset", errInvalidFilecoinSequence)
	}
	c.filecoin.mu.RLock()
	defer c.filecoin.mu.RUnlock()
	dataSet := c.filecoin.dataSets[dataSetID.String()]
	if dataSet == nil {
		return nil, fmt.Errorf("memory filecoin: unknown dataset %s", dataSetID.String())
	}
	pieceID, ok := dataSet.pieces[pieceCID.String()]
	if !ok {
		return nil, fmt.Errorf("memory filecoin: unknown CID %s", pieceCID)
	}
	return &storage.PieceStatus{Exists: true, PieceID: pieceID.Copy(), RetrievalURL: c.PieceURL(pieceCID)}, nil
}

func copyBigIntPtr(value sdktypes.BigInt) *sdktypes.BigInt {
	copy := value.Copy()
	return &copy
}

// GetWalletInfo returns a complete, funded wallet snapshot.
// ContextIdentity reports what this fake signs for, matching the identity its
// provider contexts carry.
func (m *MemoryFilecoin) ContextIdentity() storage.ContextIdentity {
	return storage.ContextIdentity{
		Payer:        common.HexToAddress("0x00000000000000000000000000000000000000a1"),
		ChainID:      314159,
		RecordKeeper: common.HexToAddress("0x00000000000000000000000000000000000000b2"),
	}
}

// VerifyServicePayer accepts every data set: the fake only holds data sets its
// own wallet created.
func (m *MemoryFilecoin) VerifyServicePayer(context.Context, sdktypes.BigInt) error {
	return nil
}

// TerminateService ends a data set's service. The fake reports an end of term
// the chain has already reached, because a system test exercises the operator
// flow rather than chain timing; use terminationDelay to make retirement wait.
func (m *MemoryFilecoin) TerminateService(_ context.Context, dataSetID sdktypes.BigInt) (*synapse.TerminationResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := dataSetID.String()
	endEpoch, ok := m.terminated[key]
	if !ok {
		endEpoch = m.epoch + m.terminationDelay
		m.terminated[key] = endEpoch
	}
	return &synapse.TerminationResult{TxHash: "0xterminate" + key, EndEpoch: endEpoch}, nil
}

// SetTerminationDelay controls how many epochs pass between termination and the
// end of term, so a test can decide whether retirement has to wait.
func (m *MemoryFilecoin) SetTerminationDelay(epochs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terminationDelay = epochs
}

func (m *MemoryFilecoin) CurrentEpoch(context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.epoch, nil
}

// AdvanceEpoch moves the observed chain head forward.
func (m *MemoryFilecoin) AdvanceEpoch(delta int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch += delta
}

// TerminationEpoch reports the recorded end of term, or false when the service
// was never terminated.
func (m *MemoryFilecoin) TerminationEpoch(dataSetID string) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	epoch, ok := m.terminated[dataSetID]
	return epoch, ok
}

func (m *MemoryFilecoin) GetWalletInfo(context.Context) (*synapse.WalletInfo, error) {
	nonce := uint64(1)
	return &synapse.WalletInfo{
		Address: "0x0000000000000000000000000000000000000001", Network: "calibration", ChainID: 314159,
		Nonce: &nonce, CurrentEpoch: big.NewInt(1), EpochDurationSeconds: 30,
		PaymentsAddress: "0x0000000000000000000000000000000000000002",
		USDFCAddress:    "0x0000000000000000000000000000000000000003",
		USDFCDecimals:   18, FILGasBalance: big.NewInt(1_000_000_000_000_000_000),
		USDFCWalletBalance: big.NewInt(1_000_000_000_000_000_000),
		PaymentAccount: &synapse.PaymentAccountInfo{
			Funds: big.NewInt(1_000_000_000_000_000_000), AvailableFunds: big.NewInt(1_000_000_000_000_000_000),
			LockupCurrent: new(big.Int), LockupRate: new(big.Int), NoActiveSpend: true,
		},
		Errors: map[string]string{},
	}, nil
}

func (m *MemoryFilecoin) FundUSDFC(context.Context, *big.Int) (string, error) {
	return "memory-fund", nil
}

func (m *MemoryFilecoin) WithdrawUSDFC(context.Context, *big.Int) (string, error) {
	return "memory-withdraw", nil
}

func (m *MemoryFilecoin) ApproveFWSS(context.Context) (string, error) {
	return "memory-approve", nil
}

func (m *MemoryFilecoin) TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error) {
	return nil, ethereum.NotFound
}

func (m *MemoryFilecoin) CheckRuntime(context.Context) synapse.ReadinessResult {
	return readyReadiness(synapse.ReadinessModeRuntime)
}

func (m *MemoryFilecoin) CheckDraft(context.Context, synapse.ReadinessConfig) synapse.ReadinessResult {
	return readyReadiness(synapse.ReadinessModeDraft)
}

func readyReadiness(mode synapse.ReadinessMode) synapse.ReadinessResult {
	return synapse.ReadinessResult{
		Status: synapse.ReadinessStatusReady, Mode: mode, CheckedAt: time.Now().UTC(),
		Checks: []synapse.ReadinessCheck{
			{ID: "sdk_client", Status: synapse.ReadinessStatusReady, Message: "Filecoin client is ready."},
			{ID: "storage_cost", Status: synapse.ReadinessStatusReady, Message: "Storage costs are available."},
			{ID: "payment_funding", Status: synapse.ReadinessStatusReady, Message: "Payment funding is sufficient."},
			{ID: "fwss_approval", Status: synapse.ReadinessStatusReady, Message: "FWSS approval is sufficient."},
		},
	}
}

// CheckProviders reports all three deterministic providers as available.
func (m *MemoryFilecoin) CheckProviders(ctx context.Context, checkedAt time.Time, _ []observability.LocalDataSet) ([]observability.ProviderState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	states := make([]observability.ProviderState, 0, len(m.providers))
	for _, providerID := range m.providers {
		active, hasPDP := true, true
		serviceURL, health := fmt.Sprintf("https://provider-%s.system.invalid", providerID.String()), "ok"
		states = append(states, observability.ProviderState{
			ProviderID: appTypes.OnChainIDFromSDK(providerID), Status: observability.StatusAvailable,
			ReasonCodes: []observability.ReasonCode{}, Active: &active, HasPDP: &hasPDP,
			ServiceURL: &serviceURL, HealthStatus: &health, LastCheckedAt: checkedAt, Evidence: map[string]any{},
		})
	}
	return states, nil
}

// CheckDataSets derives available observations from the runtime's local inventory.
func (m *MemoryFilecoin) CheckDataSets(ctx context.Context, checkedAt time.Time, local []observability.LocalDataSet) ([]observability.DataSetState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	states := make([]observability.DataSetState, 0, len(local))
	for _, dataSet := range local {
		if dataSet.DataSetID == nil {
			states = append(states, observability.DataSetState{
				LocalDataSetID: dataSet.ID, BucketID: dataSet.BucketID, BucketName: dataSet.BucketName,
				CopyIndex: dataSet.CopyIndex, ProviderID: dataSet.ProviderID, LocalStatus: dataSet.Status,
				Status: observability.StatusUnknown, ReasonCodes: []observability.ReasonCode{observability.ReasonChainDataSetMissing},
				LastCheckedAt: checkedAt, Evidence: map[string]any{},
			})
			continue
		}
		m.mu.RLock()
		chainDataSet := m.dataSets[dataSet.DataSetID.String()]
		consistent := chainDataSet != nil && chainDataSet.provider.Equal(dataSet.ProviderID.SDK())
		pieceCount := int64(0)
		if chainDataSet != nil {
			pieceCount = int64(len(chainDataSet.pieces))
		}
		m.mu.RUnlock()
		if !consistent {
			return nil, fmt.Errorf("memory filecoin: local dataset %d is inconsistent with chain state", dataSet.ID)
		}
		states = append(states, observability.DataSetState{
			LocalDataSetID: dataSet.ID, BucketID: dataSet.BucketID, BucketName: dataSet.BucketName,
			CopyIndex: dataSet.CopyIndex, ProviderID: dataSet.ProviderID, ChainDataSetID: dataSet.DataSetID,
			ClientDataSetID: dataSet.ClientDataSetID, LocalStatus: dataSet.Status, Status: observability.StatusAvailable,
			ReasonCodes: []observability.ReasonCode{}, ActivePieceCount: &pieceCount, LastCheckedAt: checkedAt,
			Evidence: map[string]any{},
		})
	}
	return states, nil
}

var (
	_ synapse.StorageClient        = (*MemoryFilecoin)(nil)
	_ synapse.WalletQuerier        = (*MemoryFilecoin)(nil)
	_ synapse.WalletOperator       = (*MemoryFilecoin)(nil)
	_ observability.RefreshChecker = (*MemoryFilecoin)(nil)
	_ synapse.ProviderTarget       = (*memoryProviderTarget)(nil)
	_ synapse.DataSetTarget        = (*memoryDataSetTarget)(nil)
	_ synapse.CleanupContext       = (*memoryDataSetTarget)(nil)
)
