//go:build systemtest

package systemtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	appTypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

var errInvalidFilecoinSequence = errors.New("memory filecoin: invalid operation sequence")

type memoryDataSet struct {
	id       sdktypes.BigInt
	clientID sdktypes.BigInt
	provider sdktypes.BigInt
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

	providers       []sdktypes.BigInt
	dataSets        map[string]*memoryDataSet
	pendingDataSets map[string]sdktypes.BigInt
	submissions     map[string]sdktypes.BigInt
	pieces          map[string]*memoryPiece
	nextDataSet     map[string]uint64
	nextPiece       uint64
}

// NewMemoryFilecoin creates three deterministic active storage providers.
func NewMemoryFilecoin() *MemoryFilecoin {
	return &MemoryFilecoin{
		providers: []sdktypes.BigInt{
			sdktypes.NewBigInt(101),
			sdktypes.NewBigInt(102),
			sdktypes.NewBigInt(103),
		},
		dataSets:        make(map[string]*memoryDataSet),
		pendingDataSets: make(map[string]sdktypes.BigInt),
		submissions:     make(map[string]sdktypes.BigInt),
		pieces:          make(map[string]*memoryPiece),
		nextDataSet:     make(map[string]uint64),
		nextPiece:       1,
	}
}

func (m *MemoryFilecoin) PrepareUpload(ctx context.Context, _ uint64, contexts []synapse.UploadContext) (*storage.MultiContextCosts, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(contexts) == 0 {
		return nil, fmt.Errorf("%w: upload has no contexts", errInvalidFilecoinSequence)
	}
	return &storage.MultiContextCosts{DepositNeeded: new(big.Int), Ready: true}, nil
}

func (m *MemoryFilecoin) CreateContexts(ctx context.Context, opts *storage.CreateContextsOptions) ([]synapse.UploadContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts == nil || opts.Copies <= 0 {
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
	contexts := make([]synapse.UploadContext, 0, len(selected))
	for _, providerID := range selected {
		contexts = append(contexts, m.newContext(providerID, nil, optionBool(opts.WithCDN)))
	}
	return contexts, nil
}

func (m *MemoryFilecoin) CreateContext(ctx context.Context, opts *storage.CreateContextOptions) (synapse.UploadContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts == nil || (opts.ProviderID == nil && opts.DataSetID == nil) {
		return nil, errors.New("memory filecoin: provider or dataset ID is required")
	}
	var providerID sdktypes.BigInt
	if opts.DataSetID != nil {
		m.mu.RLock()
		dataSet := m.dataSets[opts.DataSetID.String()]
		m.mu.RUnlock()
		if dataSet == nil {
			return nil, fmt.Errorf("memory filecoin: unknown dataset %s", opts.DataSetID.String())
		}
		providerID = dataSet.provider.Copy()
		if opts.ProviderID != nil && !dataSet.provider.Equal(*opts.ProviderID) {
			return nil, fmt.Errorf("memory filecoin: dataset %s belongs to provider %s, not %s", opts.DataSetID.String(), dataSet.provider.String(), opts.ProviderID.String())
		}
	} else {
		providerID = opts.ProviderID.Copy()
	}
	if !m.hasProvider(providerID) {
		return nil, fmt.Errorf("memory filecoin: unknown provider %s", providerID.String())
	}
	return m.newContext(providerID, opts.DataSetID, optionBool(opts.WithCDN)), nil
}

func (m *MemoryFilecoin) CreateCleanupContext(ctx context.Context, opts *storage.CreateContextOptions) (synapse.CleanupContext, error) {
	uploadContext, err := m.CreateContext(ctx, opts)
	if err != nil {
		return nil, err
	}
	return uploadContext.(*memoryUploadContext), nil
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

func (m *MemoryFilecoin) newContext(providerID sdktypes.BigInt, dataSetID *sdktypes.BigInt, withCDN bool) *memoryUploadContext {
	var copiedDataSetID *sdktypes.BigInt
	if dataSetID != nil {
		id := dataSetID.Copy()
		copiedDataSetID = &id
	}
	return &memoryUploadContext{
		filecoin:  m,
		provider:  providerID.Copy(),
		dataSetID: copiedDataSetID,
		withCDN:   withCDN,
	}
}

func optionBool(value *bool) bool { return value != nil && *value }

type memoryUploadContext struct {
	filecoin *MemoryFilecoin
	provider sdktypes.BigInt
	withCDN  bool

	mu        sync.RWMutex
	dataSetID *sdktypes.BigInt
}

func (c *memoryUploadContext) ProviderID() sdktypes.BigInt { return c.provider.Copy() }

func (c *memoryUploadContext) DataSetID() *sdktypes.BigInt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dataSetID == nil {
		return nil
	}
	id := c.dataSetID.Copy()
	return &id
}

func (c *memoryUploadContext) GetProviderInfo() storage.Provider {
	return storage.Provider{ID: c.ProviderID(), ServiceURL: c.ServiceURL()}
}

func (c *memoryUploadContext) WithCDN() bool { return c.withCDN }

func (c *memoryUploadContext) ServiceURL() string {
	return fmt.Sprintf("https://provider-%s.system.invalid", c.provider.String())
}

func (c *memoryUploadContext) PieceURL(pieceCID cid.Cid) string {
	return c.ServiceURL() + "/piece/" + pieceCID.String()
}

func (c *memoryUploadContext) CreateDataSet(ctx context.Context, opts *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.DataSetID() != nil {
		return nil, fmt.Errorf("%w: dataset already exists", errInvalidFilecoinSequence)
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
	txID := "create-" + id.String()
	m.submissions[txID] = id.Copy()
	dataSet := &memoryDataSet{id: id.Copy(), clientID: clientID.Copy(), provider: c.provider.Copy(), pieces: make(map[string]sdktypes.BigInt)}
	m.dataSets[id.String()] = dataSet
	delete(m.pendingDataSets, providerKey)
	m.mu.Unlock()

	c.mu.Lock()
	c.dataSetID = copyBigIntPtr(id)
	c.mu.Unlock()
	submission := storage.CreateDataSetSubmission{
		TransactionID: txID, StatusURL: c.ServiceURL() + "/status/" + txID, ClientDataSetID: copyBigIntPtr(clientID),
	}
	if opts != nil && opts.OnSubmitted != nil {
		opts.OnSubmitted(submission)
	}
	return &storage.CreateDataSetResult{TransactionID: txID, DataSetID: id.Copy(), ClientDataSetID: clientID.Copy()}, nil
}

func (c *memoryUploadContext) WaitForDataSetCreated(ctx context.Context, submission storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.filecoin.mu.RLock()
	id, ok := c.filecoin.submissions[submission.TransactionID]
	dataSet := c.filecoin.dataSets[id.String()]
	c.filecoin.mu.RUnlock()
	if !ok || dataSet == nil || !dataSet.provider.Equal(c.provider) {
		return nil, fmt.Errorf("%w: unknown dataset submission %q", errInvalidFilecoinSequence, submission.TransactionID)
	}
	c.mu.Lock()
	c.dataSetID = copyBigIntPtr(id)
	c.mu.Unlock()
	return &storage.CreateDataSetResult{TransactionID: submission.TransactionID, DataSetID: id.Copy(), ClientDataSetID: dataSet.clientID.Copy()}, nil
}

func (c *memoryUploadContext) Store(ctx context.Context, reader io.Reader, opts *storage.StoreOptions) (*storage.StoreResult, error) {
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
	digest, err := multihash.Sum(content, multihash.SHA2_256, -1)
	if err != nil {
		return nil, fmt.Errorf("memory filecoin: hashing piece: %w", err)
	}
	pieceCID := cid.NewCidV1(cid.Raw, digest)
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

func (c *memoryUploadContext) PresignForCommit(ctx context.Context, pieces []storage.PieceInput) ([]byte, error) {
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

func (c *memoryUploadContext) Pull(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
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

func (c *memoryUploadContext) Commit(ctx context.Context, request storage.CommitRequest) (*storage.CommitResult, error) {
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
	if request.OnSubmitted != nil {
		request.OnSubmitted(txID)
	}
	return &storage.CommitResult{TransactionID: txID, DataSetID: dataSetID.Copy(), PieceIDs: pieceIDs}, nil
}

func (c *memoryUploadContext) DeletePieceByID(ctx context.Context, pieceID sdktypes.BigInt) (*sdktypes.WriteResult, error) {
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

func (c *memoryUploadContext) PieceStatus(ctx context.Context, pieceCID cid.Cid) (*storage.PieceStatus, error) {
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
	_ synapse.UploadContext        = (*memoryUploadContext)(nil)
	_ synapse.CleanupContext       = (*memoryUploadContext)(nil)
)
