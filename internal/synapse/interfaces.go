package synapse

import (
	"context"
	"io"
	"math/big"

	"github.com/data-preservation-programs/go-synapse/pdp"
	"github.com/data-preservation-programs/go-synapse/storage"
	"github.com/ipfs/go-cid"
)

// StorageClient abstracts the go-synapse storage.Manager for upload/download.
// *storage.Manager satisfies this interface directly.
type StorageClient interface {
	Upload(ctx context.Context, data io.Reader, opts *storage.UploadOptions) (*storage.UploadResult, error)
	Download(ctx context.Context, pieceCID cid.Cid, opts *storage.DownloadOptions) ([]byte, error)
}

// ProofSetClient abstracts the go-synapse pdp.ProofSetManager for on-chain operations.
// *pdp.Manager satisfies this interface directly.
type ProofSetClient interface {
	CreateProofSet(ctx context.Context, opts pdp.CreateProofSetOptions) (*pdp.ProofSetResult, error)
	AddRoots(ctx context.Context, proofSetID *big.Int, roots []pdp.Root) (*pdp.AddRootsResult, error)
	DeleteProofSet(ctx context.Context, proofSetID *big.Int, extraData []byte) error
}

// WalletQuerier provides on-chain wallet state for the admin dashboard.
type WalletQuerier interface {
	GetWalletInfo(ctx context.Context) (*WalletInfo, error)
}

// WalletInfo holds a snapshot of the wallet's on-chain state.
// Fields are nil when the corresponding RPC call failed; see Errors for details.
type WalletInfo struct {
	Address         string
	Network         string
	ChainID         int64
	Nonce           *uint64
	PaymentsAddress string
	USDFCAddress    string
	USDFCDecimals   uint8
	FILBalance      *big.Int
	USDFCBalance    *big.Int
	FILAccount      *TokenAccountInfo
	USDFCAccount    *TokenAccountInfo
	Errors          map[string]string
}

// TokenAccountInfo holds the PDP payments contract account state for a single token.
type TokenAccountInfo struct {
	Funds             *big.Int
	AvailableFunds    *big.Int
	LockupCurrent     *big.Int
	LockupRate        *big.Int
	LockupLastSettled *big.Int
	FundedUntilEpoch  *big.Int
	CurrentLockupRate *big.Int
}
