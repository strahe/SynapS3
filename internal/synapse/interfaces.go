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
