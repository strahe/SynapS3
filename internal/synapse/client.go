package synapse

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	gosynapse "github.com/data-preservation-programs/go-synapse"
	"github.com/data-preservation-programs/go-synapse/constants"
	"github.com/data-preservation-programs/go-synapse/pdp"
	"github.com/data-preservation-programs/go-synapse/signer"
)

// ClientBundle holds the SDK clients needed by backend and workers.
type ClientBundle struct {
	Storage  StorageClient
	ProofSet ProofSetClient
	closer   func()
}

// Close releases all SDK resources.
func (b *ClientBundle) Close() {
	if b.closer != nil {
		b.closer()
	}
}

// NewClientBundle initializes the go-synapse SDK and returns narrow-interface wrappers.
func NewClientBundle(ctx context.Context, privateKey *ecdsa.PrivateKey, rpcURL, providerURL, network string) (*ClientBundle, error) {
	client, err := gosynapse.New(ctx, gosynapse.Options{
		PrivateKey:  privateKey,
		RPCURL:      rpcURL,
		ProviderURL: providerURL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating synapse client: %w", err)
	}

	storageMgr, err := client.Storage()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("creating storage manager: %w", err)
	}

	evmSigner, err := signer.NewSecp256k1SignerFromECDSA(privateKey)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("creating EVM signer: %w", err)
	}

	pdpMgr, err := pdp.NewManagerWithContext(ctx, client.EthClient(), evmSigner, constants.Network(network))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("creating PDP manager: %w", err)
	}

	return &ClientBundle{
		Storage:  storageMgr,
		ProofSet: pdpMgr,
		closer:   client.Close,
	}, nil
}
