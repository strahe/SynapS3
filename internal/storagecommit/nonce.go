package storagecommit

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	sdktypes "github.com/strahe/synapse-go/types"
)

var addPiecesExtraData = func() abi.Arguments {
	arguments := make(abi.Arguments, 0, 4)
	for _, typeName := range []string{"uint256", "string[][]", "string[][]", "bytes"} {
		argumentType, err := abi.NewType(typeName, "", nil)
		if err != nil {
			panic(err)
		}
		arguments = append(arguments, abi.Argument{Type: argumentType})
	}
	return arguments
}()

// ExtraDataNonce returns the nonce an add-pieces authorization was signed
// with. FWSS accepts each nonce once per payer, so it names every submission
// of the same signed request on chain.
func ExtraDataNonce(extraData []byte) (sdktypes.BigInt, error) {
	values, err := addPiecesExtraData.Unpack(extraData)
	if err != nil {
		return sdktypes.BigInt{}, fmt.Errorf("decoding add-pieces extra data: %w", err)
	}
	if len(values) != len(addPiecesExtraData) {
		return sdktypes.BigInt{}, errors.New("decoding add-pieces extra data: unexpected fields")
	}
	nonce, ok := values[0].(*big.Int)
	if !ok {
		return sdktypes.BigInt{}, errors.New("decoding add-pieces extra data: invalid nonce")
	}
	return sdktypes.BigIntFromBig(nonce)
}
