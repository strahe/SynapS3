package worker

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// knownSelector maps 4-byte function selectors to human-readable error names
// and a decoder that turns the remaining ABI-encoded data into a string.
var knownSelectors = map[string]struct {
	name   string
	decode func(data []byte) string
}{
	"42d750dc": {
		name: "InvalidSignature(address,address)",
		decode: func(data []byte) string {
			if len(data) < 64 {
				return ""
			}
			expected := formatAddress(data[:32])
			actual := formatAddress(data[32:64])
			return fmt.Sprintf("expected signer %s, recovered %s (possible cause: insufficient payment account balance)", expected, actual)
		},
	},
}

// revertReasonRe matches "revert reason=[0x<hex>]" inside error strings.
var revertReasonRe = regexp.MustCompile(`revert reason=\[0x([0-9a-fA-F]+)\]`)

// decodeRevertReason inspects errMsg for a revert reason hex pattern and, if a
// known selector is found, returns an enriched error message. If nothing can be
// decoded the original message is returned unchanged.
func decodeRevertReason(errMsg string) string {
	m := revertReasonRe.FindStringSubmatch(errMsg)
	if len(m) < 2 {
		return errMsg
	}

	raw, err := hex.DecodeString(m[1])
	if err != nil || len(raw) < 4 {
		return errMsg
	}

	selector := hex.EncodeToString(raw[:4])
	entry, ok := knownSelectors[selector]
	if !ok {
		return errMsg
	}

	detail := entry.decode(raw[4:])
	var decoded string
	if detail != "" {
		decoded = fmt.Sprintf("[decoded revert: %s — %s]", entry.name, detail)
	} else {
		decoded = fmt.Sprintf("[decoded revert: %s]", entry.name)
	}

	return errMsg + " " + decoded
}

// formatAddress extracts an Ethereum address from a 32-byte ABI-encoded word.
func formatAddress(word []byte) string {
	if len(word) < 32 {
		return "0x" + hex.EncodeToString(word)
	}
	// ABI address is right-aligned in 32 bytes (last 20 bytes)
	addr := word[12:32]
	return "0x" + strings.ToLower(hex.EncodeToString(addr))
}
