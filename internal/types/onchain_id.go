package types

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strconv"

	sdktypes "github.com/strahe/synapse-go/types"
)

// OnChainID is a uint256 chain identifier stored and encoded as decimal text.
type OnChainID struct {
	//nolint:unused // Prevents == comparisons and map keys for this value type.
	noCompare [0]func()
	value     sdktypes.BigInt
}

// NewOnChainID returns an OnChainID from a uint64 value.
func NewOnChainID(value uint64) OnChainID {
	return OnChainIDFromSDK(sdktypes.NewBigInt(value))
}

// OnChainIDFromSDK returns an OnChainID from a synapse-go BigInt.
func OnChainIDFromSDK(value sdktypes.BigInt) OnChainID {
	return OnChainID{value: value.Copy()}
}

// ParseOnChainID parses a decimal uint256 chain identifier.
func ParseOnChainID(name, value string) (OnChainID, error) {
	if value == "" {
		return OnChainID{}, fmt.Errorf("parsing %s: empty", name)
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return OnChainID{}, fmt.Errorf("parsing %s %q: invalid decimal", name, value)
		}
	}
	parsed, err := sdktypes.ParseBigInt(value)
	if err != nil {
		return OnChainID{}, fmt.Errorf("parsing %s %q: %w", name, value, err)
	}
	return OnChainIDFromSDK(parsed), nil
}

// SDK returns a defensive copy of id as a synapse-go BigInt.
func (id OnChainID) SDK() sdktypes.BigInt {
	return id.value.Copy()
}

func (id OnChainID) String() string {
	return id.value.String()
}

func (id OnChainID) IsZero() bool {
	return id.value.IsZero()
}

func (id OnChainID) Equal(other OnChainID) bool {
	return id.value.Equal(other.value)
}

func (id OnChainID) Value() (driver.Value, error) {
	return id.String(), nil
}

func (id *OnChainID) Scan(src any) error {
	if id == nil {
		return fmt.Errorf("scan OnChainID: nil receiver")
	}
	switch value := src.(type) {
	case nil:
		*id = OnChainID{}
		return nil
	case string:
		return id.scanString(value)
	case []byte:
		return id.scanString(string(value))
	default:
		return fmt.Errorf("scan OnChainID: unsupported %T", src)
	}
}

func (id *OnChainID) scanString(value string) error {
	parsed, err := ParseOnChainID("onChainID", value)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

func (id OnChainID) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(id.String())), nil
}

func (id *OnChainID) UnmarshalJSON(data []byte) error {
	if id == nil {
		return fmt.Errorf("unmarshal OnChainID: nil receiver")
	}
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return fmt.Errorf("unmarshal OnChainID: null")
	}

	var raw string
	if len(data) > 0 && data[0] == '"' {
		var err error
		raw, err = strconv.Unquote(string(data))
		if err != nil {
			return err
		}
	} else {
		raw = string(data)
	}

	parsed, err := ParseOnChainID("onChainID", raw)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}
