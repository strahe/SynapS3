package types

import (
	"database/sql/driver"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	gotypes "go/types"
	"testing"

	sdktypes "github.com/strahe/synapse-go/types"
)

const overUint64Decimal = "18446744073709551616"

func TestOnChainIDParsesAndCopiesSDKBigInt(t *testing.T) {
	id, err := ParseOnChainID("providerID", overUint64Decimal)
	if err != nil {
		t.Fatalf("ParseOnChainID: %v", err)
	}
	if got := id.String(); got != overUint64Decimal {
		t.Fatalf("String() = %q, want %q", got, overUint64Decimal)
	}

	sdkID := id.SDK()
	if got := sdkID.String(); got != overUint64Decimal {
		t.Fatalf("SDK() = %q, want %q", got, overUint64Decimal)
	}

	roundTrip := OnChainIDFromSDK(sdkID)
	if !roundTrip.Equal(id) {
		t.Fatalf("OnChainIDFromSDK(SDK()) = %s, want %s", roundTrip.String(), id.String())
	}

	sdkCopy := id.SDK()
	if _, ok := sdkCopy.Uint64(); ok {
		t.Fatal("SDK copy unexpectedly fits uint64")
	}
}

func TestOnChainIDRejectsInvalidDecimals(t *testing.T) {
	for _, raw := range []string{"", "-1", "1.2", "1e2", "0x10", " 1", "1 ", "+1"} {
		if _, err := ParseOnChainID("providerID", raw); err == nil {
			t.Fatalf("ParseOnChainID(%q) succeeded, want error", raw)
		}
	}
}

func TestOnChainIDJSONUsesDecimalStrings(t *testing.T) {
	id, err := ParseOnChainID("dataSetID", overUint64Decimal)
	if err != nil {
		t.Fatalf("ParseOnChainID: %v", err)
	}

	raw, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if got := string(raw); got != `"`+overUint64Decimal+`"` {
		t.Fatalf("MarshalJSON = %s, want quoted decimal", got)
	}

	var fromString OnChainID
	if err := json.Unmarshal([]byte(`"`+overUint64Decimal+`"`), &fromString); err != nil {
		t.Fatalf("UnmarshalJSON string: %v", err)
	}
	if !fromString.Equal(id) {
		t.Fatalf("UnmarshalJSON string = %s, want %s", fromString.String(), id.String())
	}

	var fromNumber OnChainID
	if err := json.Unmarshal([]byte(overUint64Decimal), &fromNumber); err != nil {
		t.Fatalf("UnmarshalJSON number: %v", err)
	}
	if !fromNumber.Equal(id) {
		t.Fatalf("UnmarshalJSON number = %s, want %s", fromNumber.String(), id.String())
	}

	for _, raw := range []string{`null`, `""`, `"-1"`, `"1.2"`, `"1e2"`, `"0x10"`} {
		var invalid OnChainID
		if err := json.Unmarshal([]byte(raw), &invalid); err == nil {
			t.Fatalf("UnmarshalJSON(%s) succeeded, want error", raw)
		}
	}
}

func TestOnChainIDSQLRoundTrip(t *testing.T) {
	id := NewOnChainID(0)
	value, err := id.Value()
	if err != nil {
		t.Fatalf("Value zero: %v", err)
	}
	if value != driver.Value("0") {
		t.Fatalf("Value zero = %#v, want %q", value, "0")
	}

	id, err = ParseOnChainID("pieceID", overUint64Decimal)
	if err != nil {
		t.Fatalf("ParseOnChainID: %v", err)
	}
	value, err = id.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if value != driver.Value(overUint64Decimal) {
		t.Fatalf("Value = %#v, want %q", value, overUint64Decimal)
	}

	var scanned OnChainID
	if err := scanned.Scan([]byte(overUint64Decimal)); err != nil {
		t.Fatalf("Scan []byte: %v", err)
	}
	if !scanned.Equal(id) {
		t.Fatalf("Scan []byte = %s, want %s", scanned.String(), id.String())
	}
	if err := scanned.Scan(nil); err != nil {
		t.Fatalf("Scan nil: %v", err)
	}
	if !scanned.IsZero() {
		t.Fatalf("Scan nil = %s, want zero", scanned.String())
	}
}

func TestOnChainIDFromSDKRejectsNegative(t *testing.T) {
	var id sdktypes.BigInt
	if err := id.UnmarshalJSON([]byte(`"-1"`)); err == nil {
		t.Fatal("sdktypes.BigInt accepted negative JSON; test assumption invalid")
	}
}

func TestOnChainIDIsNotComparable(t *testing.T) {
	fset := token.NewFileSet()
	valueFile, err := parser.ParseFile(fset, "onchain_id.go", nil, 0)
	if err != nil {
		t.Fatalf("parse value source: %v", err)
	}
	usageFile, err := parser.ParseFile(fset, "usage.go", `
package types

func compare(a, b OnChainID) {
	_ = a == b
	_ = map[OnChainID]int{}
}
`, 0)
	if err != nil {
		t.Fatalf("parse usage source: %v", err)
	}

	var errors []error
	conf := gotypes.Config{
		Error: func(err error) {
			errors = append(errors, err)
		},
	}
	_, _ = conf.Check("github.com/strahe/synaps3/internal/types", fset, []*ast.File{valueFile, usageFile}, nil)
	if len(errors) == 0 {
		t.Fatal("OnChainID comparison and map key type-checked, want errors")
	}
}
