package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/provider"
	"github.com/strahe/synaps3/internal/types"
)

func TestProviderOutputJSONPreservesPartialResultsAndStringIDs(t *testing.T) {
	id, err := types.ParseOnChainID("provider id", "18446744073709551616")
	if err != nil {
		t.Fatalf("ParseOnChainID: %v", err)
	}

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	originalStdout := os.Stdout
	os.Stdout = writeEnd
	t.Cleanup(func() { os.Stdout = originalStdout })

	cause := errors.New("provider 2 lookup failed: RPC overloaded")
	partial := &provider.PartialListError{Failures: []error{cause}}
	if err := outputProviderList([]provider.ProviderDetail{{ID: id, Name: "large-id-provider"}}, partial, true); !errors.Is(err, cause) {
		t.Fatalf("partial result did not return its error: %v", err)
	}
	if err := writeEnd.Close(); err != nil {
		t.Fatalf("close stdout pipe: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, readEnd); err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	if err := readEnd.Close(); err != nil {
		t.Fatalf("close read pipe: %v", err)
	}

	out := buf.String()
	var providers []provider.ProviderDetail
	if err := json.Unmarshal(buf.Bytes(), &providers); err != nil || len(providers) != 1 {
		t.Fatalf("partial result JSON = %q, error %v", out, err)
	}
	if !strings.Contains(out, `"id": "18446744073709551616"`) {
		t.Fatalf("provider JSON = %s, want id as decimal string", out)
	}
}
