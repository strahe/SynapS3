package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/provider"
	"github.com/strahe/synaps3/internal/types"
)

func TestProviderOutputJSONEncodesIDAsString(t *testing.T) {
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

	if err := outputJSON([]provider.ProviderDetail{{ID: id, Name: "large-id-provider"}}); err != nil {
		t.Fatalf("outputJSON: %v", err)
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
	if !strings.Contains(out, `"id": "18446744073709551616"`) {
		t.Fatalf("provider JSON = %s, want id as decimal string", out)
	}
}
