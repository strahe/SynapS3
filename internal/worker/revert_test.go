package worker

import (
	"strings"
	"testing"
)

func TestDecodeRevertReason_InvalidSignature(t *testing.T) {
	// Real-world error string from task ID 11 (trimmed for readability).
	errMsg := `failed to ensure data set: failed to create data set: unexpected status 500: ` +
		`chain: message execution failed (exit=[33]), revert reason=[0x42d750dc` +
		`0000000000000000000000008c04a1ad757bc02069489dde155fd1379c58980e` +
		`000000000000000000000000ee4758fcd302f8eb61977d50d3891c6e66ca64d8]`

	got := decodeRevertReason(errMsg)

	if got == errMsg {
		t.Fatal("expected decoded message, got original unchanged")
	}

	// Should contain the decoded revert info.
	wantSubstrings := []string{
		"InvalidSignature(address,address)",
		"0x8c04a1ad757bc02069489dde155fd1379c58980e",
		"0xee4758fcd302f8eb61977d50d3891c6e66ca64d8",
		"insufficient payment account balance",
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(got, s) {
			t.Errorf("decoded message missing %q\ngot: %s", s, got)
		}
	}
}

func TestDecodeRevertReason_UnknownSelector(t *testing.T) {
	errMsg := `revert reason=[0xdeadbeef0000000000000000000000000000000000000000000000000000000000000001]`
	got := decodeRevertReason(errMsg)
	if got != errMsg {
		t.Errorf("expected original message for unknown selector, got: %s", got)
	}
}

func TestDecodeRevertReason_NoRevertPattern(t *testing.T) {
	errMsg := "connection refused"
	got := decodeRevertReason(errMsg)
	if got != errMsg {
		t.Errorf("expected original message when no revert pattern, got: %s", got)
	}
}

func TestDecodeRevertReason_SelectorOnly(t *testing.T) {
	// Selector matches but data too short for address decoding.
	errMsg := `revert reason=[0x42d750dc00]`
	got := decodeRevertReason(errMsg)

	if !strings.Contains(got, "InvalidSignature(address,address)") {
		t.Errorf("expected selector name in output, got: %s", got)
	}
}
