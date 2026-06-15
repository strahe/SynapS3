package objectkey

import (
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{name: "ascii", key: "logs/2026/01.txt"},
		{name: "unicode under byte limit", key: strings.Repeat("你", 341)},
		{name: "exact byte limit", key: strings.Repeat("a", maxUTF8Bytes)},
		{name: "too long by bytes", key: strings.Repeat("你", 342), wantErr: errTooLong},
		{name: "invalid utf8", key: string([]byte{0xff}), wantErr: errInvalidUTF8},
		{name: "nul", key: "folder/\x00/file.txt", wantErr: errContainsNUL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.key)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
