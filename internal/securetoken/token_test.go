package securetoken

import (
	"encoding/base64"
	"strconv"
	"testing"
)

func TestURLReturnsRawURLTokenWithRequestedEntropy(t *testing.T) {
	for _, n := range []int{16, 20, 32} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			token, err := URL(n)
			if err != nil {
				t.Fatalf("URL(%d) error = %v, want nil", n, err)
			}
			if token == "" {
				t.Fatalf("URL(%d) token is empty", n)
			}
			decoded, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				t.Fatalf("URL(%d) token is not raw URL base64: %v", n, err)
			}
			if len(decoded) != n {
				t.Fatalf("URL(%d) decoded bytes = %d, want %d", n, len(decoded), n)
			}
		})
	}
}

func TestURLRejectsInvalidEntropy(t *testing.T) {
	for _, n := range []int{0, -1} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("URL(%d) panicked, want error: %v", n, r)
				}
			}()
			token, err := URL(n)
			if err == nil {
				t.Fatalf("URL(%d) error = nil, want error", n)
			}
			if token != "" {
				t.Fatalf("URL(%d) token = %q, want empty", n, token)
			}
		})
	}
}
