package securetoken

import (
	"crypto/rand"
	"encoding/base64"
)

// URL generates a URL-safe random token from n bytes of entropy.
func URL(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
