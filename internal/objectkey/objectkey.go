package objectkey

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const maxUTF8Bytes = 1024

var (
	errInvalidUTF8 = errors.New("object key must be valid UTF-8")
	errContainsNUL = errors.New("object key must not contain NUL")
	errTooLong     = errors.New("object key exceeds 1024 UTF-8 bytes")
)

func Validate(key string) error {
	if !utf8.ValidString(key) {
		return errInvalidUTF8
	}
	if strings.ContainsRune(key, '\x00') {
		return errContainsNUL
	}
	if len(key) > maxUTF8Bytes {
		return errTooLong
	}
	return nil
}
