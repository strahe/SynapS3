package model

import "github.com/oklog/ulid/v2"

// NewVersionID returns an S3-compatible ULID version identifier.
func NewVersionID() string {
	return ulid.Make().String()
}
