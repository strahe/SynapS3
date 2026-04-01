//go:build !dev

package ui

import "embed"

//go:embed dist/*
var distFS embed.FS

// DistFS returns the embedded frontend assets.
func DistFS() embed.FS {
	return distFS
}
