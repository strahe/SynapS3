//go:build dev

package ui

import "embed"

// DistFS returns an empty FS in dev mode.
// Use Vite dev server instead.
func DistFS() embed.FS {
	return embed.FS{}
}
