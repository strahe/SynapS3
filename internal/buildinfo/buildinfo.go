package buildinfo

import "fmt"

// Populated at build time via ldflags.
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

func String() string {
	return fmt.Sprintf("synaps3 %s (commit %s, built %s)", Version, Commit, Date)
}
