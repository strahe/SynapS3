//go:build systemtest && !postgres

package system_test

import (
	"testing"

	"github.com/strahe/synaps3/internal/config"
)

// systemDatabase keeps the harness's private SQLite database.
func systemDatabase(*testing.T) *config.DatabaseConfig { return nil }
