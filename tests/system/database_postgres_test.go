//go:build systemtest && postgres

package system_test

import (
	"testing"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/testpg"
)

// systemDatabase gives each scenario a private PostgreSQL schema and a pool
// with several connections, as production uses.
func systemDatabase(t *testing.T) *config.DatabaseConfig {
	return &config.DatabaseConfig{Driver: "postgres", DSN: testpg.SchemaDSN(t), MaxOpenConns: 8, MaxIdleConns: 2}
}
