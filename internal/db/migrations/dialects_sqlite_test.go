//go:build !postgres

package migrations

import (
	"testing"

	"github.com/uptrace/bun"
)

func testPostgresMigrationDialect(*testing.T, func(*testing.T, *bun.DB)) {}
