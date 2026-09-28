//go:build !postgres

package task

import "testing"

func testPostgresCheckpointedEffect(*testing.T) {}
