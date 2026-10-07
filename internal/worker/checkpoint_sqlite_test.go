//go:build !postgres

package worker

import "testing"

func testPostgresCheckpointedEffect(*testing.T) {}
