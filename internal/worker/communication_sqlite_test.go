//go:build !postgres

package worker

import "testing"

func testPostgresMessageSettlement(*testing.T) {}
func testPostgresSchedulerWake(*testing.T)     {}
