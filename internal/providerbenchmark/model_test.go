package providerbenchmark

import (
	"testing"
	"time"
)

func TestMeasurementPriority(t *testing.T) {
	for _, tc := range []struct {
		endorsed, approved, bound bool
		want                      int
	}{
		{endorsed: true, approved: true, bound: true, want: 0},
		{approved: true, bound: true, want: 1},
		{bound: true, want: 2},
		{want: 3},
	} {
		if got := Priority(tc.endorsed, tc.approved, tc.bound); got != tc.want {
			t.Fatalf("priority = %d, want %d", got, tc.want)
		}
	}
}

func TestAutomaticMeasurementDue(t *testing.T) {
	now := time.Now().UTC()
	previous := now.Add(-time.Hour)
	for _, tc := range []struct {
		name     string
		row      *Result
		interval time.Duration
		want     bool
	}{
		{name: "first measurement", want: true},
		{name: "changed URL", row: &Result{State: StateFailed, ServiceURLHash: "old", TestedAt: &previous, FailureCode: new("upload_failed")}, want: true},
		{name: "active changed URL", row: &Result{State: StateTesting, ServiceURLHash: "old"}},
		{name: "retained success active", row: &Result{State: StateTesting, ServiceURLHash: "current", TestedAt: &previous}, interval: time.Minute},
		{name: "success without periodic retest", row: &Result{State: StateSucceeded, ServiceURLHash: "current", TestedAt: &previous}},
		{name: "measured failure without periodic retest", row: &Result{State: StateFailed, ServiceURLHash: "current", TestedAt: &previous, FailureCode: new("upload_failed")}},
		{name: "unknown failure", row: &Result{State: StateFailed, ServiceURLHash: "current", TestedAt: &previous, FailureCode: new("future_failure")}},
		{name: "periodic boundary", row: &Result{State: StateSucceeded, ServiceURLHash: "current", TestedAt: &previous}, interval: time.Hour, want: true},
		{name: "periodic not due", row: &Result{State: StateFailed, ServiceURLHash: "current", TestedAt: &previous, FailureCode: new("timeout")}, interval: 2 * time.Hour},
		{name: "no settled result", row: &Result{State: StateFailed, ServiceURLHash: "current"}, want: true},
		{name: "unknown state", row: &Result{State: "future", ServiceURLHash: "old"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Due(tc.row, "current", tc.interval, now); got != tc.want {
				t.Fatalf("due = %v, want %v", got, tc.want)
			}
		})
	}
	for _, code := range NonMeasuredFailureCodes() {
		t.Run(code, func(t *testing.T) {
			row := &Result{State: StateFailed, ServiceURLHash: "current", TestedAt: &previous, FailureCode: &code}
			if MeasuredFailure(code) || !Due(row, "current", 0, now) {
				t.Fatalf("nonmeasurement failure %q did not allow a new measurement", code)
			}
		})
	}
}
