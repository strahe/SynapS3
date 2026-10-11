package testutil

import "time"

// Business-flow tests leave enough lease time for transactional settlement under load.
const (
	TaskPollInterval  = 25 * time.Millisecond
	TaskLeaseDuration = 5 * time.Second
)
