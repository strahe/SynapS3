// Package systemtask owns inputs for recurring built-in tasks.
package systemtask

const CacheCapacityKey = "system:cache-capacity"

type Input struct{}

func ValidateInput(Input) error { return nil }
