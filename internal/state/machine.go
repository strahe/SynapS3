package state

import (
	"fmt"
	"sync"
)

// Transition defines a valid state change.
type Transition struct {
	From string
	To   string
}

// Machine is a generic, extensible finite state machine.
// States and transitions can be registered dynamically, supporting
// future additions without modifying existing logic.
type Machine struct {
	mu          sync.RWMutex
	transitions map[string]map[string]bool // from → set of valid to-states
}

// New creates a new empty state machine.
func New() *Machine {
	return &Machine{
		transitions: make(map[string]map[string]bool),
	}
}

// Register adds one or more valid transitions.
func (m *Machine) Register(transitions ...Transition) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range transitions {
		if m.transitions[t.From] == nil {
			m.transitions[t.From] = make(map[string]bool)
		}
		m.transitions[t.From][t.To] = true
	}
}

// CanTransition reports whether moving from → to is allowed.
func (m *Machine) CanTransition(from, to string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	targets, ok := m.transitions[from]
	if !ok {
		return false
	}
	return targets[to]
}

// Validate checks the transition and returns an error if it is not allowed.
func (m *Machine) Validate(from, to string) error {
	if !m.CanTransition(from, to) {
		return fmt.Errorf("invalid state transition: %s → %s", from, to)
	}
	return nil
}

// NextStates returns all states reachable from the given state.
func (m *Machine) NextStates(from string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	targets := m.transitions[from]
	out := make([]string, 0, len(targets))
	for t := range targets {
		out = append(out, t)
	}
	return out
}
