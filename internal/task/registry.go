package task

import (
	"fmt"
	"slices"
	"sync"

	"github.com/strahe/synaps3/internal/model"
)

// Registry is immutable after Engine starts.
type Registry struct {
	mu          sync.RWMutex
	handlers    map[model.TaskType]Handler
	definitions map[model.TaskType]Definition
	frozen      bool
}

func NewRegistry() *Registry {
	return &Registry{
		handlers:    make(map[model.TaskType]Handler),
		definitions: make(map[model.TaskType]Definition),
	}
}

func (r *Registry) Register(handler Handler) error {
	if r == nil || handler == nil {
		return fmt.Errorf("registering nil handler: %w", ErrUnknownType)
	}
	definition := handler.Definition()
	if err := definition.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRegistryFrozen
	}
	if _, exists := r.handlers[definition.Type]; exists {
		return fmt.Errorf("handler %q already registered", definition.Type)
	}
	definition.RetryLimit = cloneInt(definition.RetryLimit)
	r.handlers[definition.Type] = handler
	r.definitions[definition.Type] = definition
	return nil
}

func (r *Registry) freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

func (r *Registry) Handler(taskType model.TaskType) (Handler, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.handlers[taskType]
	return handler, ok
}

func (r *Registry) Definition(taskType model.TaskType) (Definition, bool) {
	if r == nil {
		return Definition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, ok := r.definitions[taskType]
	if !ok {
		return Definition{}, false
	}
	definition.RetryLimit = cloneInt(definition.RetryLimit)
	return definition, true
}

func (r *Registry) Types() []model.TaskType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]model.TaskType, 0, len(r.handlers))
	for taskType := range r.handlers {
		types = append(types, taskType)
	}
	slices.Sort(types)
	return types
}
