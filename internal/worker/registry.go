package worker

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
	schedulers  []*Scheduler
	periodic    []ScheduleDefinition
	producers   map[string]*Messenger
	messages    map[string]*messageRoute
	messageIDs  map[string]struct{}
	frozen      bool
}

func NewRegistry() *Registry {
	return &Registry{
		handlers:    make(map[model.TaskType]Handler),
		definitions: make(map[model.TaskType]Definition),
		producers:   make(map[string]*Messenger),
		messages:    make(map[string]*messageRoute),
		messageIDs:  make(map[string]struct{}),
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
	r.handlers[definition.Type] = handler
	r.definitions[definition.Type] = definition
	return nil
}

func (r *Registry) freeze() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return nil
	}
	for _, scheduler := range r.schedulers {
		for taskType := range scheduler.types {
			if _, exists := r.definitions[taskType]; !exists {
				return fmt.Errorf("scheduler %q authorizes unregistered type %q: %w", scheduler.owner, taskType, ErrUnknownType)
			}
		}
	}
	if err := r.validateMessages(); err != nil {
		return err
	}
	r.frozen = true
	return nil
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
