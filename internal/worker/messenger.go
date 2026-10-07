package worker

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/strahe/synaps3/internal/db/repository"
)

type Message interface {
	MessageType() string
}

type MessageHandler func(context.Context, *repository.Repositories, Message) error

type messageMode uint8

const (
	messageHandover messageMode = iota + 1
	messageNotice
)

type messageReceiver struct {
	id      string
	handler MessageHandler
}

type messageRoute struct {
	mode      messageMode
	receivers []messageReceiver
}

// Messenger dispatches declared messages synchronously in the source transaction.
type Messenger struct {
	registry  *Registry
	producer  string
	handovers map[string]struct{}
	notices   map[string]struct{}
}

func (r *Registry) OwnHandover(id, kind string, handler MessageHandler) error {
	return r.registerMessage(id, kind, messageHandover, handler)
}

func (r *Registry) Subscribe(id, kind string, handler MessageHandler) error {
	return r.registerMessage(id, kind, messageNotice, handler)
}

func (r *Registry) registerMessage(id, kind string, mode messageMode, handler MessageHandler) error {
	if r == nil || strings.TrimSpace(id) == "" || strings.TrimSpace(kind) == "" || handler == nil {
		return fmt.Errorf("message registration requires id, kind, and handler: %w", repository.ErrInvalidInput)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRegistryFrozen
	}
	if _, duplicate := r.messageIDs[id]; duplicate {
		return fmt.Errorf("message handler %q is already registered", id)
	}
	route, exists := r.messages[kind]
	if exists && route.mode != mode {
		return fmt.Errorf("message %q cannot be both handover and notice", kind)
	}
	if exists && mode == messageHandover && len(route.receivers) != 0 {
		return fmt.Errorf("handover %q already has an owner", kind)
	}
	if !exists {
		route = &messageRoute{mode: mode}
		r.messages[kind] = route
	}
	route.receivers = append(route.receivers, messageReceiver{id: id, handler: handler})
	r.messageIDs[id] = struct{}{}
	return nil
}

func (r *Registry) Messenger(producer string, handovers, notices []string) (*Messenger, error) {
	if r == nil || strings.TrimSpace(producer) == "" {
		return nil, fmt.Errorf("message producer is required: %w", repository.ErrInvalidInput)
	}
	messenger := &Messenger{
		registry: r, producer: producer,
		handovers: make(map[string]struct{}, len(handovers)),
		notices:   make(map[string]struct{}, len(notices)),
	}
	for _, declaration := range []struct {
		kinds []string
		set   map[string]struct{}
	}{{handovers, messenger.handovers}, {notices, messenger.notices}} {
		for _, kind := range declaration.kinds {
			if strings.TrimSpace(kind) == "" {
				return nil, fmt.Errorf("producer %q has an empty message kind: %w", producer, repository.ErrInvalidInput)
			}
			declaration.set[kind] = struct{}{}
		}
	}
	for kind := range messenger.handovers {
		if _, duplicate := messenger.notices[kind]; duplicate {
			return nil, fmt.Errorf("producer %q declares %q as both handover and notice", producer, kind)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return nil, ErrRegistryFrozen
	}
	if _, duplicate := r.producers[producer]; duplicate {
		return nil, fmt.Errorf("message producer %q is already declared", producer)
	}
	r.producers[producer] = messenger
	return messenger, nil
}

// validateMessages runs while holding the registry's write lock.
func (r *Registry) validateMessages() error {
	declared := make(map[string]messageMode)
	for _, producer := range r.producers {
		for _, declaration := range []struct {
			kinds map[string]struct{}
			mode  messageMode
		}{{producer.handovers, messageHandover}, {producer.notices, messageNotice}} {
			for kind := range declaration.kinds {
				if previous, exists := declared[kind]; exists && previous != declaration.mode {
					return fmt.Errorf("message %q is declared as both handover and notice", kind)
				}
				declared[kind] = declaration.mode
				route, exists := r.messages[kind]
				if !exists || route.mode != declaration.mode || len(route.receivers) == 0 {
					return fmt.Errorf("producer %q has no matching receiver for %q", producer.producer, kind)
				}
			}
		}
	}
	for kind, route := range r.messages {
		if mode, exists := declared[kind]; !exists || mode != route.mode {
			return fmt.Errorf("message %q has no matching producer", kind)
		}
		slices.SortFunc(route.receivers, func(a, b messageReceiver) int { return strings.Compare(a.id, b.id) })
	}
	return nil
}

func (m *Messenger) Handover(ctx context.Context, tx *repository.Repositories, message Message) error {
	return m.dispatch(ctx, tx, message, messageHandover)
}

func (m *Messenger) Notify(ctx context.Context, tx *repository.Repositories, message Message) error {
	return m.dispatch(ctx, tx, message, messageNotice)
}

type messageDepthKey struct{}

func (m *Messenger) dispatch(ctx context.Context, tx *repository.Repositories, message Message, mode messageMode) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("message dispatch panicked: %v", recovered)
		}
	}()
	if m == nil || m.registry == nil || ctx == nil || nilMessage(message) {
		return fmt.Errorf("messenger, context, and message are required: %w", repository.ErrInvalidInput)
	}
	if err := requireTransaction(tx); err != nil {
		return err
	}
	kind := message.MessageType()
	permissions := m.handovers
	if mode == messageNotice {
		permissions = m.notices
	}
	if _, allowed := permissions[kind]; !allowed {
		return fmt.Errorf("producer %q cannot dispatch %q: %w", m.producer, kind, repository.ErrInvalidInput)
	}
	m.registry.mu.RLock()
	if !m.registry.frozen {
		m.registry.mu.RUnlock()
		return fmt.Errorf("message registry must be frozen before dispatch")
	}
	route := m.registry.messages[kind]
	if route == nil || route.mode != mode {
		m.registry.mu.RUnlock()
		return fmt.Errorf("message %q has no matching route", kind)
	}
	receivers := route.receivers
	m.registry.mu.RUnlock()
	depth, _ := ctx.Value(messageDepthKey{}).(int)
	if depth >= 8 {
		return fmt.Errorf("message %q exceeds maximum dispatch depth 8", kind)
	}
	ctx = context.WithValue(ctx, messageDepthKey{}, depth+1)
	for _, receiver := range receivers {
		if err := invokeMessageHandler(ctx, tx, message, receiver.handler); err != nil {
			return fmt.Errorf("dispatching %q to %q: %w", kind, receiver.id, err)
		}
	}
	return nil
}

func nilMessage(message Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func invokeMessageHandler(ctx context.Context, tx *repository.Repositories, message Message, handler MessageHandler) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("message handler panicked: %v", recovered)
		}
	}()
	return handler(ctx, tx, message)
}
