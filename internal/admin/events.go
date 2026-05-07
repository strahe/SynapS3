package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	adminEventSubscriberBuffer = 32
	adminEventHeartbeat        = 15 * time.Second
)

type adminEvent struct {
	seq   uint64
	topic string
	data  []byte
}

type adminEventListener func(topic string)

type EventPublisher interface {
	Publish(topic string, payload map[string]any)
}

type EventHub struct {
	publishMu   sync.Mutex
	mu          sync.Mutex
	nextSeq     uint64
	subscribers map[chan adminEvent]struct{}
	listeners   []adminEventListener
}

func NewEventHub() *EventHub {
	return newAdminEventHub()
}

func newAdminEventHub() *EventHub {
	return &EventHub{
		subscribers: make(map[chan adminEvent]struct{}),
	}
}

func (h *EventHub) subscribe() (<-chan adminEvent, func()) {
	ch := make(chan adminEvent, adminEventSubscriberBuffer)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()

	unsubscribe := func() {
		h.mu.Lock()
		delete(h.subscribers, ch)
		h.mu.Unlock()
	}
	return ch, unsubscribe
}

func (h *EventHub) onPublish(listener adminEventListener) {
	if h == nil || listener == nil {
		return
	}
	h.mu.Lock()
	h.listeners = append(h.listeners, listener)
	h.mu.Unlock()
}

func (h *EventHub) Publish(topic string, payload map[string]any) {
	if h == nil {
		return
	}

	h.publishMu.Lock()
	defer h.publishMu.Unlock()

	h.mu.Lock()
	h.nextSeq++
	seq := h.nextSeq
	h.mu.Unlock()

	envelope := make(map[string]any, len(payload)+2)
	envelope["seq"] = seq
	envelope["topic"] = topic
	for key, value := range payload {
		envelope[key] = value
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	event := adminEvent{seq: seq, topic: topic, data: data}

	h.mu.Lock()
	listeners := append([]adminEventListener(nil), h.listeners...)
	subscribers := make([]chan adminEvent, 0, len(h.subscribers))
	for ch := range h.subscribers {
		subscribers = append(subscribers, ch)
	}
	h.mu.Unlock()

	for _, listener := range listeners {
		listener(topic)
	}
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *EventHub) publish(topic string, payload map[string]any) {
	h.Publish(topic, payload)
}

func (s *Server) publishProviderIdentity(identity *providerIdentityResponse) {
	if s == nil || s.events == nil || identity == nil || identity.RegistryProviderID == "" {
		return
	}
	s.events.publish("provider_identity_updated", map[string]any{
		"provider_id": identity.RegistryProviderID,
		"identity":    identity,
	})
}

func (s *Server) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	if s.events == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "events unavailable"})
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil && s.logger != nil {
		s.logger.Warn("api: failed to clear events write deadline", "error", err)
	}

	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}

	events, unsubscribe := s.events.subscribe()
	defer unsubscribe()

	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(adminEventHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case event, ok := <-events:
			if !ok {
				return
			}
			if _, err := fmt.Fprintf(w, "event: %s\nid: %d\ndata: %s\n\n", event.topic, event.seq, event.data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
