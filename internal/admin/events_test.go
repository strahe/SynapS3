package admin

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAdminEventHubPublishesEnvelopeToSubscribers(t *testing.T) {
	hub := newAdminEventHub()
	sub, unsubscribe := hub.subscribe()
	defer unsubscribe()

	hub.publish("provider_identity_updated", map[string]any{
		"provider_id": "101",
		"identity":    &providerIdentityResponse{RegistryProviderID: "101", Name: "alpha-pdp"},
	})

	select {
	case event := <-sub:
		if event.seq != 1 || event.topic != "provider_identity_updated" {
			t.Fatalf("event metadata = %#v, want seq/topic", event)
		}
		var payload struct {
			Seq        uint64                    `json:"seq"`
			Topic      string                    `json:"topic"`
			ProviderID string                    `json:"provider_id"`
			Identity   *providerIdentityResponse `json:"identity"`
		}
		if err := json.Unmarshal(event.data, &payload); err != nil {
			t.Fatalf("Unmarshal event: %v", err)
		}
		if payload.Seq != 1 || payload.Topic != "provider_identity_updated" || payload.ProviderID != "101" || payload.Identity.Name != "alpha-pdp" {
			t.Fatalf("payload = %#v, want provider identity envelope", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("event was not delivered")
	}
}

func TestAdminEventHubDoesNotBlockOnSlowSubscribers(t *testing.T) {
	hub := newAdminEventHub()
	slow, unsubscribeSlow := hub.subscribe()
	defer unsubscribeSlow()
	fast, unsubscribeFast := hub.subscribe()
	defer unsubscribeFast()

	for i := 0; i < adminEventSubscriberBuffer; i++ {
		hub.publish("provider_identity_updated", map[string]any{"provider_id": "101"})
		select {
		case <-fast:
		case <-time.After(time.Second):
			t.Fatal("fast subscriber did not receive event while slow subscriber filled")
		}
	}
	if len(slow) != adminEventSubscriberBuffer {
		t.Fatalf("slow subscriber queue len = %d, want full buffer", len(slow))
	}

	hub.publish("provider_identity_updated", map[string]any{"provider_id": "202"})
	select {
	case event := <-fast:
		if event.seq != adminEventSubscriberBuffer+1 {
			t.Fatalf("fast event seq = %d, want latest event after slow subscriber filled", event.seq)
		}
	case <-time.After(time.Second):
		t.Fatal("fast subscriber was blocked by slow subscriber")
	}
}

func TestAdminEventHubPublishesConcurrentEventsInSequence(t *testing.T) {
	hub := newAdminEventHub()
	sub, unsubscribe := hub.subscribe()
	defer unsubscribe()
	blocking := &blockingJSON{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	firstDone := make(chan struct{})
	go func() {
		hub.publish("provider_identity_updated", map[string]any{"blocked": blocking})
		close(firstDone)
	}()
	<-blocking.started

	secondDone := make(chan struct{})
	go func() {
		hub.publish("provider_identity_updated", map[string]any{"provider_id": "202"})
		close(secondDone)
	}()

	select {
	case event := <-sub:
		t.Fatalf("event delivered before earlier publish completed: %#v", event)
	case <-time.After(20 * time.Millisecond):
	}

	close(blocking.release)
	waitForClosed(t, firstDone, "first publish")
	waitForClosed(t, secondDone, "second publish")
	first := receiveAdminEvent(t, sub)
	second := receiveAdminEvent(t, sub)
	if first.seq != 1 || second.seq != 2 {
		t.Fatalf("event seqs = %d, %d, want in publish order", first.seq, second.seq)
	}
}

func TestAdminEventsHandlerStreamsProviderIdentityEvent(t *testing.T) {
	srv := &Server{events: newAdminEventHub(), logger: testLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events", srv.handleAPIEvents)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/events")
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("content-type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read connected comment: %v", err)
	}
	if line != ": connected\n" {
		t.Fatalf("first SSE line = %q, want connected comment", line)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read connected separator: %v", err)
	}

	srv.publishProviderIdentity(&providerIdentityResponse{RegistryProviderID: "101", Name: "alpha-pdp"})
	lines := readSSEEventLines(t, reader)
	if !containsLine(lines, "event: provider_identity_updated\n") {
		t.Fatalf("SSE lines = %#v, want provider_identity_updated event", lines)
	}
	if !containsLine(lines, "id: 1\n") {
		t.Fatalf("SSE lines = %#v, want event id", lines)
	}
	dataLine := findDataLine(lines)
	if dataLine == "" {
		t.Fatalf("SSE lines = %#v, want data line", lines)
	}
	var payload struct {
		Seq        uint64                    `json:"seq"`
		Topic      string                    `json:"topic"`
		ProviderID string                    `json:"provider_id"`
		Identity   *providerIdentityResponse `json:"identity"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(dataLine), "data: ")), &payload); err != nil {
		t.Fatalf("Unmarshal SSE data: %v", err)
	}
	if payload.Seq != 1 || payload.Topic != "provider_identity_updated" || payload.ProviderID != "101" || payload.Identity.Name != "alpha-pdp" {
		t.Fatalf("payload = %#v, want provider identity event payload", payload)
	}
}

func TestAdminEventsHandlerStreamsUploadProgressEvent(t *testing.T) {
	events := NewEventHub()
	srv := &Server{events: events, logger: testLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events", srv.handleAPIEvents)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/events")
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	reader := bufio.NewReader(resp.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read connected comment: %v", err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read connected separator: %v", err)
	}

	events.Publish("upload_progress_updated", map[string]any{
		"upload_id":   int64(11),
		"version_id":  "01J000000000000000PROG01",
		"bucket_name": "photos",
		"object_key":  "image.jpg",
		"progress": map[string]any{
			"scope":          "primary_store",
			"attempt":        1,
			"uploaded_bytes": int64(4),
			"total_bytes":    int64(10),
			"percent":        40,
			"done":           false,
			"updated_at":     "2026-05-06T00:00:00Z",
		},
	})
	lines := readSSEEventLines(t, reader)
	if !containsLine(lines, "event: upload_progress_updated\n") {
		t.Fatalf("SSE lines = %#v, want upload_progress_updated event", lines)
	}
	dataLine := findDataLine(lines)
	if dataLine == "" {
		t.Fatalf("SSE lines = %#v, want data line", lines)
	}
	var payload struct {
		Seq       uint64 `json:"seq"`
		Topic     string `json:"topic"`
		UploadID  int64  `json:"upload_id"`
		VersionID string `json:"version_id"`
		Progress  struct {
			Scope         string `json:"scope"`
			Attempt       int    `json:"attempt"`
			UploadedBytes int64  `json:"uploaded_bytes"`
			TotalBytes    int64  `json:"total_bytes"`
			Percent       *int   `json:"percent"`
			Done          bool   `json:"done"`
		} `json:"progress"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(dataLine), "data: ")), &payload); err != nil {
		t.Fatalf("Unmarshal SSE data: %v", err)
	}
	if payload.Seq != 1 || payload.Topic != "upload_progress_updated" || payload.UploadID != 11 || payload.VersionID != "01J000000000000000PROG01" || payload.Progress.Percent == nil || *payload.Progress.Percent != 40 {
		t.Fatalf("payload = %#v, want upload progress event payload", payload)
	}
}

func TestAdminEventsHandlerReturnsUnavailableWithoutHub(t *testing.T) {
	srv := &Server{logger: testLogger()}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	rec := httptest.NewRecorder()

	srv.handleAPIEvents(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func readSSEEventLines(t *testing.T, reader *bufio.Reader) []string {
	t.Helper()
	lines := make([]string, 0, 4)
	deadline := time.After(time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out reading SSE event lines: %#v", lines)
		default:
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE event: %v", err)
		}
		if line == "\n" {
			return lines
		}
		lines = append(lines, line)
	}
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

func findDataLine(lines []string) string {
	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			return line
		}
	}
	return ""
}

type blockingJSON struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (b *blockingJSON) MarshalJSON() ([]byte, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return []byte(`"blocked"`), nil
}

func receiveAdminEvent(t *testing.T, events <-chan adminEvent) adminEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("event was not delivered")
		return adminEvent{}
	}
}

func waitForClosed(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s did not finish", name)
	}
}
