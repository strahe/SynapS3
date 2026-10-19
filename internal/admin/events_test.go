package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdminEventsStopForInvalidatedSessions(t *testing.T) {
	for _, condition := range []string{"logout", "expired", "database-unavailable"} {
		t.Run(condition, func(t *testing.T) {
			srv := newTestAuthServer(t, "admin-password")
			srv.events = newAdminEventHub()
			var clock atomic.Int64
			clock.Store(time.Now().Unix())
			srv.auth.now = func() time.Time { return time.Unix(clock.Load(), 0) }
			token, claims, err := srv.auth.newSession(t.Context(), false)
			if err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /api/v1/events", srv.handleAPIEvents)
			server := httptest.NewServer(srv.withAdminAuth(mux))
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/api/v1/events", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: token})
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			reader := bufio.NewReader(response.Body)
			for range 2 {
				if _, err := reader.ReadString('\n'); err != nil {
					t.Fatal(err)
				}
			}
			switch condition {
			case "logout":
				if err := srv.auth.deleteSession(t.Context(), claims); err != nil {
					t.Fatal(err)
				}
			case "expired":
				clock.Store(claims.ExpiresAt)
			case "database-unavailable":
				srv.auth.sessions = unavailableSessionStore{Store: srv.auth.sessions}
			}
			srv.events.Publish("wallet_operation_updated", map[string]any{"id": 1})
			remaining, err := io.ReadAll(reader)
			if err != nil || len(remaining) != 0 {
				t.Fatalf("invalid session stream data=%q err=%v; want immediate EOF", remaining, err)
			}
		})
	}
}

func TestAdminEventsBasicAuthDoesNotRequireBrowserSession(t *testing.T) {
	srv := newTestAuthServer(t, "admin-password")
	srv.events = newAdminEventHub()
	srv.auth.sessions = unavailableSessionStore{Store: srv.auth.sessions}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/events", srv.handleAPIEvents)
	server := httptest.NewServer(srv.withAdminAuth(mux))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("admin", "admin-password")
	client := server.Client()
	client.Timeout = 3 * time.Second
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	for range 2 {
		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatal(err)
		}
	}
	srv.events.Publish("wallet_operation_updated", map[string]any{"id": 1})
	line, err := reader.ReadString('\n')
	if err != nil || line != "event: wallet_operation_updated\n" {
		t.Fatalf("Basic Auth event=%q err=%v", line, err)
	}
}

func TestAdminEventHubPublishesEnvelopeToSubscribers(t *testing.T) {
	hub := newAdminEventHub()
	sub, unsubscribe := hub.subscribe()
	defer unsubscribe()

	hub.publish("provider_identity_updated", map[string]any{
		"provider_id": "101",
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
		if payload.Seq != 1 || payload.Topic != "provider_identity_updated" || payload.ProviderID != "101" || payload.Identity != nil {
			t.Fatalf("payload = %#v, want provider ID only", payload)
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

	for range adminEventSubscriberBuffer {
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
	ts := httptest.NewServer(withSecurityHeaders(srv.withAdminAuth(mux)))
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
	if got := resp.Header.Get("cache-control"); got != "no-cache, no-store" {
		t.Fatalf("cache-control = %q, want no-cache, no-store", got)
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

	srv.publishProviderIdentity("101")
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
	if payload.Seq != 1 || payload.Topic != "provider_identity_updated" || payload.ProviderID != "101" || payload.Identity != nil {
		t.Fatalf("payload = %#v, want provider ID only", payload)
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
		"content_id":  int64(11),
		"version_id":  "01J000000000000000PROG01",
		"bucket_name": "photos",
		"object_key":  "image.jpg",
		"progress": map[string]any{
			"scope":          "ingress_store",
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
		ContentID int64  `json:"content_id"`
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
	if payload.Seq != 1 || payload.Topic != "upload_progress_updated" || payload.ContentID != 11 || payload.VersionID != "01J000000000000000PROG01" || payload.Progress.Percent == nil || *payload.Progress.Percent != 40 {
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
	return slices.Contains(lines, want)
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
