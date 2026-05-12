package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAdminURLResolution(t *testing.T) {
	t.Run("admin url flag wins and wildcard host is normalized", func(t *testing.T) {
		got, err := resolveAdminBaseURL(context.Background(), adminCommandOptions{
			AdminURL: "http://0.0.0.0:19090/",
			Timeout:  time.Second,
		})
		if err != nil {
			t.Fatalf("resolveAdminBaseURL: %v", err)
		}
		if got != "http://127.0.0.1:19090" {
			t.Fatalf("url = %q, want http://127.0.0.1:19090", got)
		}
	})

	t.Run("config admin addr is used without full validation", func(t *testing.T) {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		if err := os.WriteFile(cfgPath, []byte("[admin]\naddr = \":19091\"\n"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		got, err := resolveAdminBaseURL(context.Background(), adminCommandOptions{
			ConfigPath: cfgPath,
			ConfigSet:  true,
			Timeout:    time.Second,
		})
		if err != nil {
			t.Fatalf("resolveAdminBaseURL: %v", err)
		}
		if got != "http://127.0.0.1:19091" {
			t.Fatalf("url = %q, want http://127.0.0.1:19091", got)
		}
	})

	t.Run("scheme is optional", func(t *testing.T) {
		got, err := normalizeAdminBaseURL("127.0.0.1:19092")
		if err != nil {
			t.Fatalf("normalizeAdminBaseURL: %v", err)
		}
		if got != "http://127.0.0.1:19092" {
			t.Fatalf("url = %q, want http://127.0.0.1:19092", got)
		}
	})

	t.Run("ipv6 literal without port keeps brackets", func(t *testing.T) {
		got, err := normalizeAdminBaseURL("http://[::1]")
		if err != nil {
			t.Fatalf("normalizeAdminBaseURL: %v", err)
		}
		if got != "http://[::1]" {
			t.Fatalf("url = %q, want http://[::1]", got)
		}
	})
}

func TestAdminStatusHandlesReadySetupAndUnhealthy(t *testing.T) {
	t.Run("ready status aggregates runtime endpoints as json", func(t *testing.T) {
		var paths []string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			switch r.URL.Path {
			case "/healthz":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"status": "ok"})
			case "/api/v1/system/info":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"version": "test-version", "commit": "abc", "build_date": "today", "uptime_seconds": 12})
			case "/api/v1/workers":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"workers": map[string]bool{"upload": true}})
			case "/api/v1/cache/stats":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"used_bytes": 1, "max_bytes": 2})
			default:
				t.Fatalf("unexpected path %s", r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "--json", "status"})
		if err != nil {
			t.Fatalf("admin status: %v\n%s", err, out)
		}
		for _, want := range []string{"/healthz", "/api/v1/system/info", "/api/v1/workers", "/api/v1/cache/stats"} {
			if !containsString(paths, want) {
				t.Fatalf("paths = %#v, missing %s", paths, want)
			}
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(out), &body); err != nil {
			t.Fatalf("json output: %v\n%s", err, out)
		}
		if body["system"] == nil || body["workers"] == nil || body["cache"] == nil {
			t.Fatalf("status json missing runtime sections: %#v", body)
		}
	})

	t.Run("ready status text is readable for operators", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/healthz":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"status": "ok"})
			case "/api/v1/system/info":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
					"version":        "test-version",
					"commit":         "abc1234",
					"build_date":     "today",
					"uptime_seconds": 2086,
				})
			case "/api/v1/workers":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"workers": map[string]bool{"uploader": true, "evictor": false}})
			case "/api/v1/cache/stats":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"used_bytes": 3690803749, "max_bytes": 107374182400})
			default:
				t.Fatalf("unexpected path %s", r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "status"})
		if err != nil {
			t.Fatalf("admin status: %v\n%s", err, out)
		}
		for _, want := range []string{"SynapS3 Admin", "Status: ok", "Uptime: 34m46s", "Cache", "3.44 GiB", "100.00 GiB", "3.4%", "uploader", "healthy", "evictor", "unhealthy"} {
			if !strings.Contains(out, want) {
				t.Fatalf("status output missing %q:\n%s", want, out)
			}
		}
		for _, unwanted := range []string{"3690803749/107374182400 bytes", "\ttrue", "\tfalse"} {
			if strings.Contains(out, unwanted) {
				t.Fatalf("status output contains raw value %q:\n%s", unwanted, out)
			}
		}
	})

	t.Run("setup status avoids runtime endpoints", func(t *testing.T) {
		var paths []string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			switch r.URL.Path {
			case "/healthz":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"status": "setup"})
			case "/api/v1/settings":
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
					"mode":              "setup",
					"config_path":       "/tmp/config.toml",
					"writable":          true,
					"restart_required":  false,
					"validation_errors": []map[string]string{{"field": "filecoin.private_key", "message": "must be non-empty"}},
				})
			default:
				t.Fatalf("unexpected path %s", r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "status"})
		if err != nil {
			t.Fatalf("admin status setup: %v\n%s", err, out)
		}
		if got := strings.Join(paths, ","); got != "/healthz,/api/v1/settings" {
			t.Fatalf("paths = %s, want healthz and settings only", got)
		}
		if !strings.Contains(out, "setup") || !strings.Contains(out, "filecoin.private_key") {
			t.Fatalf("setup output missing expected content:\n%s", out)
		}
	})

	t.Run("unhealthy status returns an error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				t.Fatalf("unexpected path %s", r.URL.Path)
			}
			writeAdminTestJSON(t, w, http.StatusServiceUnavailable, map[string]any{
				"status": "unhealthy",
				"errors": []string{"db: unreachable"},
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "status"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if !strings.Contains(out, "db: unreachable") {
			t.Fatalf("unhealthy output missing error:\n%s", out)
		}
	})
}

func TestAdminS3UserCommands(t *testing.T) {
	t.Run("create admin requires yes before sending request", func(t *testing.T) {
		var called bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			t.Fatalf("request should not be sent")
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "create", "--role", "admin"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if called {
			t.Fatal("request was sent")
		}
	})

	t.Run("create sends write headers and prints secret", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/api/v1/s3-users" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			if got := r.Header.Get("X-SynapS3-Settings-Write"); got != "1" {
				t.Fatalf("write header = %q, want 1", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("Decode body: %v", err)
			}
			if body["role"] != "admin" {
				t.Fatalf("role = %q, want admin", body["role"])
			}
			writeAdminTestJSON(t, w, http.StatusCreated, map[string]string{
				"access_key": "ak",
				"secret_key": "sk",
				"role":       "admin",
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "create", "--role", "admin", "--yes"})
		if err != nil {
			t.Fatalf("admin s3-user create: %v\n%s", err, out)
		}
		if !strings.Contains(out, "ak") || !strings.Contains(out, "sk") {
			t.Fatalf("create output missing credentials:\n%s", out)
		}
		for _, want := range []string{"S3 User Credentials", "Access key: ak", "Secret key: sk", "Role: admin"} {
			if !strings.Contains(out, want) {
				t.Fatalf("create output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("update admin requires yes", func(t *testing.T) {
		var called bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "update", "ak", "--role", "admin"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if called {
			t.Fatal("request was sent")
		}
	})

	t.Run("delete requires yes", func(t *testing.T) {
		var called bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "delete", "ak"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if called {
			t.Fatal("request was sent")
		}
	})
}

func TestAdminSettingsSetValidationAndPayload(t *testing.T) {
	t.Run("summary output groups editable settings", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/settings" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "get"})
		if err != nil {
			t.Fatalf("settings get: %v\n%s", err, out)
		}
		for _, want := range []string{"Settings", "Mode: ready", "Restart required: no", "Server", "Filecoin", "Cache", "100.00 GiB", "Logging"} {
			if !strings.Contains(out, want) {
				t.Fatalf("settings summary missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("field getter supports every editable setting", func(t *testing.T) {
		var settings adminSettingsResponse
		data, err := json.Marshal(adminTestSettings("calibration", false))
		if err != nil {
			t.Fatalf("Marshal settings: %v", err)
		}
		if err := json.Unmarshal(data, &settings); err != nil {
			t.Fatalf("Unmarshal settings: %v", err)
		}

		for field := range adminEditableSettings {
			value, err := adminSettingsFieldValue(settings, field)
			if err != nil {
				t.Fatalf("adminSettingsFieldValue(%q): %v", field, err)
			}
			if value == nil {
				t.Fatalf("adminSettingsFieldValue(%q) returned nil", field)
			}
		}
	})

	t.Run("high risk changes require yes before put", func(t *testing.T) {
		var putCalled bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/settings":
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			case r.Method == http.MethodPut:
				putCalled = true
				t.Fatalf("PUT should not be sent")
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "set", "filecoin.network=mainnet"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if putCalled {
			t.Fatal("PUT was sent")
		}
	})

	t.Run("network casing does not require yes", func(t *testing.T) {
		var putCalled bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/settings":
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			case r.Method == http.MethodPut && r.URL.Path == "/api/v1/settings":
				putCalled = true
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("CALIBRATION", false))
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		if out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "set", "filecoin.network=CALIBRATION"}); err != nil {
			t.Fatalf("settings set failed: %v\n%s", err, out)
		}
		if !putCalled {
			t.Fatal("PUT was not sent")
		}
	})

	t.Run("increased server limits require yes before put", func(t *testing.T) {
		var putCalled bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/settings":
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			case r.Method == http.MethodPut:
				putCalled = true
				t.Fatalf("PUT should not be sent")
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "set", "server.max_connections=8192"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if putCalled {
			t.Fatal("PUT was sent")
		}
	})

	t.Run("lowered server limits do not require yes", func(t *testing.T) {
		var putCalled bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/settings":
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			case r.Method == http.MethodPut && r.URL.Path == "/api/v1/settings":
				putCalled = true
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		if out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "set", "server.max_requests=256"}); err != nil {
			t.Fatalf("settings set failed: %v\n%s", err, out)
		}
		if !putCalled {
			t.Fatal("PUT was not sent")
		}
	})

	t.Run("payload is nested and typed", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/settings":
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			case r.Method == http.MethodPut && r.URL.Path == "/api/v1/settings":
				if got := r.Header.Get("X-SynapS3-Settings-Write"); got != "1" {
					t.Fatalf("write header = %q, want 1", got)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatalf("Decode body: %v", err)
				}
				cache := body["cache"].(map[string]any)
				if cache["max_size_gb"] != float64(8) {
					t.Fatalf("cache.max_size_gb = %#v, want 8", cache["max_size_gb"])
				}
				filecoin := body["filecoin"].(map[string]any)
				if filecoin["with_cdn"] != true {
					t.Fatalf("filecoin.with_cdn = %#v, want true", filecoin["with_cdn"])
				}
				logging := body["logging"].(map[string]any)
				if logging["level"] != "debug" {
					t.Fatalf("logging.level = %#v, want debug", logging["level"])
				}
				s3Access := logging["s3_access"].(map[string]any)
				if s3Access["enabled"] != false {
					t.Fatalf("logging.s3_access.enabled = %#v, want false", s3Access["enabled"])
				}
				if s3Access["level"] != "debug" {
					t.Fatalf("logging.s3_access.level = %#v, want debug", s3Access["level"])
				}
				writeAdminTestJSON(t, w, http.StatusOK, adminTestSettings("calibration", false))
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{
			"synaps3", "admin", "--admin-url", ts.URL,
			"settings", "set", "cache.max_size_gb=8", "filecoin.with_cdn=true", "logging.level=debug",
			"logging.s3_access.enabled=false", "logging.s3_access.level=debug",
		})
		if err != nil {
			t.Fatalf("settings set: %v\n%s", err, out)
		}
	})

	t.Run("non editable fields are rejected before request", func(t *testing.T) {
		var called bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "set", "filecoin.private_key=secret"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if called {
			t.Fatal("request was sent")
		}
	})
}

func TestAdminTaskCommandsAndAPIErrorFields(t *testing.T) {
	t.Run("task list query and retry path", func(t *testing.T) {
		var sawList, sawRetry bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks":
				sawList = true
				if got := r.URL.Query().Get("status"); got != "exhausted" {
					t.Fatalf("status query = %q, want exhausted", got)
				}
				if got := r.URL.Query().Get("limit"); got != "50" {
					t.Fatalf("limit query = %q, want 50", got)
				}
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"tasks": []any{}, "total": 0, "limit": 50, "offset": 0})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks/42/retry":
				sawRetry = true
				if got := r.Header.Get("X-SynapS3-Settings-Write"); got != "" {
					t.Fatalf("task retry write header = %q, want empty", got)
				}
				writeAdminTestJSON(t, w, http.StatusOK, map[string]string{"status": "requeued"})
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		if out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "list", "--status", "exhausted", "--limit", "50"}); err != nil {
			t.Fatalf("task list: %v\n%s", err, out)
		}
		if out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "retry", "42"}); err != nil {
			t.Fatalf("task retry: %v\n%s", err, out)
		}
		if !sawList || !sawRetry {
			t.Fatalf("sawList=%v sawRetry=%v", sawList, sawRetry)
		}
	})

	t.Run("api errors include fields", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeAdminTestJSON(t, w, http.StatusBadRequest, map[string]any{
				"error":  "invalid settings",
				"fields": []map[string]string{{"field": "cache.max_size_gb", "message": "must be >= 1"}},
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "get"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if !strings.Contains(err.Error(), "invalid settings") || !strings.Contains(err.Error(), "cache.max_size_gb: must be >= 1") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("plain text api errors include fallback body", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("plain failure"))
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "get"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if !strings.Contains(err.Error(), "admin API error 418: plain failure") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("stage filter requires type before request", func(t *testing.T) {
		var called bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"tasks": []any{}, "total": 0, "limit": 20, "offset": 0})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "list", "--stage", "prepare_upload"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if called {
			t.Fatal("request was sent")
		}
	})

	t.Run("task list ref includes version id", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tasks" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
				"tasks": []map[string]any{{
					"id":             7,
					"type":           "upload",
					"stage":          "primary_commit",
					"ref_type":       "object",
					"ref_id":         11,
					"ref_version_id": "version-1",
					"status":         "exhausted",
					"retry_count":    5,
					"max_retries":    5,
					"scheduled_at":   "2026-05-05T10:00:00Z",
				}},
				"total":  1,
				"limit":  20,
				"offset": 0,
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "list"})
		if err != nil {
			t.Fatalf("task list: %v\n%s", err, out)
		}
		if !strings.Contains(out, "object:11:version-1") {
			t.Fatalf("task output missing version id:\n%s", out)
		}
	})

	t.Run("task list shows waiting status details", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tasks" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
				"tasks": []map[string]any{{
					"id":             8,
					"type":           "evict_cache",
					"ref_type":       "object",
					"ref_id":         12,
					"ref_version_id": "version-2",
					"status":         "waiting",
					"retry_count":    0,
					"max_retries":    5,
					"wait_reason":    "dependency",
					"status_message": "waiting for all copies to commit",
					"scheduled_at":   "2026-05-05T10:00:00Z",
				}},
				"total":  1,
				"limit":  20,
				"offset": 0,
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "list"})
		if err != nil {
			t.Fatalf("task list: %v\n%s", err, out)
		}
		if !strings.Contains(out, "DETAILS") || strings.Contains(out, "LAST_ERROR") {
			t.Fatalf("task output did not use details column:\n%s", out)
		}
		if !strings.Contains(out, "dependency: waiting for all copies to commit") {
			t.Fatalf("task output missing waiting details:\n%s", out)
		}
	})
}

func runAdminCommand(t *testing.T, args []string) (string, error) {
	t.Helper()
	cmd := newRootCommand()
	var out bytes.Buffer
	cmd.Writer = &out
	cmd.ErrWriter = &out
	err := cmd.Run(context.Background(), args)
	return out.String(), err
}

func writeAdminTestJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("Encode: %v", err)
	}
}

func adminTestSettings(network string, allowPrivate bool) map[string]any {
	return map[string]any{
		"mode":             "ready",
		"config_path":      "/tmp/config.toml",
		"writable":         true,
		"restart_required": false,
		"env_managed":      map[string]string{},
		"config": map[string]any{
			"server": map[string]any{
				"port":            ":8080",
				"max_connections": 4096,
				"max_requests":    512,
				"tls": map[string]any{
					"enabled":   false,
					"cert_file": "",
					"key_file":  "",
				},
			},
			"s3": map[string]any{"region": "us-east-1"},
			"filecoin": map[string]any{
				"network":                network,
				"rpc_url":                "https://rpc.example.test",
				"source":                 "synaps3",
				"with_cdn":               false,
				"allow_private_networks": allowPrivate,
				"default_copies":         2,
			},
			"cache": map[string]any{
				"dir":             "/tmp/cache",
				"max_size_gb":     100,
				"eviction_policy": "lru",
			},
			"worker": map[string]any{
				"upload":  map[string]any{"concurrency": 4, "poll_interval": "5s", "max_retries": 5},
				"evictor": map[string]any{"concurrency": 2, "poll_interval": "1m0s", "max_retries": 3},
			},
			"logging": map[string]any{
				"level":     "info",
				"format":    "text",
				"s3_access": map[string]any{"enabled": true, "level": "info"},
			},
		},
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
