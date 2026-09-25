package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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
	t.Setenv(configEnvVar, "")

	t.Run("sends basic auth when admin password env is set", func(t *testing.T) {
		t.Setenv("SYNAPS3_ADMIN_PASSWORD", "admin-password")
		var gotUser, gotPassword string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				gotUser, gotPassword, _ = r.BasicAuth()
			}
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

		if _, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "--json", "status"}); err != nil {
			t.Fatalf("admin status: %v", err)
		}
		if gotUser != "admin" || gotPassword != "admin-password" {
			t.Fatalf("basic auth = %q/%q, want admin/admin-password", gotUser, gotPassword)
		}
	})

	t.Run("uses configured admin username for basic auth", func(t *testing.T) {
		t.Setenv("SYNAPS3_ADMIN_PASSWORD", "admin-password")
		var gotUser, gotPassword string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				gotUser, gotPassword, _ = r.BasicAuth()
			}
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

		cfgPath := filepath.Join(t.TempDir(), "config.toml")
		cfg := fmt.Sprintf("[admin]\naddr = %q\n\n[admin.auth]\nusername = \"root\"\n", ts.URL)
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		if _, err := runAdminCommand(t, []string{"synaps3", "--config", cfgPath, "admin", "--json", "status"}); err != nil {
			t.Fatalf("admin status: %v", err)
		}
		if gotUser != "root" || gotPassword != "admin-password" {
			t.Fatalf("basic auth = %q/%q, want root/admin-password", gotUser, gotPassword)
		}
	})

	t.Run("reads admin password from initial password file when env is unset", func(t *testing.T) {
		var gotUser, gotPassword string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				gotUser, gotPassword, _ = r.BasicAuth()
			}
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

		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		cfg := fmt.Sprintf("[admin]\naddr = %q\n\n[admin.auth]\nusername = \"root\"\n", ts.URL)
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("WriteFile config: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "admin-initial-password"), []byte("file-password\n"), 0o600); err != nil {
			t.Fatalf("WriteFile password: %v", err)
		}

		if _, err := runAdminCommand(t, []string{"synaps3", "--config", cfgPath, "admin", "--json", "status"}); err != nil {
			t.Fatalf("admin status: %v", err)
		}
		if gotUser != "root" || gotPassword != "file-password" {
			t.Fatalf("basic auth = %q/%q, want root/file-password", gotUser, gotPassword)
		}
	})

	t.Run("reads config and initial password from SYNAPS3_CONFIG", func(t *testing.T) {
		var gotUser, gotPassword string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				gotUser, gotPassword, _ = r.BasicAuth()
			}
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

		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		cfg := fmt.Sprintf("[admin]\naddr = %q\n\n[admin.auth]\nusername = \"root\"\n", ts.URL)
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("WriteFile config: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "admin-initial-password"), []byte("file-password\n"), 0o600); err != nil {
			t.Fatalf("WriteFile password: %v", err)
		}
		t.Setenv(configEnvVar, cfgPath)

		if _, err := runAdminCommand(t, []string{"synaps3", "admin", "--json", "status"}); err != nil {
			t.Fatalf("admin status: %v", err)
		}
		if gotUser != "root" || gotPassword != "file-password" {
			t.Fatalf("basic auth = %q/%q, want root/file-password", gotUser, gotPassword)
		}
	})

	t.Run("env admin password overrides initial password file", func(t *testing.T) {
		t.Setenv("SYNAPS3_ADMIN_PASSWORD", "env-password")
		var gotPassword string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				_, gotPassword, _ = r.BasicAuth()
			}
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

		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		cfg := fmt.Sprintf("[admin]\naddr = %q\n", ts.URL)
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("WriteFile config: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "admin-initial-password"), []byte("file-password\n"), 0o600); err != nil {
			t.Fatalf("WriteFile password: %v", err)
		}

		if _, err := runAdminCommand(t, []string{"synaps3", "--config", cfgPath, "admin", "--json", "status"}); err != nil {
			t.Fatalf("admin status: %v", err)
		}
		if gotPassword != "env-password" {
			t.Fatalf("basic auth password = %q, want env-password", gotPassword)
		}
	})

	t.Run("empty initial password file returns an error", func(t *testing.T) {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		if err := os.WriteFile(cfgPath, []byte("[admin]\naddr = \"127.0.0.1:1\"\n"), 0o600); err != nil {
			t.Fatalf("WriteFile config: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "admin-initial-password"), []byte("\n"), 0o600); err != nil {
			t.Fatalf("WriteFile password: %v", err)
		}

		out, err := runAdminCommand(t, []string{"synaps3", "--config", cfgPath, "admin", "status"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if !strings.Contains(err.Error(), "admin initial password file") || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("error = %v, want empty initial password file", err)
		}
	})

	t.Run("noninteractive terminal without password source returns an error", func(t *testing.T) {
		requests := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"status": "ok"})
		}))
		defer ts.Close()

		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.toml")
		cfg := fmt.Sprintf("[admin]\naddr = %q\n", ts.URL)
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("WriteFile config: %v", err)
		}

		out, err := runAdminCommand(t, []string{"synaps3", "--config", cfgPath, "admin", "status"})
		if err == nil {
			t.Fatalf("expected error, output:\n%s", out)
		}
		if !strings.Contains(err.Error(), "admin password is required") ||
			!strings.Contains(err.Error(), "SYNAPS3_ADMIN_PASSWORD") ||
			!strings.Contains(err.Error(), "admin-initial-password") {
			t.Fatalf("error = %v, want password source guidance", err)
		}
		if requests != 0 {
			t.Fatalf("requests = %d, want 0", requests)
		}
	})

	t.Run("admin url without explicit config ignores ambient password requirement", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		cfgDir := filepath.Join(home, ".synaps3")
		if err := os.MkdirAll(cfgDir, 0o700); err != nil {
			t.Fatalf("MkdirAll config dir: %v", err)
		}
		ambientConfig := "[admin]\naddr = \"127.0.0.1:1\"\n\n[admin.auth]\nusername = \"root\"\n"
		if err := os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(ambientConfig), 0o600); err != nil {
			t.Fatalf("WriteFile ambient config: %v", err)
		}

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})

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
	t.Setenv(configEnvVar, "")

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

	t.Run("create sends role and prints secret", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/api/v1/s3-users" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("Decode body: %v", err)
			}
			if body["role"] != "admin" || body["name"] != "Backup client" {
				t.Fatalf("create body = %#v", body)
			}
			writeAdminTestJSON(t, w, http.StatusCreated, map[string]string{
				"access_key": "ak",
				"name":       "Backup client",
				"secret_key": "sk",
				"role":       "admin",
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "create", "--name", "Backup client", "--role", "admin", "--yes"})
		if err != nil {
			t.Fatalf("admin s3-user create: %v\n%s", err, out)
		}
		if !strings.Contains(out, "ak") || !strings.Contains(out, "sk") {
			t.Fatalf("create output missing credentials:\n%s", out)
		}
		for _, want := range []string{"S3 User Credentials", "Name: Backup client", "Access key: ak", "Secret key: sk", "Role: admin"} {
			if !strings.Contains(out, want) {
				t.Fatalf("create output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("update name without role and clear it", func(t *testing.T) {
		var names []string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut || r.URL.Path != "/api/v1/s3-users/ak" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if _, hasRole := body["role"]; hasRole {
				t.Fatalf("name-only update sent role: %#v", body)
			}
			names = append(names, body["name"])
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
				"access_key": "ak", "name": body["name"], "role": "user", "bucket_count": 0,
			})
		}))
		defer ts.Close()
		for _, name := range []string{"Archive client", ""} {
			out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "update", "ak", "--name", name})
			if err != nil {
				t.Fatalf("update name %q: %v\n%s", name, err, out)
			}
		}
		if !slices.Equal(names, []string{"Archive client", ""}) {
			t.Fatalf("names = %#v", names)
		}
	})

	t.Run("list shows name and full access key", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeAdminTestJSON(t, w, http.StatusOK, []map[string]any{{
				"access_key": "full-access-key", "name": "Backup client", "role": "user", "bucket_count": 1,
			}})
		}))
		defer ts.Close()
		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "s3-user", "list"})
		if err != nil {
			t.Fatalf("list users: %v\n%s", err, out)
		}
		for _, want := range []string{"NAME", "ACCESS_KEY", "Backup client", "full-access-key"} {
			if !strings.Contains(out, want) {
				t.Fatalf("list missing %q:\n%s", want, out)
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
	t.Setenv(configEnvVar, "")

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
		for _, want := range []string{
			"Settings",
			"Mode: ready",
			"Restart required: no",
			"Server",
			"Filecoin",
			"Cache",
			"100.00 GiB",
			"cache.lru_high_watermark_percent",
			"cache.lru_low_watermark_percent",
			"worker.tasks.concurrency",
			"Logging",
		} {
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
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatalf("Decode body: %v", err)
				}
				cache := body["cache"].(map[string]any)
				if cache["max_size_gb"] != float64(8) {
					t.Fatalf("cache.max_size_gb = %#v, want 8", cache["max_size_gb"])
				}
				if cache["lru_high_watermark_percent"] != float64(85) {
					t.Fatalf("cache.lru_high_watermark_percent = %#v, want 85", cache["lru_high_watermark_percent"])
				}
				if cache["lru_low_watermark_percent"] != float64(70) {
					t.Fatalf("cache.lru_low_watermark_percent = %#v, want 70", cache["lru_low_watermark_percent"])
				}
				worker := body["worker"].(map[string]any)
				tasks := worker["tasks"].(map[string]any)
				if tasks["poll_interval"] != "9s" {
					t.Fatalf("worker.tasks.poll_interval = %#v, want 9s", tasks["poll_interval"])
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
			"settings", "set", "cache.max_size_gb=8", "cache.lru_high_watermark_percent=85",
			"cache.lru_low_watermark_percent=70", "worker.tasks.poll_interval=9s",
			"filecoin.with_cdn=true", "logging.level=debug",
			"logging.s3_access.enabled=false", "logging.s3_access.level=debug",
		})
		if err != nil {
			t.Fatalf("settings set: %v\n%s", err, out)
		}
	})

	t.Run("unsupported fields are rejected before request", func(t *testing.T) {
		for _, setting := range []string{"filecoin.private_key=secret", "filecoin.source=legacy"} {
			t.Run(setting, func(t *testing.T) {
				var called bool
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
				}))
				defer ts.Close()

				out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "settings", "set", setting})
				if err == nil {
					t.Fatalf("expected error, output:\n%s", out)
				}
				if called {
					t.Fatal("request was sent")
				}
			})
		}
	})
}

func TestAdminTaskCommandsAndAPIErrorFields(t *testing.T) {
	t.Setenv(configEnvVar, "")

	t.Run("task list help documents dismissed status", func(t *testing.T) {
		out, err := runAdminCommand(t, []string{"synaps3", "admin", "task", "list", "--help"})
		if err != nil {
			t.Fatalf("task list help: %v\n%s", err, out)
		}
		if !strings.Contains(out, "pending, running, completed, failed, cancelled, or dismissed") {
			t.Fatalf("task list help missing status filters:\n%s", out)
		}
	})

	t.Run("task list query and retry path", func(t *testing.T) {
		var sawList, sawRetry bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks":
				sawList = true
				if got := r.URL.Query().Get("status"); got != "failed" {
					t.Fatalf("status query = %q, want failed", got)
				}
				if got := r.URL.Query().Get("limit"); got != "50" {
					t.Fatalf("limit query = %q, want 50", got)
				}
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"tasks": []any{}})
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

		if out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "list", "--status", "failed", "--limit", "50"}); err != nil {
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

	t.Run("task list includes subject key", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tasks" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
				"tasks": []map[string]any{{
					"id":                  7,
					"type":                "upload_plan",
					"operation":           "Prepare storage",
					"status":              "failed",
					"presentation_status": "Failed",
					"subject_type":        "object_version",
					"subject_key":         "version-1",
					"retry_count":         5,
					"retry_limit":         5,
					"available_at":        "2026-05-05T10:00:00Z",
				}},
			})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "task", "list"})
		if err != nil {
			t.Fatalf("task list: %v\n%s", err, out)
		}
		if !strings.Contains(out, "object_version:version-1") {
			t.Fatalf("task output missing subject key:\n%s", out)
		}
	})

	t.Run("task list json preserves diagnostic and lifecycle fields", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"tasks": []map[string]any{{
				"id": 9, "type": "storage_store", "operation": "Store content",
				"status": "failed", "presentation_status": "dismissed",
				"retry_count": 5, "retry_limit": 5, "retryable": false, "acknowledgeable": false,
				"failure_reason": "provider_error", "last_error": "provider unavailable",
				"available_at": "2026-05-05T10:00:00Z", "started_at": "2026-05-05T10:00:01Z",
				"finished_at": "2026-05-05T10:00:02Z", "acknowledged_at": "2026-05-05T10:00:03Z",
				"created_at": "2026-05-05T09:59:00Z", "updated_at": "2026-05-05T10:00:03Z",
			}}})
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "--json", "task", "list"})
		if err != nil {
			t.Fatalf("task list json: %v\n%s", err, out)
		}
		for _, field := range []string{"failure_reason", "started_at", "finished_at", "acknowledged_at", "created_at", "updated_at"} {
			if !strings.Contains(out, `"`+field+`"`) {
				t.Fatalf("task list json dropped %s: %s", field, out)
			}
		}
	})

	t.Run("task list shows presentation status and message", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tasks" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			writeAdminTestJSON(t, w, http.StatusOK, map[string]any{
				"tasks": []map[string]any{{
					"id":                  8,
					"type":                "cache_evict",
					"operation":           "Free local cache space",
					"subject_type":        "object_version",
					"subject_key":         "version-2",
					"status":              "pending",
					"presentation_status": "Waiting",
					"retry_count":         0,
					"retry_limit":         5,
					"wait_reason":         "durability_pending",
					"status_message":      "Waiting for durable storage",
					"available_at":        "2026-05-05T10:00:00Z",
				}},
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
		if !strings.Contains(out, "Waiting for durable storage") || strings.Contains(out, "durability_pending") {
			t.Fatalf("task output missing waiting details:\n%s", out)
		}
	})
}

func TestAdminStorageConfirmationCommands(t *testing.T) {
	t.Setenv(configEnvVar, "")

	t.Run("list and release", func(t *testing.T) {
		var sawList, sawRelease bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/storage-confirmations":
				sawList = true
				if got := r.URL.Query().Get("status"); got != "needs_attention" {
					t.Fatalf("status = %q, want needs_attention", got)
				}
				if got := r.URL.Query().Get("limit"); got != "25" {
					t.Fatalf("limit = %q, want 25", got)
				}
				writeAdminTestJSON(t, w, http.StatusOK, []map[string]any{{
					"copy_id": 42, "content_id": 7, "copy_index": 1,
					"data_set_row_id": 9, "provider_id": "provider-1", "data_set_id": "dataset-1",
					"piece_cid": "bafy-piece-1", "attempt_id": "attempt-1", "transaction_id": "0xcommit",
					"reason_code":  "attempt_only_ambiguous",
					"attempted_at": "2026-08-30T01:00:00Z", "attention_at": "2026-08-30T01:00:01Z",
				}})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/storage-confirmations/42/release":
				sawRelease = true
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatalf("decode release body: %v", err)
				}
				if body["acknowledge_possible_duplicate"] != true || body["expected_attempt_id"] != "attempt-1" {
					t.Fatal("release acknowledgement was not sent")
				}
				writeAdminTestJSON(t, w, http.StatusOK, map[string]any{"copy_id": 42, "status": "released"})
			default:
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
		}))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "storage-confirmation", "list", "--limit", "25"})
		if err != nil {
			t.Fatalf("storage-confirmation list: %v\n%s", err, out)
		}
		if !strings.Contains(out, "CONTENT ID") || strings.Contains(out, "UPLOAD ID") ||
			!strings.Contains(out, "PIECE CID") || !strings.Contains(out, "ATTEMPTED AT") ||
			!strings.Contains(out, "bafy-piece-1") || !strings.Contains(out, "2026-08-30T01:00:00Z") ||
			!strings.Contains(out, "attempt_only_ambiguous") || !strings.Contains(out, "provider-1") ||
			!strings.Contains(out, "attempt-1") || !strings.Contains(out, "0xcommit") {
			t.Fatalf("list output missing confirmation details:\n%s", out)
		}
		out, err = runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "storage-confirmation", "release", "42", "--attempt-id", "attempt-1", "--yes"})
		if err != nil {
			t.Fatalf("storage-confirmation release: %v\n%s", err, out)
		}
		if !strings.Contains(out, "released for copy 42") {
			t.Fatalf("release output = %q", out)
		}
		if !sawList || !sawRelease {
			t.Fatalf("sawList=%v sawRelease=%v", sawList, sawRelease)
		}
	})

	t.Run("release requires explicit acknowledgement", func(t *testing.T) {
		var called bool
		ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		defer ts.Close()

		out, err := runAdminCommand(t, []string{"synaps3", "admin", "--admin-url", ts.URL, "storage-confirmation", "release", "42", "--attempt-id", "attempt-1"})
		if err == nil || !strings.Contains(err.Error(), "requires --yes") {
			t.Fatalf("error = %v, output=%s", err, out)
		}
		if called {
			t.Fatal("request was sent without --yes")
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
				"with_cdn":               false,
				"allow_private_networks": allowPrivate,
				"default_copies":         3,
			},
			"cache": map[string]any{
				"dir":                        "/tmp/cache",
				"max_size_gb":                100,
				"eviction_policy":            "lru",
				"lru_high_watermark_percent": 90,
				"lru_low_watermark_percent":  80,
			},
			"worker": map[string]any{
				"tasks": map[string]any{
					"concurrency":                      12,
					"poll_interval":                    "5s",
					"lease_duration":                   "5m0s",
					"max_retries":                      5,
					"retention":                        "168h0m0s",
					"provider_mutation_concurrency":    4,
					"destructive_mutation_concurrency": 2,
				},
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
	return slices.Contains(values, want)
}
