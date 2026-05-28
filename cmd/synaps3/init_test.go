package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
)

func TestRootCommandWithoutSubcommandShowsHelp(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(t.TempDir())

	var out bytes.Buffer
	cmd := newRootCommand()
	cmd.Writer = &out
	cmd.ErrWriter = &out

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := cmd.Run(ctx, []string{"synaps3"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	text := out.String()
	if !strings.Contains(text, "USAGE:") || !strings.Contains(text, "init") {
		t.Fatalf("help output missing expected content:\n%s", text)
	}
	if _, err := os.Stat(filepath.Join(home, ".synaps3", "db")); !os.IsNotExist(err) {
		t.Fatalf("root help should not create runtime data, stat error = %v", err)
	}
}

func TestInitCommandCreatesConfigInCustomDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "synaps3-data")

	var out bytes.Buffer
	cmd := newRootCommand()
	cmd.Writer = &out
	if err := cmd.Run(context.Background(), []string{"synaps3", "init", "--dir", dir}); err != nil {
		t.Fatalf("init command error = %v", err)
	}

	cfgPath := filepath.Join(dir, "config.toml")
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("Stat(%q): %v", cfgPath, err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load(%q): %v", cfgPath, err)
	}
	if cfg.Cache.Dir != filepath.Join(dir, "cache") {
		t.Fatalf("Cache.Dir = %q, want %q", cfg.Cache.Dir, filepath.Join(dir, "cache"))
	}
	if cfg.Admin.Auth.PasswordHash == "" {
		t.Fatal("Admin.Auth.PasswordHash is empty")
	}
	passwordPath := filepath.Join(dir, "admin-initial-password")
	password, err := os.ReadFile(passwordPath)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", passwordPath, err)
	}
	if strings.TrimSpace(string(password)) == "" {
		t.Fatal("admin initial password file is empty")
	}
	info, err := os.Stat(passwordPath)
	if err != nil {
		t.Fatalf("Stat(%q): %v", passwordPath, err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("password file mode = %o, want 600", got)
	}
	if strings.Contains(out.String(), strings.TrimSpace(string(password))) {
		t.Fatal("non-interactive init output must not print the admin password")
	}
	if !strings.Contains(out.String(), passwordPath) {
		t.Fatalf("init output must include password file path:\n%s", out.String())
	}
}

func TestInitCommandFailsWhenConfigAlreadyExists(t *testing.T) {
	dir := t.TempDir()
	if err := newRootCommand().Run(context.Background(), []string{"synaps3", "init", "--dir", dir}); err != nil {
		t.Fatalf("first init command error = %v", err)
	}

	err := newRootCommand().Run(context.Background(), []string{"synaps3", "init", "--dir", dir})
	if err == nil {
		t.Fatal("expected second init command to fail")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want already exists", err)
	}
}

func TestInitCommandRejectsRootConfigFlag(t *testing.T) {
	dir := t.TempDir()
	err := newRootCommand().Run(context.Background(), []string{"synaps3", "--config", filepath.Join(dir, "ignored.toml"), "init", "--dir", dir})
	if err == nil {
		t.Fatal("expected root --config with init to fail")
	}
	if !strings.Contains(err.Error(), "--dir") {
		t.Fatalf("error = %v, want --dir guidance", err)
	}
}

func TestInitCommandRejectsPositionalArgs(t *testing.T) {
	err := newRootCommand().Run(context.Background(), []string{"synaps3", "init", "extra"})
	if err == nil {
		t.Fatal("expected positional arg error")
	}
	if !strings.Contains(err.Error(), "unexpected argument") {
		t.Fatalf("error = %v, want unexpected argument", err)
	}
}
