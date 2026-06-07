package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func TestAdminAuthResetPasswordUpdatesConfigAndWritesPasswordFile(t *testing.T) {
	dir := t.TempDir()
	initCmd := newRootCommand()
	initCmd.Writer = &bytes.Buffer{}
	if err := initCmd.Run(context.Background(), []string{"synaps3", "init", "--dir", dir}); err != nil {
		t.Fatalf("init command error = %v", err)
	}
	cfgPath := filepath.Join(dir, "config.toml")
	before, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load before reset: %v", err)
	}

	var out bytes.Buffer
	cmd := newRootCommand()
	cmd.Writer = &out
	if err := cmd.Run(context.Background(), []string{"synaps3", "--config", cfgPath, "admin-auth", "reset-password"}); err != nil {
		t.Fatalf("reset-password command error = %v", err)
	}

	after, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load after reset: %v", err)
	}
	if after.Admin.Auth.PasswordHash == "" || after.Admin.Auth.PasswordHash == before.Admin.Auth.PasswordHash {
		t.Fatalf("password hash was not updated")
	}
	if after.Admin.Auth.SessionSecret == "" || after.Admin.Auth.SessionSecret == before.Admin.Auth.SessionSecret {
		t.Fatalf("session secret was not updated")
	}
	if _, err := bcrypt.Cost([]byte(after.Admin.Auth.PasswordHash)); err != nil {
		t.Fatalf("updated password hash is not bcrypt: %v", err)
	}
	passwordPath := filepath.Join(dir, "admin-initial-password")
	password, err := os.ReadFile(passwordPath)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", passwordPath, err)
	}
	if bcrypt.CompareHashAndPassword([]byte(after.Admin.Auth.PasswordHash), []byte(strings.TrimSpace(string(password)))) != nil {
		t.Fatal("password file does not match updated hash")
	}
	if strings.Contains(string(mustReadConfigFile(t, cfgPath)), strings.TrimSpace(string(password))) {
		t.Fatal("config contains plaintext reset password")
	}
	if !strings.Contains(out.String(), passwordPath) {
		t.Fatalf("reset output must include password file path:\n%s", out.String())
	}
}

func TestAdminAuthResetPasswordUsesSYNAPS3Config(t *testing.T) {
	dir := t.TempDir()
	initCmd := newRootCommand()
	initCmd.Writer = &bytes.Buffer{}
	if err := initCmd.Run(context.Background(), []string{"synaps3", "init", "--dir", dir}); err != nil {
		t.Fatalf("init command error = %v", err)
	}
	cfgPath := filepath.Join(dir, "config.toml")
	before, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load before reset: %v", err)
	}
	t.Setenv(configEnvVar, cfgPath)

	var out bytes.Buffer
	cmd := newRootCommand()
	cmd.Writer = &out
	if err := cmd.Run(context.Background(), []string{"synaps3", "admin-auth", "reset-password"}); err != nil {
		t.Fatalf("reset-password command error = %v", err)
	}

	after, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load after reset: %v", err)
	}
	if after.Admin.Auth.PasswordHash == before.Admin.Auth.PasswordHash {
		t.Fatal("password hash was not updated")
	}
	if after.Admin.Auth.SessionSecret == before.Admin.Auth.SessionSecret {
		t.Fatal("session secret was not updated")
	}
	passwordPath := filepath.Join(dir, "admin-initial-password")
	if !strings.Contains(out.String(), passwordPath) {
		t.Fatalf("reset output must include password file path:\n%s", out.String())
	}
}

func TestAdminAuthResetPasswordRequiresConfigSource(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(configEnvVar, " \t ")
	initCmd := newRootCommand()
	initCmd.Writer = &bytes.Buffer{}
	if err := initCmd.Run(context.Background(), []string{"synaps3", "init"}); err != nil {
		t.Fatalf("init command error = %v", err)
	}

	err := newRootCommand().Run(context.Background(), []string{"synaps3", "admin-auth", "reset-password"})
	if err == nil {
		t.Fatal("expected reset-password without config source to fail")
	}
	if !strings.Contains(err.Error(), "requires --config or SYNAPS3_CONFIG") {
		t.Fatalf("error = %v, want config source requirement", err)
	}
}

func TestAdminAuthResetPasswordRestoresPasswordFileWhenConfigSaveFails(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	if err := newRootCommand().Run(context.Background(), []string{"synaps3", "init", "--dir", dir}); err != nil {
		t.Fatalf("init command error = %v", err)
	}
	beforeConfig := string(mustReadConfigFile(t, cfgPath))
	passwordPath := filepath.Join(dir, "admin-initial-password")
	oldPassword := []byte("old-password\n")
	if err := os.WriteFile(passwordPath, oldPassword, 0o640); err != nil {
		t.Fatalf("WriteFile old password: %v", err)
	}
	if err := os.Chmod(passwordPath, 0o640); err != nil {
		t.Fatalf("Chmod old password: %v", err)
	}

	saveErr := errors.New("forced save failure")
	origSave := saveAdminAuthSettings
	saveAdminAuthSettings = func(string, *config.Config, config.PersistedFieldPresence) error {
		return saveErr
	}
	t.Cleanup(func() { saveAdminAuthSettings = origSave })

	cmd := newRootCommand()
	err := cmd.Run(context.Background(), []string{"synaps3", "--config", cfgPath, "admin-auth", "reset-password"})
	if !errors.Is(err, saveErr) {
		t.Fatalf("reset-password error = %v, want forced save failure", err)
	}
	if got := string(mustReadConfigFile(t, cfgPath)); got != beforeConfig {
		t.Fatalf("config changed after failed save:\n%s", got)
	}
	afterPassword, err := os.ReadFile(passwordPath)
	if err != nil {
		t.Fatalf("ReadFile restored password: %v", err)
	}
	if string(afterPassword) != string(oldPassword) {
		t.Fatalf("password file = %q, want %q", string(afterPassword), string(oldPassword))
	}
	info, err := os.Stat(passwordPath)
	if err != nil {
		t.Fatalf("Stat password file: %v", err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("password file mode = %o, want 640", info.Mode().Perm())
	}
}

func mustReadConfigFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	return data
}
