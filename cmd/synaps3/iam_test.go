package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/config"
	"github.com/versity/versitygw/auth"
)

func TestNewS3IAMServiceInitializesUsersFile(t *testing.T) {
	iamDir := t.TempDir()

	iamSvc, err := newS3IAMService(config.S3Config{
		AccessKey: "root-access",
		SecretKey: "root-secret",
		IAMDir:    iamDir,
	})
	if err != nil {
		t.Fatalf("newS3IAMService: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	data, err := os.ReadFile(filepath.Join(iamDir, "users.json"))
	if err != nil {
		t.Fatalf("users.json was not initialized: %v", err)
	}
	var stored struct {
		AccessAccounts map[string]auth.Account `json:"accessAccounts"`
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("users.json is not valid IAM JSON: %v", err)
	}
	if _, ok := stored.AccessAccounts["root-access"]; ok {
		t.Fatalf("root access key should remain config-backed and not be written to users.json: %s", data)
	}
}

func TestBootstrapS3RootCredentialsReloadsServeConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml"), Exists: true}
	if err := os.WriteFile(source.Path, []byte(`
s3:
  region: us-east-1
filecoin:
  private_key: manual-filecoin-private-key
`), 0o600); err != nil {
		t.Fatalf("WriteFile initial config: %v", err)
	}
	cfg, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource: %v", err)
	}
	if strings.TrimSpace(cfg.S3.AccessKey) != "" || strings.TrimSpace(cfg.S3.SecretKey) != "" {
		t.Fatalf("initial S3 credentials should be missing: %#v", cfg.S3)
	}

	cfg, changed, err := bootstrapS3RootCredentials(source, cfg)
	if err != nil {
		t.Fatalf("bootstrapS3RootCredentials: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if strings.TrimSpace(cfg.S3.AccessKey) == "" || strings.TrimSpace(cfg.S3.SecretKey) == "" {
		t.Fatalf("bootstrapped config did not reload generated credentials: %#v", cfg.S3)
	}
}

func TestNewS3IAMServiceRejectsUnknownAccessKeyAsNoSuchUser(t *testing.T) {
	iamSvc, err := newS3IAMService(config.S3Config{
		AccessKey: "root-access",
		SecretKey: "root-secret",
		IAMDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("newS3IAMService: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	acct, err := iamSvc.GetUserAccount("wrong-access")
	if !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("GetUserAccount wrong access error = %v, want ErrNoSuchUser", err)
	}
	if acct != (auth.Account{}) {
		t.Fatalf("account = %#v, want zero value", acct)
	}
}

func TestNewS3IAMServiceReturnsRootAccount(t *testing.T) {
	iamSvc, err := newS3IAMService(config.S3Config{
		AccessKey: "root-access",
		SecretKey: "root-secret",
		IAMDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("newS3IAMService: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	acct, err := iamSvc.GetUserAccount("root-access")
	if err != nil {
		t.Fatalf("GetUserAccount root access: %v", err)
	}
	if acct.Access != "root-access" || acct.Secret != "root-secret" || acct.Role != auth.RoleAdmin {
		t.Fatalf("account = %#v, want root admin account", acct)
	}
}
