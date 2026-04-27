package main

import (
	"testing"

	"github.com/strahe/synaps3/internal/config"
)

func TestShouldStartSetupModeAllowsMissingManualCredentials(t *testing.T) {
	cfg, err := config.DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}

	for _, want := range []string{"s3.access_key", "s3.secret_key", "filecoin.private_key"} {
		if !hasServeConfigFieldError(cfg.FieldValidationErrors(), want) {
			t.Fatalf("FieldValidationErrors() missing %q: %#v", want, cfg.FieldValidationErrors())
		}
	}

	if !shouldStartSetupMode(cfg.FieldValidationErrors()) {
		t.Fatal("missing manual credentials should allow setup mode")
	}
}

func TestShouldStartSetupModeAllowsEditableConfigErrors(t *testing.T) {
	cfg := validServeConfig(t)
	cfg.Server.Port = "not-a-port"
	cfg.S3.Region = ""
	cfg.Filecoin.RPCURL = "ftp://example.invalid/rpc"
	cfg.Filecoin.Source = ""
	cfg.Cache.MaxSizeGB = 0
	cfg.Worker.Upload.PollInterval = 0
	cfg.Worker.Upload.MaxRetries = -1
	cfg.Logging.Level = "verbose"

	if !shouldStartSetupMode(cfg.FieldValidationErrors()) {
		t.Fatal("editable config validation errors should allow setup mode")
	}
}

func TestShouldStartSetupModeRejectsManualOnlyErrors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{
			name: "database driver",
			mutate: func(cfg *config.Config) {
				cfg.Database.Driver = "mysql"
			},
		},
		{
			name: "admin address",
			mutate: func(cfg *config.Config) {
				cfg.Admin.Addr = ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validServeConfig(t)
			tt.mutate(cfg)

			if shouldStartSetupMode(cfg.FieldValidationErrors()) {
				t.Fatal("manual-only config errors should remain fatal")
			}
		})
	}
}

func validServeConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg, err := config.DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.S3.AccessKey = "admin"
	cfg.S3.SecretKey = "password"
	cfg.Filecoin.PrivateKey = "filecoin-private-key"
	return cfg
}

func hasServeConfigFieldError(errs []config.FieldError, field string) bool {
	for _, err := range errs {
		if err.Field == field {
			return true
		}
	}
	return false
}
