package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synapse-go/pdp"
)

type diagnosticLogValue string

func (value diagnosticLogValue) LogValue() slog.Value {
	return slog.GroupValue(slog.String("detail", string(value)), slog.Int("code", 42))
}

func TestSetupLoggerRedactsDiagnostics(t *testing.T) {
	const secret = "diagnostic-secret-value"
	errtext.Configure("", secret)
	t.Cleanup(func() { errtext.Configure("") })

	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			readEnd, writeEnd, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			originalStdout := os.Stdout
			os.Stdout = writeEnd
			t.Cleanup(func() {
				os.Stdout = originalStdout
				_ = writeEnd.Close()
				_ = readEnd.Close()
			})

			cause := &pdp.HTTPError{
				Method: "POST", URL: "https://provider.example/pdp/pieces?token=hidden",
				StatusCode: 503, Body: "provider busy: " + secret,
			}
			logger := setupLogger(config.LoggingConfig{Level: "info", Format: format})
			logger.With("attached", secret).WithGroup("request").Info("Could not upload "+secret,
				"error", fmt.Errorf("storing piece: %w", cause),
				"body", secret,
				slog.Group("details", slog.String("value", secret), slog.Int("attempt", 2), slog.Bool("ready", false)),
				slog.Any("resolved", diagnosticLogValue(secret)),
			)
			if err := writeEnd.Close(); err != nil {
				t.Fatal(err)
			}
			output, err := io.ReadAll(readEnd)
			if err != nil {
				t.Fatal(err)
			}
			text := string(output)
			for _, forbidden := range []string{secret, "token=hidden"} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("log contains %q: %s", forbidden, text)
				}
			}
			for _, want := range []string{"[redacted]", "storing piece", "https://provider.example/pdp/pieces", "HTTP 503", "provider busy"} {
				if !strings.Contains(text, want) {
					t.Fatalf("log is missing %q: %s", want, text)
				}
			}
			if format == "json" {
				var record map[string]any
				if err := json.Unmarshal(output, &record); err != nil {
					t.Fatal(err)
				}
				request, ok := record["request"].(map[string]any)
				if !ok {
					t.Fatalf("request group is missing: %s", text)
				}
				details, ok := request["details"].(map[string]any)
				if !ok || details["attempt"] != float64(2) || details["ready"] != false {
					t.Fatalf("grouped values changed type or value: %s", text)
				}
				resolved, ok := request["resolved"].(map[string]any)
				if !ok || resolved["detail"] != "[redacted]" || resolved["code"] != float64(42) || record["attached"] != "[redacted]" || request["body"] != "[redacted]" {
					t.Fatalf("resolved or attached values were not preserved safely: %s", text)
				}
			} else if !strings.Contains(text, "request.details.attempt=2") || !strings.Contains(text, "request.details.ready=false") || !strings.Contains(text, "request.resolved.code=42") {
				t.Fatalf("text log lost grouped values: %s", text)
			}
		})
	}
}

func TestLoadConfigAndDBRegistersSecretsBeforeDatabaseFailure(t *testing.T) {
	const secret = "startup-private-key"
	cfgPath := writeWalletTestConfig(t, secret)
	t.Setenv("SYNAPS3_DATABASE_DSN", "")
	t.Setenv("SYNAPS3_FILECOIN_PRIVATE_KEY", secret)
	t.Cleanup(func() { errtext.Configure("") })

	_, _, err := loadConfigAndDB(t.Context(), config.Source{Path: cfgPath})
	if err == nil || !strings.Contains(err.Error(), "opening database") {
		t.Fatalf("expected a database initialization failure, got %v", err)
	}
	if got := errtext.Format(fmt.Errorf("initializing client with %s", secret)); strings.Contains(got, secret) || !strings.Contains(got, "[redacted]") {
		t.Fatalf("startup secret was not registered: %s", got)
	}
}

func TestWalletDepositRegistersSecretsBeforeClientFailure(t *testing.T) {
	const secret = "wallet-private-key"
	cfgPath := writeWalletTestConfig(t, secret)
	t.Setenv("SYNAPS3_FILECOIN_PRIVATE_KEY", secret)
	t.Cleanup(func() { errtext.Configure("") })
	cause := errors.New("client initialization failed")
	originalDeposit := walletDeposit
	walletDeposit = func(_ context.Context, cfg *config.Config, _ *big.Int, _ time.Duration) (walletDepositResult, error) {
		return walletDepositResult{}, fmt.Errorf("using %s: %w", cfg.Filecoin.PrivateKey, cause)
	}
	t.Cleanup(func() { walletDeposit = originalDeposit })

	_, err := runWalletCommand(t, []string{"synaps3", "--config", cfgPath, "wallet", "deposit", "1"})
	if !errors.Is(err, cause) {
		t.Fatalf("client error identity was lost: %v", err)
	}
	if got := errtext.Format(err); strings.Contains(got, secret) || !strings.Contains(got, "[redacted]") || !strings.Contains(got, cause.Error()) {
		t.Fatalf("client error was not preserved safely: %s", got)
	}
}
