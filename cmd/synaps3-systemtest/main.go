//go:build systemtest

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/strahe/synaps3/internal/systemtest"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "systemtest server: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	harness, err := systemtest.NewHarness(ctx, logger)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"admin_url": harness.AdminURL}); err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		return errors.Join(err, harness.Close(closeCtx))
	}

	select {
	case <-ctx.Done():
	case <-harness.Done():
		if err := harness.Err(); err != nil {
			return err
		}
		return fmt.Errorf("runtime stopped unexpectedly")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	return harness.Close(closeCtx)
}
