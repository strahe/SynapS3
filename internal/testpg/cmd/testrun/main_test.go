package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/testpg"
)

const helperEnv = "SYNAPS3_TEST_RUNNER_HELPER"

func TestRunPassesEnvironmentOnlyToChild(t *testing.T) {
	t.Setenv(postgresDSNEnv, "parent-dsn")
	t.Setenv(testpg.RunTokenEnv, "parent-token")
	t.Setenv("CGO_ENABLED", "1")
	t.Setenv("GOFLAGS", "-trimpath")
	cmd := helperCommand("environment")
	var output bytes.Buffer
	cmd.Stdout = &output
	pg, events := stubPostgres()
	if err := run(cmd, make(chan os.Signal), pg, time.Second); err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("decode child environment: %v; output %q", err, output.String())
	}
	want := map[string]string{
		postgresDSNEnv: "child-dsn", testpg.RunTokenEnv: "child-token",
		"CGO_ENABLED": "1", "GOFLAGS": "-trimpath",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("child environment = %v, want %v", got, want)
	}
	if os.Getenv(postgresDSNEnv) != "parent-dsn" || os.Getenv(testpg.RunTokenEnv) != "parent-token" {
		t.Fatal("runner changed its own PostgreSQL environment")
	}
	if !reflect.DeepEqual(*events, []string{"open", "cleanup", "close"}) {
		t.Fatalf("lifecycle = %v", *events)
	}
}

func TestRunReportsFailuresAndStillCleansUp(t *testing.T) {
	for _, failure := range []string{"startup", "start command", "tests", "cleanup", "close"} {
		t.Run(failure, func(t *testing.T) {
			cmd := helperCommand("success")
			pg, events := stubPostgres()
			sentinel := errors.New("injected failure")
			switch failure {
			case "startup":
				pg.baseDSN = func(context.Context) (string, error) {
					*events = append(*events, "open")
					return "", sentinel
				}
			case "start command":
				cmd = exec.Command(filepath.Join(t.TempDir(), "missing-command"))
			case "tests":
				cmd = helperCommand("failure")
			case "cleanup":
				pg.cleanup = func(context.Context, string, string) error {
					*events = append(*events, "cleanup")
					return sentinel
				}
			case "close":
				pg.close = func() error {
					*events = append(*events, "close")
					return sentinel
				}
			}
			err := run(cmd, make(chan os.Signal), pg, time.Second)
			if err == nil {
				t.Fatal("run succeeded after a lifecycle failure")
			}
			if (failure == "startup" || failure == "cleanup" || failure == "close") && !errors.Is(err, sentinel) {
				t.Fatalf("run error = %v, want injected failure", err)
			}
			want := []string{"open", "cleanup", "close"}
			if failure == "startup" {
				want = []string{"open", "close"}
			}
			if !reflect.DeepEqual(*events, want) {
				t.Fatalf("lifecycle = %v, want %v", *events, want)
			}
		})
	}
}

func TestRunCancellationStopsPostgreSQLStartup(t *testing.T) {
	pg, events := stubPostgres()
	opening := make(chan struct{})
	pg.baseDSN = func(ctx context.Context) (string, error) {
		*events = append(*events, "open")
		close(opening)
		<-ctx.Done()
		return "", ctx.Err()
	}
	signals := make(chan os.Signal, 1)
	go func() {
		<-opening
		signals <- syscall.SIGTERM
	}()
	if err := run(helperCommand("success"), signals, pg, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(*events, []string{"open", "close"}) {
		t.Fatalf("lifecycle = %v", *events)
	}
}

func TestRunRetainsTestAndCleanupFailures(t *testing.T) {
	pg, _ := stubPostgres()
	cleanupErr := errors.New("cleanup failed")
	closeErr := errors.New("close failed")
	pg.cleanup = func(context.Context, string, string) error { return cleanupErr }
	pg.close = func() error { return closeErr }
	err := run(helperCommand("failure"), make(chan os.Signal), pg, time.Second)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || !errors.Is(err, cleanupErr) || !errors.Is(err, closeErr) {
		t.Fatalf("run lost a test or cleanup failure: %v", err)
	}
}

func TestRunForwardsSignalBeforeCleanup(t *testing.T) {
	for _, received := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(received.String(), func(t *testing.T) {
			ready := filepath.Join(t.TempDir(), "ready")
			cmd := helperCommand("signal")
			cmd.Env = append(cmd.Env, "SYNAPS3_TEST_RUNNER_READY="+ready)
			pg, _ := stubPostgres()
			pg.cleanup = func(ctx context.Context, _, _ string) error {
				if ctx.Err() != nil {
					t.Errorf("cleanup received a canceled context: %v", ctx.Err())
					return ctx.Err()
				}
				data, err := os.ReadFile(ready + ".received")
				if err != nil {
					return fmt.Errorf("child did not handle signal before cleanup: %w", err)
				}
				if string(data) != received.String() {
					return fmt.Errorf("child received %q, want %s", data, received)
				}
				return nil
			}
			signals := make(chan os.Signal, 1)
			readyErr := make(chan error, 1)
			go func() {
				readyErr <- waitForFile(ready)
				signals <- received
			}()
			err := run(cmd, signals, pg, time.Second)
			if readinessErr := <-readyErr; readinessErr != nil {
				t.Fatal(readinessErr)
			}
			if err == nil || !strings.Contains(err.Error(), "interrupted") {
				t.Fatalf("run error = %v, want interruption", err)
			}
			data, err := os.ReadFile(ready + ".received")
			if err != nil || string(data) != received.String() {
				t.Fatalf("forwarded signal = %q, want %s: %v", data, received, err)
			}
		})
	}
}

func TestRunKillsRemainingDescendantBeforeCleanup(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := helperCommand("descendant")
	cmd.Env = append(cmd.Env, "SYNAPS3_TEST_RUNNER_READY="+ready)
	pg, _ := stubPostgres()
	var cleanupErr error
	pg.cleanup = func(context.Context, string, string) error {
		exists, err := processGroupExists(cmd.Process.Pid)
		if err != nil {
			cleanupErr = err
		} else if exists {
			cleanupErr = errors.New("descendant still running during cleanup")
		}
		return cleanupErr
	}
	signals := make(chan os.Signal, 1)
	readyErr := make(chan error, 1)
	go func() {
		readyErr <- waitForFile(ready)
		signals <- syscall.SIGTERM
	}()
	err := run(cmd, signals, pg, 50*time.Millisecond)
	if readinessErr := <-readyErr; readinessErr != nil {
		t.Fatal(readinessErr)
	}
	if err == nil {
		t.Fatal("interrupted run succeeded")
	}
	if cleanupErr != nil {
		t.Fatal(cleanupErr)
	}
	if !cmd.ProcessState.Success() {
		t.Fatalf("group leader did not exit gracefully: %v", cmd.ProcessState)
	}
}

func stubPostgres() (postgresLifecycle, *[]string) {
	events := new([]string)
	return postgresLifecycle{
		baseDSN: func(context.Context) (string, error) {
			*events = append(*events, "open")
			return "child-dsn", nil
		},
		newToken: func() (string, error) { return "child-token", nil },
		cleanup: func(ctx context.Context, dsn, token string) error {
			*events = append(*events, "cleanup")
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if dsn != "child-dsn" || token != "child-token" {
				return errors.New("cleanup received the wrong PostgreSQL DSN or run token")
			}
			return nil
		},
		close: func() error {
			*events = append(*events, "close")
			return nil
		},
	}, events
}

func helperCommand(mode string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestRunnerSubprocess$", "--", mode)
	cmd.Env = append(os.Environ(), helperEnv+"=1", "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	return cmd
}

func waitForFile(path string) error {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-deadline.C:
			return errors.New("child readiness timed out")
		case <-poll.C:
		}
	}
}

func TestRunnerSubprocess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	ready := os.Getenv("SYNAPS3_TEST_RUNNER_READY")
	markReady := func(path string) {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			os.Exit(90)
		}
	}
	switch mode {
	case "environment":
		env := make(map[string]string)
		for _, key := range []string{postgresDSNEnv, testpg.RunTokenEnv, "CGO_ENABLED", "GOFLAGS"} {
			env[key] = os.Getenv(key)
		}
		if err := json.NewEncoder(os.Stdout).Encode(env); err != nil {
			os.Exit(91)
		}
	case "failure":
		os.Exit(7)
	case "signal", "descendant":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		if mode == "descendant" {
			child := helperCommand("ignore")
			child.Env = append(child.Env, "SYNAPS3_TEST_RUNNER_READY="+ready+".child")
			if err := child.Start(); err != nil {
				os.Exit(92)
			}
			if err := waitForFile(ready + ".child"); err != nil {
				os.Exit(93)
			}
		}
		markReady(ready)
		received := <-signals
		if err := os.WriteFile(ready+".received", []byte(received.String()), 0o600); err != nil {
			os.Exit(94)
		}
	case "ignore":
		signal.Ignore(syscall.SIGTERM)
		markReady(ready)
		for {
			time.Sleep(time.Hour)
		}
	}
	os.Exit(0)
}
