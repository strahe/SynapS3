package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/strahe/synaps3/internal/testpg"
)

const postgresDSNEnv = "SYNAPS3_POSTGRES_TEST_DSN"

type postgresLifecycle struct {
	baseDSN  func(context.Context) (string, error)
	newToken func() (string, error)
	cleanup  func(context.Context, string, string) error
	close    func() error
}

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	cmd := exec.Command("go", append([]string{"test"}, args...)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	pg := postgresLifecycle{
		baseDSN: testpg.BaseDSNContext, newToken: testpg.NewRunToken,
		cleanup: testpg.CleanupRun, close: testpg.Close,
	}
	if err := run(cmd, signals, pg, 10*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "test run failed: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *exec.Cmd, signals <-chan os.Signal, pg postgresLifecycle, grace time.Duration) (runErr error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := make(chan os.Signal, 1)
	go func() {
		select {
		case received := <-signals:
			cancel()
			interrupted <- received
		case <-ctx.Done():
		}
	}()
	defer func() {
		if err := pg.close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close test PostgreSQL: %w", err))
		}
	}()
	token, err := pg.newToken()
	if err != nil {
		return fmt.Errorf("create test run token: %w", err)
	}
	dsn, err := pg.baseDSN(ctx)
	if err != nil {
		return fmt.Errorf("start test PostgreSQL: %w", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := pg.cleanup(cleanupCtx, dsn, token); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("clean up test PostgreSQL resources: %w", err))
		}
	}()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tests interrupted: %w", err)
	}
	cmd.Env = testEnvironment(cmd.Environ(), dsn, token)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start go test: %w", err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	select {
	case err := <-wait:
		if ctx.Err() != nil {
			completed := make(chan error, 1)
			completed <- err
			received := <-interrupted
			return errors.Join(fmt.Errorf("tests interrupted by %s", received), stopProcessGroup(cmd.Process.Pid, received, completed, grace))
		}
		return err
	case received := <-interrupted:
		return errors.Join(fmt.Errorf("tests interrupted by %s", received), stopProcessGroup(cmd.Process.Pid, received, wait, grace))
	}
}

func testEnvironment(base []string, dsn, token string) []string {
	env := make([]string, 0, len(base)+2)
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if key != postgresDSNEnv && key != testpg.RunTokenEnv {
			env = append(env, entry)
		}
	}
	return append(env, postgresDSNEnv+"="+dsn, testpg.RunTokenEnv+"="+token)
}

func stopProcessGroup(pid int, received os.Signal, wait <-chan error, grace time.Duration) error {
	sig, ok := received.(syscall.Signal)
	if !ok {
		sig = syscall.SIGTERM
	}
	var stopErr error
	if err := signalProcessGroup(pid, sig); err != nil {
		stopErr = errors.Join(stopErr, err)
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	var childErr error
	var reaped, killed bool
	for {
		if reaped {
			exists, err := processGroupExists(pid)
			if err != nil {
				return errors.Join(stopErr, childErr, err)
			}
			if !exists {
				return errors.Join(stopErr, childErr)
			}
		}
		select {
		case childErr = <-wait:
			reaped = true
			wait = nil
		case <-poll.C:
		case <-timer.C:
			if killed {
				if !reaped {
					childErr = <-wait
				}
				return errors.Join(stopErr, childErr, errors.New("test process group did not exit after SIGKILL"))
			}
			stopErr = errors.Join(stopErr, signalProcessGroup(pid, syscall.SIGKILL))
			killed = true
			timer.Reset(time.Second)
		}
	}
}

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if err := syscall.Kill(-pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("signal test process group: %w", err)
	}
	return nil
}

func processGroupExists(pid int) (bool, error) {
	err := syscall.Kill(-pid, 0)
	if err == nil || errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return false, fmt.Errorf("inspect test process group: %w", err)
}
