//go:build integration

package calibration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/tests/testutil/e2e"
	sdktypes "github.com/strahe/synapse-go/types"
)

const (
	integrationNetwork = "calibration"
	integrationCopies  = 3
	uploadTaskTimeout  = 2 * time.Minute

	adminUsername = "admin"
)

var uploadDepositPattern = regexp.MustCompile(`deposit ([0-9]+) USDFC base units`)

var errLoopbackAddressReservation = errors.New("loopback address reservation failed")

func TestCalibrationBackedGoldenPath(t *testing.T) {
	repoRoot, err := e2e.RepoRootFrom(".")
	if err != nil {
		t.Fatalf("locate repo root: %v", err)
	}
	privateKey, err := e2e.LoadCalibrationPrivateKey(repoRoot)
	if err != nil {
		t.Fatalf("load %s: %v", e2e.TestEnvFileName, err)
	}
	logStep(t, "loaded %s", e2e.TestEnvFileName)

	runtime := startCalibrationRuntime(t, repoRoot, privateKey)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("calibration: runtime diagnostics %s", runtime.Diagnostics())
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("stop calibration runtime: %v\n%s", err, runtime.Diagnostics())
		}
	})

	admin := runtime.Admin
	walletActions := &calibrationWalletActions{}
	logStep(t, "checking Calibration readiness")
	ensureCalibrationReadiness(t, t.Context(), admin, walletActions)
	preflightCalibrationProviders(t, t.Context(), admin)

	logStep(t, "creating S3 user")
	credentials := admin.CreateS3User(t, t.Context())
	s3Client := e2e.NewS3Client("http://"+runtime.S3Addr, credentials.AccessKey, credentials.SecretKey)
	bucket := uniqueBucketName()
	key := "objects/calibration-golden.bin"
	logStep(t, "creating bucket %s", bucket)
	if _, err := s3Client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v\n%s", err, runtime.Diagnostics())
	}

	content := bytes.Repeat([]byte("synaps3-calibration-e2e\n"), 6000)
	checksum := sha256.Sum256(content)
	if len(content) < 128*1024 {
		t.Fatalf("test content is %d bytes, want at least 128 KiB", len(content))
	}
	logStep(t, "uploading object %s/%s size=%d sha256=%x", bucket, key, len(content), checksum)
	if _, err := s3Client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	}); err != nil {
		t.Fatalf("PutObject: %v\n%s", err, runtime.Diagnostics())
	}
	logStep(t, "verifying immediate S3 read")
	e2e.AssertS3Object(t, t.Context(), s3Client, bucket, key, content, checksum)

	object := waitForStoredObject(t, admin, bucket, key, walletActions)
	provenance := waitForCommittedCopies(t, admin, bucket, object.VersionID)
	assertDataSetMetadata(t, t.Context(), privateKey, bucket, provenance)
	waitForCompletedUploadTasks(t, admin, object.VersionID)
	waitForCacheEviction(t, admin, bucket, key)
	logStep(t, "verifying cold S3 read after cache eviction")
	waitForS3Object(t, runtime, s3Client, bucket, key, content, checksum)
	waitForObservabilitySnapshot(t, admin, bucket, provenance)
	logStep(t, "Calibration integration workflow completed")
}

type calibrationWalletActions struct {
	ApproveAttempted bool
	FundAttempted    bool
}

func (a *calibrationWalletActions) beginFund() error {
	if a == nil {
		return errors.New("wallet action state is required")
	}
	if a.FundAttempted {
		return errors.New("fund was already attempted; refusing a second transaction")
	}
	a.FundAttempted = true
	return nil
}

type calibrationRuntime struct {
	BinaryPath    string
	AppDir        string
	ConfigPath    string
	AdminAddr     string
	S3Addr        string
	AdminPassword string
	Admin         *e2e.AdminClient

	process *calibrationProcess
	stdout  *e2e.BoundedLog
	stderr  *e2e.BoundedLog
}

type calibrationProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startCalibrationRuntime(t *testing.T, repoRoot, privateKey string) *calibrationRuntime {
	t.Helper()
	runtime := &calibrationRuntime{
		BinaryPath: filepath.Join(repoRoot, "bin", "synaps3-integration-server"),
		AppDir:     t.TempDir(),
	}
	if info, err := os.Stat(runtime.BinaryPath); err != nil || info.IsDir() {
		t.Fatalf("integration binary %s is unavailable; run make build-integration-server first", runtime.BinaryPath)
	}
	logStep(t, "initializing isolated app dir")
	if err := runtime.Init(t.Context()); err != nil {
		t.Fatalf("initialize calibration app dir: %s", runtime.sanitizeDiagnostics(err.Error()))
	}
	logStep(t, "app dir initialized")

	for attempt := 1; attempt <= 3; attempt++ {
		logStep(t, "preparing config attempt=%d", attempt)
		if err := runtime.PrepareConfig(); err != nil {
			if attempt < 3 && runtime.RetryableStartupFailure(err) {
				logStep(t, "config port reservation failed; retrying with fresh ports")
				continue
			}
			t.Fatalf("prepare calibration config: %s", runtime.sanitizeDiagnostics(err.Error()))
		}
		runtime.Admin = e2e.NewAdminClient(t, "http://"+runtime.AdminAddr, e2e.WithAdminTimeout(90*time.Second))
		logStep(t, "starting runtime attempt=%d admin=http://%s s3=http://%s", attempt, runtime.AdminAddr, runtime.S3Addr)
		if err := runtime.Start(t.Context(), privateKey); err == nil {
			logStep(t, "runtime ready admin=http://%s s3=http://%s", runtime.AdminAddr, runtime.S3Addr)
			return runtime
		} else if attempt < 3 && runtime.RetryableStartupFailure(err) {
			logStep(t, "runtime bind failed; retrying with fresh ports")
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = runtime.Close(stopCtx)
			cancel()
			continue
		} else {
			t.Fatalf("start calibration runtime: %s\n%s", runtime.sanitizeDiagnostics(err.Error()), runtime.Diagnostics())
		}
	}
	t.Fatal("start calibration runtime: exhausted bind retries")
	return nil
}

func (r *calibrationRuntime) Init(ctx context.Context) error {
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stdout := e2e.NewBoundedLog(64 * 1024)
	stderr := e2e.NewBoundedLog(64 * 1024)
	cmd := exec.CommandContext(initCtx, r.BinaryPath, "init", "--dir", r.AppDir)
	cmd.Env = sanitizedBaseEnv()
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"synaps3 init failed: %w; stdout=%s stderr=%s",
			err,
			r.sanitizeDiagnostics(stdout.String()),
			r.sanitizeDiagnostics(stderr.String()),
		)
	}
	password, ok, err := config.ReadAdminInitialPasswordFile(r.AppDir)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("admin initial password file is missing")
	}
	r.AdminPassword = password
	r.ConfigPath = filepath.Join(r.AppDir, "config.toml")
	return nil
}

func (r *calibrationRuntime) PrepareConfig() error {
	r.AdminAddr = ""
	r.S3Addr = ""
	adminAddr, err := reserveLoopbackAddress()
	if err != nil {
		return fmt.Errorf("reserving admin loopback address: %w: %v", errLoopbackAddressReservation, err)
	}
	s3Addr, err := reserveLoopbackAddress()
	if err != nil {
		return fmt.Errorf("reserving s3 loopback address: %w: %v", errLoopbackAddressReservation, err)
	}
	cfg, err := config.LoadFile(r.ConfigPath)
	if err != nil {
		return fmt.Errorf("loading generated config: %w", err)
	}
	rpcURL, ok := config.DefaultFilecoinRPCURL(integrationNetwork)
	if !ok {
		return fmt.Errorf("default RPC URL for %s is missing", integrationNetwork)
	}
	cfg.Server.Port = s3Addr
	cfg.Server.MaxConnections = 128
	cfg.Server.MaxRequests = 64
	cfg.Admin.Addr = adminAddr
	cfg.Filecoin.Network = integrationNetwork
	cfg.Filecoin.RPCURL = rpcURL
	cfg.Filecoin.PrivateKey = ""
	cfg.Filecoin.WithCDN = false
	cfg.Filecoin.AllowPrivateNetworks = true
	cfg.Filecoin.DefaultCopies = integrationCopies
	cfg.Filecoin.Observability.Interval = 15 * time.Second
	cfg.Filecoin.Observability.Timeout = 10 * time.Second
	cfg.Filecoin.Observability.Concurrency = 4
	cfg.Cache.MaxSizeGB = 1
	cfg.Cache.EvictionPolicy = "lru"
	cfg.Worker.Upload = config.WorkerPoolConfig{Concurrency: 1, PollInterval: 5 * time.Second, MaxRetries: 3}
	cfg.Worker.Evictor = config.WorkerPoolConfig{Concurrency: 1, PollInterval: 5 * time.Second, MaxRetries: 3}
	cfg.Worker.StorageCleanup = config.WorkerPoolConfig{Concurrency: 1, PollInterval: 30 * time.Second, MaxRetries: 3}
	cfg.Logging.Level = "warn"
	cfg.Logging.Format = "text"
	cfg.Logging.S3Access.Enabled = false
	if err := config.Save(r.ConfigPath, cfg); err != nil {
		return fmt.Errorf("saving integration config: %w", err)
	}
	r.AdminAddr = adminAddr
	r.S3Addr = s3Addr
	return nil
}

func (r *calibrationRuntime) Start(ctx context.Context, privateKey string) error {
	r.stdout = e2e.NewBoundedLog(128 * 1024)
	r.stderr = e2e.NewBoundedLog(128 * 1024)
	cmd := exec.Command(r.BinaryPath, "--config", r.ConfigPath, "serve")
	cmd.Env = append(sanitizedBaseEnv(), "SYNAPS3_FILECOIN_PRIVATE_KEY="+privateKey)
	cmd.Stdout = r.stdout
	cmd.Stderr = r.stderr
	process, err := startCalibrationProcess(cmd)
	if err != nil {
		return err
	}
	r.process = process
	if err := r.WaitReady(ctx, 90*time.Second); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = r.Close(closeCtx)
		cancel()
		return err
	}
	return nil
}

func startCalibrationProcess(cmd *exec.Cmd) (*calibrationProcess, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	process := &calibrationProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		process.err = cmd.Wait()
		close(process.done)
	}()
	return process, nil
}

func (r *calibrationRuntime) WaitReady(ctx context.Context, timeout time.Duration) error {
	process := r.process
	if process == nil {
		return fmt.Errorf("runtime process is not started")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var last string
	var lastErr error
	for {
		select {
		case <-process.done:
			return runtimeExitError("runtime exited before readiness", process.err, last)
		default:
		}

		reqCtx, reqCancel := context.WithTimeout(waitCtx, 10*time.Second)
		health, raw, err := r.Admin.Health(reqCtx)
		reqCancel()
		last = raw
		lastErr = err
		if err == nil && health.Status == "ok" {
			authCtx, authCancel := context.WithTimeout(waitCtx, 10*time.Second)
			err = r.Admin.Authenticate(authCtx, adminUsername, r.AdminPassword)
			authCancel()
			lastErr = err
			if err == nil {
				settingsCtx, settingsCancel := context.WithTimeout(waitCtx, 10*time.Second)
				settings, settingsRaw, settingsErr := r.Admin.Settings(settingsCtx)
				settingsCancel()
				last = settingsRaw
				lastErr = settingsErr
				if settingsErr == nil && settings.Mode == "ready" && settings.RuntimeAvailable {
					return nil
				}
			}
		}

		select {
		case <-process.done:
			return runtimeExitError("runtime exited before readiness", process.err, last)
		case <-waitCtx.Done():
			return fmt.Errorf("waiting for runtime readiness: %w; last=%s; last_error=%v", waitCtx.Err(), e2e.Redact(last), lastErr)
		case <-ticker.C:
		}
	}
}

func runtimeExitError(message string, err error, last string) error {
	last = e2e.Redact(last)
	suffix := ""
	if last != "" {
		suffix = "; last=" + last
	}
	if err == nil {
		return errors.New(message + suffix)
	}
	return fmt.Errorf("%s: %w%s", message, err, suffix)
}

func (r *calibrationRuntime) BindFailure(err error) bool {
	text := strings.ToLower(err.Error() + "\n" + r.Diagnostics())
	return strings.Contains(text, "address already in use") ||
		strings.Contains(text, "bind:") ||
		strings.Contains(text, "listen tcp")
}

func (r *calibrationRuntime) RetryableStartupFailure(err error) bool {
	return errors.Is(err, errLoopbackAddressReservation) || r.BindFailure(err)
}

func (r *calibrationRuntime) Close(ctx context.Context) error {
	if r == nil || r.process == nil || r.process.cmd == nil || r.process.cmd.Process == nil {
		return nil
	}
	process := r.process
	select {
	case <-process.done:
		r.process = nil
		return runtimeExitError("runtime exited before shutdown", process.err, "")
	default:
	}
	signalErr := process.cmd.Process.Signal(syscall.SIGTERM)
	if signalErr != nil && !errors.Is(signalErr, os.ErrProcessDone) {
		return fmt.Errorf("send SIGTERM: %w", signalErr)
	}
	select {
	case <-process.done:
		r.process = nil
		if signalErr != nil {
			return runtimeExitError("runtime exited before SIGTERM", process.err, "")
		}
		if process.err != nil {
			return fmt.Errorf("runtime exited with error: %w", process.err)
		}
		return nil
	case <-ctx.Done():
		killErr := process.cmd.Process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
		select {
		case <-process.done:
			r.process = nil
		default:
			if killErr == nil {
				r.process = nil
			}
		}
		return errors.Join(fmt.Errorf("waiting for runtime shutdown: %w", ctx.Err()), killErr)
	}
}

func TestCalibrationRuntimeCloseReportsEarlyExit(t *testing.T) {
	process, err := startCalibrationProcess(exec.Command("/bin/sh", "-c", "exit 7"))
	if err != nil {
		t.Fatalf("start early-exit process: %v", err)
	}
	<-process.done
	runtime := &calibrationRuntime{process: process}
	err = runtime.Close(t.Context())
	if err == nil || !strings.Contains(err.Error(), "runtime exited before shutdown") {
		t.Fatalf("Close error = %v, want early exit", err)
	}
}

func TestCalibrationRuntimeDiagnosticsRedactsLocalPaths(t *testing.T) {
	appDir := t.TempDir()
	stdout := e2e.NewBoundedLog(1024)
	stderr := e2e.NewBoundedLog(1024)
	_, _ = stdout.Write([]byte("config=" + filepath.Join(appDir, "config.toml")))
	_, _ = stderr.Write([]byte("db=" + filepath.Join(appDir, "synaps3.db") + " socket=" + filepath.Join(appDir, "s3.sock")))
	runtime := &calibrationRuntime{
		AppDir:     appDir,
		ConfigPath: filepath.Join(appDir, "config.toml"),
		stdout:     stdout,
		stderr:     stderr,
	}

	diagnostics := runtime.Diagnostics()
	if strings.Contains(diagnostics, appDir) {
		t.Fatalf("Diagnostics leaked app directory: %s", diagnostics)
	}
	if !strings.Contains(diagnostics, "[REDACTED_PATH]") {
		t.Fatalf("Diagnostics did not include a path redaction marker: %s", diagnostics)
	}
}

func TestCalibrationRuntimeTreatsLoopbackReservationAsRetryableStartupFailure(t *testing.T) {
	runtime := &calibrationRuntime{}
	err := fmt.Errorf("reserving admin loopback address: %w: listen tcp 127.0.0.1:0: address already in use", errLoopbackAddressReservation)

	if !runtime.RetryableStartupFailure(err) {
		t.Fatalf("RetryableStartupFailure(%v) = false, want true", err)
	}
}

func TestCalibrationWalletActionsAllowSingleFundAttempt(t *testing.T) {
	actions := new(calibrationWalletActions)
	if err := actions.beginFund(); err != nil {
		t.Fatalf("first fund attempt: %v", err)
	}
	if err := actions.beginFund(); err == nil {
		t.Fatal("second fund attempt succeeded")
	}
}

func TestCalibrationRuntimeCloseReturnsAfterDeadline(t *testing.T) {
	readyPath := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCalibrationRuntimeCloseHelperProcess$")
	cmd.Env = append(os.Environ(), "SYNAPS3_CALIBRATION_CLOSE_HELPER=1", "SYNAPS3_CALIBRATION_CLOSE_HELPER_READY="+readyPath)
	process, err := startCalibrationProcess(cmd)
	if err != nil {
		t.Fatalf("start stubborn process: %v", err)
	}
	defer func() {
		_ = process.cmd.Process.Kill()
		select {
		case <-process.done:
		case <-time.After(3 * time.Second):
			t.Fatal("stubborn process did not exit after kill")
		}
	}()
	waitForCloseHelperReady(t, readyPath)

	runtime := &calibrationRuntime{process: process}
	closeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = runtime.Close(closeCtx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want context deadline exceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Close elapsed = %s, want bounded by caller deadline", elapsed)
	}
}

func waitForCloseHelperReady(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("close helper did not become ready")
}

func TestCalibrationRuntimeCloseHelperProcess(t *testing.T) {
	if os.Getenv("SYNAPS3_CALIBRATION_CLOSE_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if readyPath := os.Getenv("SYNAPS3_CALIBRATION_CLOSE_HELPER_READY"); readyPath != "" {
		if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
			t.Fatalf("write ready file: %v", err)
		}
	}
	select {}
}

func (r *calibrationRuntime) Diagnostics() string {
	if r == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("calibration runtime diagnostics\n")
	fmt.Fprintf(&b, "admin_url: http://%s\n", r.AdminAddr)
	fmt.Fprintf(&b, "s3_url: http://%s\n", r.S3Addr)
	appendDiagnosticSection(&b, "stdout", boundedLogString(r.stdout))
	appendDiagnosticSection(&b, "stderr", boundedLogString(r.stderr))
	return r.sanitizeDiagnostics(b.String())
}

func (r *calibrationRuntime) sanitizeDiagnostics(value string) string {
	if r == nil {
		return e2e.Redact(value)
	}
	for _, path := range []string{r.ConfigPath, r.AppDir} {
		if path != "" {
			value = strings.ReplaceAll(value, path, "[REDACTED_PATH]")
		}
	}
	return e2e.Redact(value)
}

func boundedLogString(log *e2e.BoundedLog) string {
	if log == nil {
		return ""
	}
	return log.String()
}

func appendDiagnosticSection(b *strings.Builder, label, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		fmt.Fprintf(b, "%s: <empty>\n", label)
		return
	}
	fmt.Fprintf(b, "%s:\n%s\n", label, indentLines(value, "  "))
}

func ensureCalibrationReadiness(t *testing.T, ctx context.Context, admin *e2e.AdminClient, actions *calibrationWalletActions) e2e.ReadinessResult {
	t.Helper()
	var lastReadiness e2e.ReadinessResult
	var lastWallet e2e.WalletResponse
	for step := 0; step < 3; step++ {
		readiness := getReadiness(t, ctx, admin)
		wallet := getWallet(t, ctx, admin)
		lastReadiness, lastWallet = readiness, wallet
		walletBalance := ""
		if wallet.WalletBalances != nil && wallet.WalletBalances.USDFC != nil {
			walletBalance = *wallet.WalletBalances.USDFC
		}
		plan, err := e2e.PlanReadinessAction(e2e.ReadinessPlanState{
			Readiness:        readiness,
			USDFCWallet:      walletBalance,
			ApproveAttempted: actions.ApproveAttempted,
			FundAttempted:    actions.FundAttempted,
			DepositCap:       e2e.DepositCapUSDFCBaseUnits,
			FundingBuffer:    e2e.FundingBufferBaseUnits,
		})
		logStep(t, "readiness step=%d status=%s checks=%s wallet_usdfc=%s", step+1, readiness.Status, readinessChecksSummary(readiness), walletBalance)
		if err != nil {
			t.Fatalf("calibration readiness is blocked: %v\nreadiness=%s\nwallet=%s", err, e2e.DiagnosticValue(readiness), e2e.DiagnosticValue(wallet))
		}
		switch plan.Action {
		case e2e.ReadinessActionNone:
			warnings := e2e.NonCriticalWarnings(readiness)
			if len(warnings) > 0 {
				t.Logf("calibration readiness has non-critical warnings: %s", e2e.DiagnosticValue(warnings))
			}
			return readiness
		case e2e.ReadinessActionApprove:
			if actions.ApproveAttempted {
				t.Fatalf("approve was already attempted\nreadiness=%s", e2e.DiagnosticValue(readiness))
			}
			logStep(t, "readiness requires FWSS approval")
			operation := createWalletOperation(t, ctx, admin, "approve", "")
			waitForWalletOperation(t, ctx, admin, operation)
			actions.ApproveAttempted = true
		case e2e.ReadinessActionFund:
			logStep(t, "readiness requires payment funding amount=%s", plan.Amount)
			fundWallet(t, ctx, admin, actions, plan.Amount)
		default:
			t.Fatalf("unsupported readiness action %q", plan.Action)
		}
	}
	t.Fatalf("calibration readiness did not converge after wallet actions\nreadiness=%s\nwallet=%s", e2e.DiagnosticValue(lastReadiness), e2e.DiagnosticValue(lastWallet))
	return e2e.ReadinessResult{}
}

func getReadiness(t *testing.T, ctx context.Context, admin *e2e.AdminClient) e2e.ReadinessResult {
	t.Helper()
	var readiness e2e.ReadinessResult
	raw, err := admin.GetJSON(ctx, "/api/v1/filecoin/readiness", &readiness)
	if err != nil {
		t.Fatalf("GET readiness: %v; body=%s", err, e2e.Redact(raw))
	}
	return readiness
}

func getWallet(t *testing.T, ctx context.Context, admin *e2e.AdminClient) e2e.WalletResponse {
	t.Helper()
	var wallet e2e.WalletResponse
	raw, err := admin.GetJSON(ctx, "/api/v1/wallet", &wallet)
	if err != nil {
		t.Fatalf("GET wallet: %v; body=%s", err, e2e.Redact(raw))
	}
	return wallet
}

func preflightCalibrationProviders(t *testing.T, ctx context.Context, admin *e2e.AdminClient) {
	t.Helper()
	logStep(t, "refreshing provider health before upload")
	var providers e2e.ProviderObservationPage
	raw, err := admin.DoJSON(ctx, http.MethodPost, "/api/v1/observability/providers/refresh?limit=100", nil, &providers)
	if err != nil {
		logStep(t, "provider health preflight failed; continuing err=%v body=%s", err, e2e.Redact(raw))
		return
	}
	logStep(t, "provider health preflight:\n%s", providerPageSummary(providers))
	if providers.Summary.Available < integrationCopies {
		logStep(t, "provider health preflight observed %d available providers; SDK selection may skip unavailable providers during upload", providers.Summary.Available)
	}
}

func createWalletOperation(t *testing.T, ctx context.Context, admin *e2e.AdminClient, opType, amount string) e2e.WalletOperation {
	t.Helper()
	payload := map[string]string{
		"client_request_id": fmt.Sprintf("calibration-%s-%d", opType, time.Now().UnixNano()),
	}
	if opType == "fund" {
		payload["amount"] = amount
	}
	var response e2e.WalletOperationResponse
	raw, err := admin.DoJSON(ctx, http.MethodPost, "/api/v1/wallet/"+opType, payload, &response)
	if err != nil {
		t.Fatalf("POST wallet %s: %v; body=%s", opType, err, e2e.Redact(raw))
	}
	logStep(t, "created wallet operation id=%d type=%s amount=%s", response.Operation.ID, response.Operation.Type, response.Operation.Amount)
	return response.Operation
}

func waitForWalletOperation(t *testing.T, ctx context.Context, admin *e2e.AdminClient, created e2e.WalletOperation) e2e.WalletOperation {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var last string
	progress := newProgressLog()
	for {
		var response e2e.WalletOperationsResponse
		raw, err := admin.GetJSON(waitCtx, "/api/v1/wallet/operations?limit=50", &response)
		last = raw
		if err == nil {
			for _, operation := range response.Operations {
				if operation.ID != created.ID {
					continue
				}
				progress.Changed(t, "wallet operation", walletOperationSummary(operation))
				switch operation.Status {
				case "confirmed":
					return operation
				case "failed":
					t.Fatalf("wallet operation %d failed: %s\noperation=%s", operation.ID, nullableString(operation.LastError), e2e.DiagnosticValue(operation))
				case "unknown":
					t.Fatalf("wallet operation %d became unknown: %s\noperation=%s", operation.ID, nullableString(operation.LastError), e2e.DiagnosticValue(operation))
				}
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("timed out waiting for wallet operation %d confirmation: %v; last=%s", created.ID, waitCtx.Err(), e2e.Redact(last))
		case <-ticker.C:
		}
	}
}

type storedObject struct {
	VersionID string
	Snapshot  string
}

func waitForStoredObject(t *testing.T, admin *e2e.AdminClient, bucket, key string, actions *calibrationWalletActions) storedObject {
	t.Helper()
	progress := newProgressLog()
	taskProgress := newProgressLog()
	return e2e.Eventually(t, t.Context(), 30*time.Minute, "Calibration upload to complete", func(ctx context.Context) (storedObject, bool, error) {
		var list e2e.ObjectListResponse
		raw, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list)
		if err != nil {
			return storedObject{Snapshot: raw}, false, err
		}
		progress.Changed(t, "object", objectListSummary(list))
		if len(list.Objects) != 1 {
			return storedObject{Snapshot: raw}, false, nil
		}
		item := list.Objects[0]
		if item.Status == "warning" || item.State == "failed" || item.UploadStatus == "failed" || item.UploadStatus == "rejected" {
			return storedObject{VersionID: item.CurrentVersionID, Snapshot: raw}, false, fmt.Errorf("object entered failed state: %s", e2e.DiagnosticValue(item))
		}
		resolveUploadDependency(t, ctx, admin, item.CurrentVersionID, actions, taskProgress)
		return storedObject{VersionID: item.CurrentVersionID, Snapshot: raw}, item.CurrentVersionID != "" && item.UploadStatus == "complete" && item.Location.Filecoin, nil
	}, e2e.WithPollInterval(5*time.Second))
}

func resolveUploadDependency(t *testing.T, ctx context.Context, admin *e2e.AdminClient, versionID string, actions *calibrationWalletActions, progress *progressLog) {
	t.Helper()
	if versionID == "" {
		return
	}
	var tasks e2e.TaskListResponse
	raw, err := admin.GetJSON(ctx, "/api/v1/tasks?type=upload&limit=100", &tasks)
	if err != nil {
		t.Fatalf("GET upload tasks: %v; body=%s", err, e2e.Redact(raw))
	}
	if progress != nil {
		progress.Changed(t, "upload tasks", uploadTaskSummary(tasks, versionID))
	}
	for _, task := range tasks.Tasks {
		if task.RefVersionID != versionID || task.Status != "waiting" || task.WaitReason == nil || *task.WaitReason != "dependency" {
			continue
		}
		message := nullableString(task.StatusMessage)
		logStep(t, "upload dependency task id=%d message=%s", task.ID, message)
		if strings.Contains(message, "approve FWSS spending") {
			if actions.ApproveAttempted {
				t.Fatalf("upload still needs FWSS approval after approve action: %s", e2e.DiagnosticValue(task))
			}
			logStep(t, "upload dependency requires FWSS approval")
			operation := createWalletOperation(t, ctx, admin, "approve", "")
			waitForWalletOperation(t, ctx, admin, operation)
			actions.ApproveAttempted = true
			return
		}
		if match := uploadDepositPattern.FindStringSubmatch(message); len(match) == 2 {
			if actions.FundAttempted {
				return
			}
			amount, err := e2e.BufferedFundingAmount(match[1], e2e.DepositCapUSDFCBaseUnits, e2e.FundingBufferBaseUnits)
			if err != nil {
				t.Fatalf("invalid upload funding dependency %q: %v", message, err)
			}
			logStep(t, "upload dependency requires payment funding amount=%s", amount)
			fundWallet(t, ctx, admin, actions, amount)
			return
		}
	}
}

func fundWallet(t *testing.T, ctx context.Context, admin *e2e.AdminClient, actions *calibrationWalletActions, amount string) {
	t.Helper()
	amountInt, ok := new(big.Int).SetString(amount, 10)
	if !ok || amountInt.Sign() <= 0 {
		t.Fatalf("invalid fund amount %q", amount)
	}
	capAmount, ok := new(big.Int).SetString(e2e.DepositCapUSDFCBaseUnits, 10)
	if !ok || capAmount.Sign() <= 0 {
		t.Fatalf("invalid deposit cap %q", e2e.DepositCapUSDFCBaseUnits)
	}
	if amountInt.Cmp(capAmount) > 0 {
		t.Fatalf("planned funding amount %s exceeds cap %s", amountInt.String(), capAmount.String())
	}
	wallet := getWallet(t, ctx, admin)
	walletBalance := ""
	if wallet.WalletBalances != nil && wallet.WalletBalances.USDFC != nil {
		walletBalance = *wallet.WalletBalances.USDFC
	}
	walletAmount, ok := new(big.Int).SetString(walletBalance, 10)
	if !ok || walletAmount.Cmp(amountInt) < 0 {
		t.Fatalf("USDFC wallet balance %q is less than planned funding %s", walletBalance, amountInt.String())
	}
	if err := actions.beginFund(); err != nil {
		t.Fatal(err)
	}
	logStep(t, "funding payment account amount=%s", amountInt.String())
	operation := createWalletOperation(t, ctx, admin, "fund", amountInt.String())
	waitForWalletOperation(t, ctx, admin, operation)
}

func waitForCommittedCopies(t *testing.T, admin *e2e.AdminClient, bucket, versionID string) e2e.ProvenanceResponse {
	t.Helper()
	path := "/api/v1/buckets/" + bucket + "/objects/provenance?version_id=" + url.QueryEscape(versionID)
	progress := newProgressLog()
	return e2e.Eventually(t, t.Context(), 20*time.Minute, "three committed readable Calibration copies", func(ctx context.Context) (e2e.ProvenanceResponse, bool, error) {
		var provenance e2e.ProvenanceResponse
		_, err := admin.GetJSON(ctx, path, &provenance)
		if err != nil {
			return provenance, false, err
		}
		progress.Changed(t, "provenance", provenanceSummary(provenance))
		if provenance.UploadStatus == "failed" || provenance.UploadStatus == "rejected" {
			return provenance, false, fmt.Errorf("upload failed: %s", e2e.DiagnosticValue(provenance))
		}
		if provenance.RequestedCopies != integrationCopies || provenance.SuccessCopies != integrationCopies || len(provenance.Copies) != integrationCopies {
			return provenance, false, nil
		}
		for _, copy := range provenance.Copies {
			if copy.Status != "committed" || copy.ProviderID == "" || copy.DataSetID == "" || copy.PieceID == "" || copy.RetrievalURL == "" {
				return provenance, false, nil
			}
		}
		return provenance, true, nil
	}, e2e.WithPollInterval(5*time.Second))
}

func assertDataSetMetadata(t *testing.T, ctx context.Context, privateKey, bucket string, provenance e2e.ProvenanceResponse) {
	t.Helper()
	rpcURL, ok := config.DefaultFilecoinRPCURL(integrationNetwork)
	if !ok {
		t.Fatalf("default RPC URL for %s is missing", integrationNetwork)
	}
	client, err := synapse.NewClient(ctx, synapse.ClientConfig{
		PrivateKey: privateKey,
		RPCURL:     rpcURL,
	})
	if err != nil {
		t.Fatalf("create metadata inspection client: %v", err)
	}
	defer func() { _ = client.Close() }()

	logStep(t, "verifying fixed dataset metadata for %d copies", len(provenance.Copies))
	for _, copy := range provenance.Copies {
		dataSetID, err := sdktypes.ParseBigInt(copy.DataSetID)
		if err != nil {
			t.Fatalf("parse dataset id %q: %v", copy.DataSetID, err)
		}
		queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
		metadata, err := client.WarmStorage().GetAllDataSetMetadata(queryCtx, dataSetID)
		cancel()
		if err != nil {
			t.Fatalf("read dataset %s metadata: %v", copy.DataSetID, err)
		}
		if metadata["source"] != "synaps3" || metadata["bucket"] != bucket || len(metadata) != 2 {
			t.Fatalf("dataset %s metadata = %#v, want source=synaps3 and bucket=%s", copy.DataSetID, metadata, bucket)
		}
	}
}

func waitForCompletedUploadTasks(t *testing.T, admin *e2e.AdminClient, versionID string) {
	t.Helper()
	progress := newProgressLog()
	lastSummary := "none"
	e2e.Eventually(t, t.Context(), uploadTaskTimeout, "all upload tasks completed without failures", func(ctx context.Context) (string, bool, error) {
		var tasks e2e.TaskListResponse
		_, err := admin.GetJSON(ctx, "/api/v1/tasks?type=upload&limit=100", &tasks)
		if err != nil {
			return lastSummary, false, err
		}
		lastSummary = uploadTaskSummary(tasks, versionID)
		progress.Changed(t, "upload tasks", lastSummary)
		seen, active, failed := 0, 0, 0
		for _, task := range tasks.Tasks {
			if task.RefVersionID != versionID {
				continue
			}
			seen++
			switch task.Status {
			case "completed":
			case "failed", "exhausted", "cancelled":
				failed++
			default:
				active++
			}
		}
		return lastSummary, seen > 0 && active == 0 && failed == 0, nil
	}, e2e.WithPollInterval(5*time.Second))
}

func waitForCacheEviction(t *testing.T, admin *e2e.AdminClient, bucket, key string) {
	t.Helper()
	progress := newProgressLog()
	e2e.Eventually(t, t.Context(), 5*time.Minute, "automatic cache eviction", func(ctx context.Context) (string, bool, error) {
		var list e2e.ObjectListResponse
		raw, err := admin.GetJSON(ctx, "/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list)
		if err != nil {
			return raw, false, err
		}
		progress.Changed(t, "cache eviction", objectListSummary(list))
		ready := len(list.Objects) == 1 && !list.Objects[0].Location.Cache && list.Objects[0].Location.Filecoin
		return raw, ready, nil
	}, e2e.WithPollInterval(5*time.Second))
}

func waitForS3Object(t *testing.T, runtime *calibrationRuntime, client *awss3.Client, bucket, key string, want []byte, checksum [sha256.Size]byte) {
	t.Helper()
	progress := newProgressLog()
	e2e.Eventually(t, t.Context(), 5*time.Minute, "cold S3 read after cache eviction", func(ctx context.Context) (string, bool, error) {
		output, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			last := fmt.Sprintf("GetObject: %v", err)
			progress.Changed(t, "cold read", last)
			return last + "\n" + runtime.Diagnostics(), false, nil
		}
		defer func() { _ = output.Body.Close() }()
		got, err := io.ReadAll(output.Body)
		if err != nil {
			last := fmt.Sprintf("read GetObject: %v", err)
			progress.Changed(t, "cold read", last)
			return last + "\n" + runtime.Diagnostics(), false, nil
		}
		gotChecksum := sha256.Sum256(got)
		if !bytes.Equal(got, want) || gotChecksum != checksum {
			return fmt.Sprintf("bytes=%d checksum=%x", len(got), gotChecksum), false, fmt.Errorf("GetObject content or checksum differs from uploaded object")
		}
		progress.Changed(t, "cold read", fmt.Sprintf("success bytes=%d sha256=%x", len(got), gotChecksum))
		return "ok", true, nil
	}, e2e.WithPollInterval(10*time.Second))
}

func waitForObservabilitySnapshot(t *testing.T, admin *e2e.AdminClient, bucket string, provenance e2e.ProvenanceResponse) {
	t.Helper()
	expectedProviders := make(map[string]struct{})
	expectedDataSets := make(map[string]struct{})
	for _, copy := range provenance.Copies {
		expectedProviders[copy.ProviderID] = struct{}{}
		expectedDataSets[copy.DataSetID] = struct{}{}
	}
	progress := newProgressLog()
	e2e.Eventually(t, t.Context(), 5*time.Minute, "observability snapshot for Calibration provider and data set inventory", func(ctx context.Context) (string, bool, error) {
		var providers e2e.ProviderObservationPage
		providerRaw, err := admin.GetJSON(ctx, "/api/v1/observability/providers?limit=50", &providers)
		if err != nil {
			return providerRaw, false, err
		}
		var dataSets e2e.DataSetObservationPage
		dataSetRaw, err := admin.GetJSON(ctx, "/api/v1/observability/data-sets?bucket="+url.QueryEscape(bucket)+"&limit=50", &dataSets)
		if err != nil {
			return providerRaw + "\n" + dataSetRaw, false, err
		}
		progress.Changed(t, "observability", observabilitySnapshotSummary(providers, dataSets))
		observedProviders := make(map[string]struct{})
		for _, provider := range providers.Items {
			observedProviders[provider.Facts.ProviderID] = struct{}{}
		}
		observedDataSets := make(map[string]struct{})
		for _, dataSet := range dataSets.Items {
			if dataSet.Facts.ChainDataSetID != nil {
				observedDataSets[*dataSet.Facts.ChainDataSetID] = struct{}{}
			}
		}
		return providerRaw + "\n" + dataSetRaw,
			containsAll(observedProviders, expectedProviders) && containsAll(observedDataSets, expectedDataSets),
			nil
	}, e2e.WithPollInterval(15*time.Second))
}

func containsAll(got, want map[string]struct{}) bool {
	for value := range want {
		if _, ok := got[value]; !ok {
			return false
		}
	}
	return true
}

func logStep(t testing.TB, format string, args ...any) {
	t.Helper()
	message := fmt.Sprintf(format, args...)
	t.Log("calibration: " + indentLines(message, "  "))
}

func indentLines(value, prefix string) string {
	value = strings.TrimRight(value, "\n")
	if value == "" {
		return ""
	}
	return strings.ReplaceAll(value, "\n", "\n"+prefix)
}

type progressLog struct {
	last map[string]string
}

func newProgressLog() *progressLog {
	return &progressLog{last: make(map[string]string)}
}

func (p *progressLog) Changed(t testing.TB, label, value string) {
	t.Helper()
	if p.last[label] == value {
		return
	}
	p.last[label] = value
	if strings.Contains(value, "\n") {
		logStep(t, "%s:\n%s", label, value)
		return
	}
	logStep(t, "%s %s", label, value)
}

func readinessChecksSummary(result e2e.ReadinessResult) string {
	parts := make([]string, 0, len(e2e.CriticalReadinessChecks)+1)
	for _, id := range e2e.CriticalReadinessChecks {
		check, ok := result.Check(id)
		if !ok {
			parts = append(parts, id+"=missing")
			continue
		}
		value := id + "=" + check.Status
		if check.RequiredUSDFC != "" {
			value += ":required_usdfc=" + check.RequiredUSDFC
		}
		if check.Action != "" {
			value += ":action=" + check.Action
		}
		parts = append(parts, value)
	}
	if len(result.PartialErrors) > 0 {
		parts = append(parts, fmt.Sprintf("partial_errors=%d", len(result.PartialErrors)))
	}
	return strings.Join(parts, " ")
}

func providerPageSummary(page e2e.ProviderObservationPage) string {
	lines := []string{fmt.Sprintf(
		"summary total=%d available=%d degraded=%d unavailable=%d unknown=%d",
		page.Summary.Total,
		page.Summary.Available,
		page.Summary.Degraded,
		page.Summary.Unavailable,
		page.Summary.Unknown,
	)}
	for _, provider := range page.Items {
		reasons := "-"
		if len(provider.Signal.ReasonCodes) > 0 {
			reasons = strings.Join(provider.Signal.ReasonCodes, ",")
		}
		lines = append(lines, fmt.Sprintf(
			"- provider=%s status=%s health=%s active=%s pdp=%s endpoint=%s reasons=%s error=%s",
			provider.Facts.ProviderID,
			provider.Signal.Status,
			nullableString(provider.Facts.HealthStatus),
			boolPointerText(provider.Facts.Active),
			boolPointerText(provider.Facts.HasPDP),
			providerEndpoint(provider.Facts.ServiceURL),
			reasons,
			shortLogValue(nullableString(provider.Signal.LastError)),
		))
	}
	return strings.Join(lines, "\n")
}

func providerEndpoint(value *string) string {
	if value == nil || *value == "" {
		return ""
	}
	parsed, err := url.Parse(*value)
	if err != nil || parsed.Host == "" {
		return *value
	}
	return parsed.Host
}

func boolPointerText(value *bool) string {
	if value == nil {
		return "unknown"
	}
	if *value {
		return "true"
	}
	return "false"
}

func walletOperationSummary(operation e2e.WalletOperation) string {
	txHash := "none"
	if operation.TxHash != nil && *operation.TxHash != "" {
		txHash = "present"
	}
	return fmt.Sprintf(
		"id=%d type=%s amount=%s status=%s tx=%s error=%s",
		operation.ID,
		operation.Type,
		operation.Amount,
		operation.Status,
		txHash,
		shortLogValue(nullableString(operation.LastError)),
	)
}

func objectListSummary(list e2e.ObjectListResponse) string {
	if len(list.Objects) == 0 {
		return "count=0"
	}
	item := list.Objects[0]
	return fmt.Sprintf(
		"count=%d version=%s state=%s status=%s upload=%s cache=%t filecoin=%t",
		len(list.Objects),
		item.CurrentVersionID,
		item.State,
		item.Status,
		item.UploadStatus,
		item.Location.Cache,
		item.Location.Filecoin,
	)
}

func provenanceSummary(provenance e2e.ProvenanceResponse) string {
	lines := []string{fmt.Sprintf(
		"status=%s upload=%s requested=%d success=%d",
		provenance.Status,
		provenance.UploadStatus,
		provenance.RequestedCopies,
		provenance.SuccessCopies,
	)}
	for index, copy := range provenance.Copies {
		lines = append(lines, fmt.Sprintf(
			"- copy=%d status=%s provider=%s dataset=%s piece=%t retrieval=%t",
			index,
			copy.Status,
			copy.ProviderID,
			copy.DataSetID,
			copy.PieceID != "",
			copy.RetrievalURL != "",
		))
	}
	return strings.Join(lines, "\n")
}

func uploadTaskSummary(tasks e2e.TaskListResponse, versionID string) string {
	lines := make([]string, 0, len(tasks.Tasks))
	for _, task := range tasks.Tasks {
		if task.RefVersionID != versionID {
			continue
		}
		lines = append(lines, fmt.Sprintf(
			"- id=%d type=%s stage=%s status=%s retry=%d claimed=%s wait=%s message=%s error=%s",
			task.ID,
			task.Type,
			nullableString(task.Stage),
			task.Status,
			task.RetryCount,
			nullableString(task.ClaimedAt),
			nullableString(task.WaitReason),
			shortLogValue(nullableString(task.StatusMessage)),
			shortLogValue(nullableString(task.LastError)),
		))
	}
	if len(lines) == 0 {
		return "none"
	}
	return strings.Join(lines, "\n")
}

func observabilitySnapshotSummary(providers e2e.ProviderObservationPage, dataSets e2e.DataSetObservationPage) string {
	lines := []string{fmt.Sprintf(
		"providers total=%d available=%d degraded=%d unavailable=%d unknown=%d",
		providers.Summary.Total,
		providers.Summary.Available,
		providers.Summary.Degraded,
		providers.Summary.Unavailable,
		providers.Summary.Unknown,
	), fmt.Sprintf(
		"data_sets total=%d available=%d degraded=%d unavailable=%d unknown=%d",
		dataSets.Summary.Total,
		dataSets.Summary.Available,
		dataSets.Summary.Degraded,
		dataSets.Summary.Unavailable,
		dataSets.Summary.Unknown,
	)}
	if len(dataSets.Items) > 0 {
		lines = append(lines, dataSetObservationDetails(dataSets.Items))
	}
	return strings.Join(lines, "\n")
}

func dataSetObservationDetails(items []e2e.DataSetObservation) string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		reasons := "-"
		if len(item.Signal.ReasonCodes) > 0 {
			reasons = strings.Join(item.Signal.ReasonCodes, ",")
		}
		lines = append(lines, fmt.Sprintf(
			"- provider=%s dataset=%s status=%s reasons=%s error=%s",
			item.Facts.ProviderID,
			nullableString(item.Facts.ChainDataSetID),
			item.Signal.Status,
			reasons,
			shortLogValue(nullableString(item.Signal.LastError)),
		))
	}
	return strings.Join(lines, "\n")
}

// reserveLoopbackAddress closes the listener before the runtime binds it.
// Callers must retry bind failures caused by this unavoidable TOCTOU window.
func reserveLoopbackAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func sanitizedBaseEnv() []string {
	env := make([]string, 0, len(os.Environ()))
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(name, "SYNAPS3_") {
			continue
		}
		env = append(env, item)
	}
	return env
}

func nullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func shortLogValue(value string) string {
	value = e2e.Redact(strings.Join(strings.Fields(value), " "))
	const maxRunes = 360
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes]) + "...[truncated]"
}

func uniqueBucketName() string {
	return fmt.Sprintf("calibration-e2e-%d", time.Now().UnixNano())
}
