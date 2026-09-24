//go:build systemtest && s3compat

package system_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

func TestS3Clients(t *testing.T) {
	for _, name := range []string{"aws", "rclone", "mc"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Fatalf("required S3 client %s is unavailable: %v", name, err)
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	harness, err := systemtest.NewHarness(t.Context(), logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	endpoint := startS3LoopbackProxy(t, harness.S3SocketPath())
	client := e2e.NewUnixSocketS3Client(harness.S3SocketPath(), systemtest.OwnerAccess, systemtest.OwnerSecret)
	bucket := aws.String("s3-clients")
	if _, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
	e2e.Eventually(t, t.Context(), 10*time.Second, "bucket provisioning", func(ctx context.Context) (*s3.PutObjectOutput, bool, error) {
		out, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("readiness.bin"), Body: bytes.NewReader(bytes.Repeat([]byte("ready"), 26))})
		return out, err == nil, err
	})

	temp := t.TempDir()
	payload := bytes.Repeat([]byte("SynapS3 offline CLI compatibility\n"), 8192)
	input := filepath.Join(temp, "input.bin")
	if err := os.WriteFile(input, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(payload)
	configFile := filepath.Join(temp, "aws-config")
	if err := os.WriteFile(configFile, []byte("[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	commonEnv := []string{
		"HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY=",
		"http_proxy=", "https_proxy=", "all_proxy=",
		"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
		"AWS_ACCESS_KEY_ID=" + systemtest.OwnerAccess,
		"AWS_SECRET_ACCESS_KEY=" + systemtest.OwnerSecret,
		"AWS_DEFAULT_REGION=us-east-1",
		"AWS_PROFILE=default",
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_MAX_ATTEMPTS=2",
		"AWS_CONFIG_FILE=" + configFile,
		"AWS_SHARED_CREDENTIALS_FILE=" + filepath.Join(temp, "no-credentials"),
		"RCLONE_CONFIG=" + filepath.Join(temp, "no-rclone-config"),
		"RCLONE_CONFIG_MATRIX_TYPE=s3",
		"RCLONE_CONFIG_MATRIX_PROVIDER=Other",
		"RCLONE_CONFIG_MATRIX_ACCESS_KEY_ID=" + systemtest.OwnerAccess,
		"RCLONE_CONFIG_MATRIX_SECRET_ACCESS_KEY=" + systemtest.OwnerSecret,
		"RCLONE_CONFIG_MATRIX_ENDPOINT=" + endpoint,
		"RCLONE_CONFIG_MATRIX_REGION=us-east-1",
		"RCLONE_CONFIG_MATRIX_FORCE_PATH_STYLE=true",
		"MC_CONFIG_DIR=" + filepath.Join(temp, "mc"),
		"MC_HOST_matrix=" + strings.Replace(endpoint, "http://", "http://"+systemtest.OwnerAccess+":"+systemtest.OwnerSecret+"@", 1),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name string
		put  []string
		get  []string
	}{
		{
			"AWS_CLI",
			[]string{"aws", "--endpoint-url", endpoint, "s3", "cp", "--only-show-errors", input, "s3://s3-clients/aws.bin"},
			[]string{"aws", "--endpoint-url", endpoint, "s3", "cp", "--only-show-errors", "s3://s3-clients/aws.bin"},
		},
		{
			"rclone",
			[]string{"rclone", "copyto", input, "matrix:s3-clients/rclone.bin", "--retries", "1", "--low-level-retries", "1"},
			[]string{"rclone", "copyto", "matrix:s3-clients/rclone.bin", "--retries", "1", "--low-level-retries", "1"},
		},
		{
			"mc",
			[]string{"mc", "--quiet", "cp", input, "matrix/s3-clients/mc.bin"},
			[]string{"mc", "--quiet", "cp", "matrix/s3-clients/mc.bin"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := runS3Client(ctx, commonEnv, tc.put...); err != nil {
				t.Fatalf("upload: %v", err)
			}
			output := filepath.Join(temp, strings.ReplaceAll(tc.name, " ", "-")+"-output.bin")
			if err := runS3Client(ctx, commonEnv, append(tc.get, output)...); err != nil {
				t.Fatalf("download: %v", err)
			}
			got, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if digest := sha256.Sum256(got); digest != want {
				t.Fatalf("SHA256 mismatch: got %x, want %x", digest, want)
			}
		})
	}
}

func runS3Client(ctx context.Context, extraEnv []string, args ...string) error {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = append(os.Environ(), extraEnv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.ReplaceAll(string(output), systemtest.OwnerSecret, "[redacted]")
		message = strings.ReplaceAll(message, systemtest.OwnerAccess, "[redacted]")
		return fmt.Errorf("%s failed: %w: %s", args[0], err, message)
	}
	return nil
}

func startS3LoopbackProxy(t *testing.T, socket string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = incoming.Close() }()
				upstream, err := net.Dial("unix", socket)
				if err != nil {
					return
				}
				defer func() { _ = upstream.Close() }()
				copyDone := make(chan struct{})
				go func() {
					defer close(copyDone)
					_, _ = io.Copy(upstream, incoming)
					if unix, ok := upstream.(*net.UnixConn); ok {
						_ = unix.CloseWrite()
					}
				}()
				_, _ = io.Copy(incoming, upstream)
				<-copyDone
			}()
		}
	}()
	return "http://" + listener.Addr().String()
}
