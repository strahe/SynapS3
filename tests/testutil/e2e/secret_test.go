package e2e

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRepoRootFromFindsGoMod(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.test\n"), 0o600); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	nested := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	got, err := RepoRootFrom(nested)
	if err != nil {
		t.Fatalf("RepoRootFrom: %v", err)
	}
	if got != root {
		t.Fatalf("root = %q, want %q", got, root)
	}
}

func TestReadDotEnvFileParsesWithoutExecutingShell(t *testing.T) {
	path := filepath.Join(t.TempDir(), TestEnvFileName)
	data := strings.Join([]string{
		"# comment",
		"export SYNAPS3_CALIBRATION_PRIVATE_KEY='0x" + strings.Repeat("a", 64) + "' # don't execute comments",
		"IGNORED=$(echo should-not-run)",
		`QUOTED="value # not comment"`,
		"UNQUOTED=value # comment",
	}, "\n")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}
	values, err := ReadDotEnvFile(path)
	if err != nil {
		t.Fatalf("ReadDotEnvFile: %v", err)
	}
	if got := values[CalibrationPrivateKeyEnv]; got != "0x"+strings.Repeat("a", 64) {
		t.Fatalf("private key parse failed: %q", got)
	}
	if got := values["IGNORED"]; got != "$(echo should-not-run)" {
		t.Fatalf("shell content was not preserved literally: %q", got)
	}
	if values["QUOTED"] != "value # not comment" || values["UNQUOTED"] != "value" {
		t.Fatalf("quoted/unquoted parse mismatch: %#v", values)
	}
}

func TestLoadCalibrationPrivateKeyValidation(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadCalibrationPrivateKey(t.TempDir()); err == nil {
			t.Fatal("LoadCalibrationPrivateKey succeeded without .env.test")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, TestEnvFileName), []byte("OTHER=value\n"), 0o600); err != nil {
			t.Fatalf("write env: %v", err)
		}
		if _, err := LoadCalibrationPrivateKey(root); err == nil {
			t.Fatal("LoadCalibrationPrivateKey succeeded without key")
		}
	})

	t.Run("invalid key", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, TestEnvFileName), []byte(CalibrationPrivateKeyEnv+"=not-a-key\n"), 0o600); err != nil {
			t.Fatalf("write env: %v", err)
		}
		if _, err := LoadCalibrationPrivateKey(root); err == nil {
			t.Fatal("LoadCalibrationPrivateKey succeeded with invalid key")
		}
	})

	t.Run("unsafe permissions", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Unix permission bits are not portable on Windows")
		}
		root := t.TempDir()
		path := filepath.Join(root, TestEnvFileName)
		if err := os.WriteFile(path, []byte(CalibrationPrivateKeyEnv+"=0x"+strings.Repeat("b", 64)+"\n"), 0o644); err != nil {
			t.Fatalf("write env: %v", err)
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("chmod env: %v", err)
		}
		if _, err := LoadCalibrationPrivateKey(root); err == nil {
			t.Fatal("LoadCalibrationPrivateKey succeeded with unsafe permissions")
		}
	})

	t.Run("valid", func(t *testing.T) {
		root := t.TempDir()
		want := "0x" + strings.Repeat("c", 64)
		if err := os.WriteFile(filepath.Join(root, TestEnvFileName), []byte(CalibrationPrivateKeyEnv+"="+want+"\n"), 0o600); err != nil {
			t.Fatalf("write env: %v", err)
		}
		got, err := LoadCalibrationPrivateKey(root)
		if err != nil {
			t.Fatalf("LoadCalibrationPrivateKey: %v", err)
		}
		if got != want {
			t.Fatalf("key = %q, want %q", got, want)
		}
	})
}
