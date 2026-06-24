package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

const (
	CalibrationPrivateKeyEnv = "SYNAPS3_CALIBRATION_PRIVATE_KEY"
	TestEnvFileName          = ".env.test"
)

var privateKeyPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

func RepoRootFrom(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolving start directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("checking go.mod in %s: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not locate repository root from %s", start)
		}
		dir = parent
	}
}

func LoadCalibrationPrivateKey(repoRoot string) (string, error) {
	path := filepath.Join(repoRoot, TestEnvFileName)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%s is required; add %s=0x... and chmod 600 %s", TestEnvFileName, CalibrationPrivateKeyEnv, TestEnvFileName)
		}
		return "", fmt.Errorf("checking %s: %w", TestEnvFileName, err)
	}
	values, err := ReadDotEnvFile(path)
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(values[CalibrationPrivateKeyEnv])
	if key == "" {
		return "", fmt.Errorf("%s is missing from %s", CalibrationPrivateKeyEnv, TestEnvFileName)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s contains %s and must not be group/world readable; run chmod 600 %s", TestEnvFileName, CalibrationPrivateKeyEnv, TestEnvFileName)
	}
	if !privateKeyPattern.MatchString(key) {
		return "", fmt.Errorf("%s must be 0x followed by 64 hex characters; got %d characters", CalibrationPrivateKeyEnv, len(key))
	}
	return key, nil
}

func ReadDotEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = file.Close() }()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		key, value, ok, err := parseDotEnvLine(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", filepath.Base(path), lineNo, err)
		}
		if ok {
			values[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", filepath.Base(path), err)
	}
	return values, nil
}

func parseDotEnvLine(line string) (key, value string, ok bool, err error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}
	line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
	before, after, found := strings.Cut(line, "=")
	if !found {
		return "", "", false, fmt.Errorf("expected KEY=value")
	}
	key = strings.TrimSpace(before)
	if key == "" {
		return "", "", false, fmt.Errorf("empty key")
	}
	value, err = parseDotEnvValue(strings.TrimSpace(after))
	if err != nil {
		return "", "", false, err
	}
	return key, value, true, nil
}

func parseDotEnvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	switch raw[0] {
	case '\'':
		end := strings.Index(raw[1:], "'")
		if end < 0 {
			return "", fmt.Errorf("unterminated single-quoted value")
		}
		return raw[1 : 1+end], nil
	case '"':
		end, err := closingDoubleQuote(raw)
		if err != nil {
			return "", err
		}
		value, err := strconv.Unquote(raw[:end+1])
		if err != nil {
			return "", fmt.Errorf("invalid double-quoted value: %w", err)
		}
		return value, nil
	default:
		return strings.TrimSpace(stripInlineComment(raw)), nil
	}
}

func closingDoubleQuote(raw string) (int, error) {
	escaped := false
	for i := 1; i < len(raw); i++ {
		ch := raw[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '"' {
			return i, nil
		}
	}
	return 0, fmt.Errorf("unterminated double-quoted value")
}

func stripInlineComment(raw string) string {
	for i := 0; i < len(raw); i++ {
		if raw[i] == '#' && (i == 0 || raw[i-1] == ' ' || raw[i-1] == '\t') {
			return raw[:i]
		}
	}
	return raw
}
