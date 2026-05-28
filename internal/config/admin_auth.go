package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/strahe/synaps3/internal/securetoken"
	"golang.org/x/crypto/bcrypt"
)

const adminInitialPasswordFileName = "admin-initial-password"

// AdminAuthBootstrap contains generated local admin credentials and session secret.
type AdminAuthBootstrap struct {
	Password      string
	PasswordHash  string
	SessionSecret string
}

// NewAdminAuthBootstrap generates a password, bcrypt hash, and session secret.
func NewAdminAuthBootstrap() (AdminAuthBootstrap, error) {
	password, err := securetoken.URL(32)
	if err != nil {
		return AdminAuthBootstrap{}, fmt.Errorf("generating admin password: %w", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return AdminAuthBootstrap{}, fmt.Errorf("hashing admin password: %w", err)
	}
	sessionSecret, err := securetoken.URL(32)
	if err != nil {
		return AdminAuthBootstrap{}, fmt.Errorf("generating admin session secret: %w", err)
	}
	return AdminAuthBootstrap{
		Password:      password,
		PasswordHash:  string(hash),
		SessionSecret: sessionSecret,
	}, nil
}

func WriteAdminInitialPasswordFile(appDir, password string) (string, error) {
	password = strings.TrimSpace(password)
	if password == "" {
		return "", fmt.Errorf("admin initial password is empty")
	}
	path := AdminInitialPasswordFilePath(appDir)
	data := []byte(password + "\n")
	f, err := os.CreateTemp(appDir, adminInitialPasswordFileName+".*.tmp")
	if err != nil {
		return "", fmt.Errorf("creating admin initial password temp file in %s: %w", appDir, err)
	}
	tempPath := f.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = f.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if err := f.Chmod(0o600); err != nil {
		return "", fmt.Errorf("locking admin initial password temp file %s: %w", tempPath, err)
	}
	if _, err := f.Write(data); err != nil {
		return "", fmt.Errorf("writing admin initial password temp file %s: %w", tempPath, err)
	}
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("syncing admin initial password temp file %s: %w", tempPath, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("closing admin initial password temp file %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return "", fmt.Errorf("replacing admin initial password file %s: %w", path, err)
	}
	renamed = true
	return path, nil
}

// AdminInitialPasswordFilePath returns the generated Admin password file path.
func AdminInitialPasswordFilePath(appDir string) string {
	return filepath.Join(appDir, adminInitialPasswordFileName)
}

// ReadAdminInitialPasswordFile reads a generated Admin password if the file exists.
func ReadAdminInitialPasswordFile(appDir string) (string, bool, error) {
	path := AdminInitialPasswordFilePath(appDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", true, fmt.Errorf("reading admin initial password file %s: %w", path, err)
	}
	password := strings.TrimSpace(string(data))
	if password == "" {
		return "", true, fmt.Errorf("admin initial password file %s is empty", path)
	}
	return password, true, nil
}
