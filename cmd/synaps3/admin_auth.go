package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/strahe/synaps3/internal/config"
	"github.com/urfave/cli/v3"
)

var saveAdminAuthSettings = config.SaveForSettings

func adminAuthCommand() *cli.Command {
	return &cli.Command{
		Name:  "admin-auth",
		Usage: "manage local Admin UI authentication",
		Commands: []*cli.Command{
			{
				Name:  "reset-password",
				Usage: "generate a new local Admin password",
				Action: func(_ context.Context, cmd *cli.Command) error {
					if cmd.Args().Len() > 0 {
						return fmt.Errorf("unexpected argument %q, reset-password takes no positional arguments", cmd.Args().First())
					}
					root := cmd.Root()
					if !root.IsSet("config") {
						return errors.New("admin-auth reset-password requires --config")
					}
					src, err := configSourceFromCommand(cmd)
					if err != nil {
						return err
					}
					cfg, presence, err := config.LoadFileForSettings(src.Path)
					if err != nil {
						return fmt.Errorf("loading config: %w", err)
					}
					bootstrap, err := config.NewAdminAuthBootstrap()
					if err != nil {
						return err
					}
					if strings.TrimSpace(cfg.Admin.Auth.Username) == "" {
						cfg.Admin.Auth.Username = "admin"
					}
					cfg.Admin.Auth.Enabled = true
					cfg.Admin.Auth.PasswordHash = bootstrap.PasswordHash
					cfg.Admin.Auth.SessionSecret = bootstrap.SessionSecret
					presence.AdminAuthEnabled = true
					presence.AdminAuthUsername = true
					presence.AdminAuthPasswordHash = true
					presence.AdminAuthSessionSecret = true
					passwordFile := config.AdminInitialPasswordFilePath(filepath.Dir(src.Path))
					backup, err := backupAdminInitialPasswordFile(passwordFile)
					if err != nil {
						return err
					}
					passwordPath, err := config.WriteAdminInitialPasswordFile(filepath.Dir(src.Path), bootstrap.Password)
					if err != nil {
						return err
					}
					if err := saveAdminAuthSettings(src.Path, cfg, presence); err != nil {
						if restoreErr := restoreAdminInitialPasswordFile(passwordPath, backup); restoreErr != nil {
							return fmt.Errorf("saving config: %w; restoring admin initial password file: %v", err, restoreErr)
						}
						return fmt.Errorf("saving config: %w", err)
					}
					_, err = fmt.Fprintf(root.Writer, "Admin password reset\nAdmin username: %s\nAdmin initial password file: %s\n", cfg.Admin.Auth.Username, passwordPath)
					return err
				},
			},
		},
	}
}

type adminInitialPasswordBackup struct {
	Exists bool
	Data   []byte
	Mode   fs.FileMode
}

func backupAdminInitialPasswordFile(path string) (adminInitialPasswordBackup, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return adminInitialPasswordBackup{}, nil
		}
		return adminInitialPasswordBackup{}, fmt.Errorf("checking admin initial password file %s: %w", path, err)
	}
	if info.IsDir() {
		return adminInitialPasswordBackup{}, fmt.Errorf("admin initial password file %s is a directory", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return adminInitialPasswordBackup{}, fmt.Errorf("reading admin initial password file %s: %w", path, err)
	}
	return adminInitialPasswordBackup{Exists: true, Data: data, Mode: info.Mode().Perm()}, nil
}

func restoreAdminInitialPasswordFile(path string, backup adminInitialPasswordBackup) error {
	if !backup.Exists {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := os.WriteFile(path, backup.Data, backup.Mode); err != nil {
		return err
	}
	return os.Chmod(path, backup.Mode)
}
