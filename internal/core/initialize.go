package core

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/gdiet/backup/internal/meta"
)

func Initialize(repo string, settings RepositorySettings) error {
	_, err := os.Stat(repo)
	if err == nil {
		return fmt.Errorf("repository directory %s already exists", repo)
	}

	err = os.MkdirAll(filepath.Join(repo, "data"), 0o755)
	if err != nil {
		return fmt.Errorf("failed to create data directory in %s: %w", repo, err)
	}

	dbDir := filepath.Join(repo, "meta")
	err = os.MkdirAll(dbDir, 0o755)
	if err != nil {
		return fmt.Errorf("failed to create meta directory in %s: %w", repo, err)
	}

	m, err := meta.NewMetadata(dbDir, settings.SettingsMap())
	if err != nil {
		return fmt.Errorf("failed to initialize metadata database in %s: %w", dbDir, err)
	}

	err = m.Close()
	if err != nil {
		return fmt.Errorf("failed to close metadata database in %s: %w", dbDir, err)
	}

	slog.Info("repository initialized", "repo", repo)
	return nil
}
