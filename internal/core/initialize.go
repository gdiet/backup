package core

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

func Initialize(repo string) error {
	_, err := os.Stat(repo)
	if err == nil {
		return fmt.Errorf("repository directory %s already exists", repo)
	}
	err = os.MkdirAll(filepath.Join(repo, "meta"), 0o755)
	if err != nil {
		return fmt.Errorf("failed to create meta directory in %s: %w", repo, err)
	}
	err = os.MkdirAll(filepath.Join(repo, "data"), 0o755)
	if err != nil {
		return fmt.Errorf("failed to create data directory in %s: %w", repo, err)
	}
	slog.Info("repository initialized", "repo", repo)
	return nil
}
