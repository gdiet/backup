package core

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

func Initialize(repository string, settings RepositorySettings) error {
	_, err := os.Stat(repository)
	if err == nil {
		return errors.New("repository already exists: " + repository)
	}

	err = os.Mkdir(repository, 0o755)
	if err != nil {
		return fmt.Errorf("could not create repository directory %s: %w", repository, err)
	}

	err = os.Mkdir(filepath.Join(repository, "data"), 0o755)
	if err != nil {
		return fmt.Errorf("could not create data directory in %s: %w", repository, err)
	}

	db, err := InitDB(repository, settings)
	if err != nil {
		return err
	}

	err = db.Close()
	if err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	slog.Info("repository initialized", "repo", repository)
	return nil
}
