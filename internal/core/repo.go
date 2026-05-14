package core

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gdiet/backup/internal/meta"
)

func DBDir(repository string) string {
	return filepath.Join(repository, "meta")
}

func InitDB(repository string, settings repositorySettings) (*meta.DB, error) {
	dbDir := DBDir(repository)
	err := os.MkdirAll(dbDir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create database directory %s: %w", dbDir, err)
	}

	db, err := meta.InitDB(dbDir, settings.SettingsMap())
	if err != nil {
		return nil, fmt.Errorf("failed to open database from %s: %w", dbDir, err)
	}
	return db, nil
}

func OpenDB(repository string) (*meta.DB, repositorySettings, error) {
	dbDir := DBDir(repository)
	db, settings, err := meta.OpenDB(dbDir)
	if err != nil {
		return nil, repositorySettings{}, err
	}
	return db, NewRepositorySettingsFrom(settings), nil
}
