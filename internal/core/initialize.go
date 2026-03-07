package core

import (
	"log/slog"
	"os"
	"path/filepath"
)

func Initialize(repo string) {
	if _, err := os.Stat(repo); err == nil {
		slog.Error("Repository directory already exists", "repo", repo)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Join(repo, "meta"), 0o755); err != nil {
		slog.Error("Failed to create meta directory", "repo", repo, "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Join(repo, "data"), 0o755); err != nil {
		slog.Error("Failed to create data directory", "repo", repo, "error", err)
		os.Exit(1)
	}
	slog.Info("Repository initialized", "repo", repo)
}
