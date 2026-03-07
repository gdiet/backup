package core

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/gdiet/backup/internal/meta"
)

func Backup(repo string, sources []string, target string, tf BackupFlags) {
	// If target does not start with "/", it's invalid
	if !strings.HasPrefix(target, "/") {
		slog.Error("Target path must be absolute (start with '/')", "target", target)
		os.Exit(1)
	}

	validateSources(sources)

	// Open metadata
	metaRepo := filepath.Join(repo, "meta")
	m, err := meta.NewMetadata(metaRepo)
	if err != nil {
		slog.Error("Failed to open metadata", "repo", metaRepo, "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := m.Close(); err != nil {
			slog.Error("Failed to close metadata", "error", err)
		}
	}()

	// Split target path
	targetPath := strings.Split(strings.TrimPrefix(strings.TrimSuffix(target, "/"), "/"), "/")

	// Target validation logic
	if tf.TargetExists {
		ensureTargetExistsAndIsDir(m, targetPath)
	} else if tf.CreateDirs {
		createMissingTargetDirs(m, targetPath)
	} else {
		ensureParentIsDirAndTargetDoesNotExist(m, targetPath)
	}
	slog.Info("Validation completed. Starting backup.", "sources", sources, "target", target)

	err = backup(m, sources, targetPath)
	if err != nil {
		slog.Error("Backup failed.", "error", err)
		os.Exit(1)
	}
	slog.Info("Backup completed.")
}

func backup(m *meta.Metadata, sources []string, targetPath []string) error {
	for _, src := range sources {
		err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			switch {
			case info.IsDir():
				rel, err := filepath.Rel(src, path)
				slog.Info("Processing", "rel", rel)
				if err != nil {
					return err
				}
				// Skip root
				if rel == "." || rel == "" {
					return nil
				}
				// Build target directory path in metadata
				tgtDirPath := append(targetPath, strings.Split(rel, string(os.PathSeparator))...)
				// Create directory in metadata
				_, err = m.Mkdir(tgtDirPath)
				if err != nil && !errors.Is(err, fserr.Exists) {
					return err
				}
				slog.Info("Created directory", "source", path, "target", "/"+strings.Join(tgtDirPath, "/"))
			case info.Mode().IsRegular():
				// Handle regular files
			default:
				slog.Warn("Unsupported type - skipping", "path", path, "file mode", info.Mode())
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("source %s: %w", src, err)
		}
	}
	return nil
}

func validateSources(sources []string) {
	// Sources must exist and be directories. Later, we may allow file sources as well.
	for _, src := range sources {
		info, err := os.Stat(src)
		if err != nil {
			slog.Error("Source does not exist or is not accessible", "source", src, "error", err)
			os.Exit(1)
		}
		if !info.IsDir() {
			slog.Error("Source is not a directory", "source", src)
			os.Exit(1)
		}
	}
}

func ensureTargetExistsAndIsDir(m *meta.Metadata, targetPath []string) {
	_, entry, err := m.Lookup(targetPath)
	if err != nil {
		slog.Error("Target does not exist", "target", "/"+strings.Join(targetPath, "/"), "error", err)
		os.Exit(1)
	}
	if _, isDir := entry.(*meta.DirEntry); !isDir {
		slog.Error("Target exists but is not a directory", "target", "/"+strings.Join(targetPath, "/"))
		os.Exit(1)
	}
}

func createMissingTargetDirs(m *meta.Metadata, targetPath []string) {
	for i := 1; i <= len(targetPath); i++ {
		sub := targetPath[:i]
		_, _, err := m.Lookup(sub)
		if err != nil {
			// Only create if not found
			if err := createDirWithParents(m, sub); err != nil {
				slog.Error("Failed to create target directory", "path", "/"+strings.Join(sub, "/"), "error", err)
				os.Exit(1)
			}
		}
	}
}

// createDirWithParents creates a directory and all its parents if needed (idempotent)
func createDirWithParents(m *meta.Metadata, targetPath []string) error {
	if len(targetPath) == 0 {
		return nil // root always exists
	}
	parent := targetPath[:len(targetPath)-1]
	if err := createDirWithParents(m, parent); err != nil {
		return err
	}
	_, _, err := m.Lookup(targetPath)
	if err == nil {
		return nil // already exists
	}
	_, err = m.Mkdir(targetPath)
	return err
}

func ensureParentIsDirAndTargetDoesNotExist(m *meta.Metadata, targetPath []string) {
	_, err := m.Mkdir(targetPath)
	if err != nil {
		slog.Error("Can't create target directory", "parent", "/"+strings.Join(targetPath, "/"), "error", err)
		os.Exit(1)
	}
}
