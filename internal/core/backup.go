package core

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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

	fmt.Println("Target path is valid for backup.")
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

func ensureParentIsDirAndTargetDoesNotExist(m *meta.Metadata, targetPath []string) {
	if _, _, err := m.Lookup(targetPath); err == nil {
		slog.Error("Target already exists", "target", "/"+strings.Join(targetPath, "/"))
		os.Exit(1)
	}
	parentPath := targetPath[:len(targetPath)-1]
	_, parentEntry, err := m.Lookup(parentPath)
	if err != nil {
		slog.Error("Parent directory of target does not exist", "parent", "/"+strings.Join(parentPath, "/"), "error", err)
		os.Exit(1)
	}
	if _, isDir := parentEntry.(*meta.DirEntry); !isDir {
		slog.Error("Parent of target is not a directory", "parent", "/"+strings.Join(parentPath, "/"))
		os.Exit(1)
	}
}

// createDirWithParents creates a directory and all its parents if needed (idempotent)
func createDirWithParents(m *meta.Metadata, path []string) error {
	if len(path) == 0 {
		return nil // root always exists
	}
	parent := path[:len(path)-1]
	if err := createDirWithParents(m, parent); err != nil {
		return err
	}
	_, _, err := m.Lookup(path)
	if err == nil {
		return nil // already exists
	}
	_, err = m.Mkdir(path)
	return err
}
