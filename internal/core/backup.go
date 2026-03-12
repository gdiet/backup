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
		createTargetDirOnly(m, targetPath)
	}
	slog.Info("Validation OK, starting backup.", "sources", sources, "target", target)

	err = backup(m, sources, targetPath)
	if err != nil {
		slog.Error("Backup failed.", "error", err)
		os.Exit(1)
	}
	slog.Info("Backup completed.")
}

func backup(m *meta.Metadata, sources []string, targetPath []string) error {
	for _, src := range sources {
		info, err := os.Stat(src)
		if err != nil {
			return fmt.Errorf("source %s: %w", src, err)
		}
		// Start recursion at root of source
		relPath := []string{}
		err = backupRecursive(m, src, info, targetPath, relPath)
		if err != nil {
			return fmt.Errorf("source %s: %w", src, err)
		}
	}
	return nil
}

// backupRecursive recursively backs up a directory tree, passing the current relative path
func backupRecursive(m *meta.Metadata, src string, info os.FileInfo, targetPath []string, relPath []string) error {
	// Build target directory path in metadata
	tgtDirPath := append(targetPath, relPath...)
	if info.IsDir() {
		// Create directory in metadata (idempotent)
		_, err := m.Mkdir(tgtDirPath)
		if err != nil && !errors.Is(err, fserr.Exists) {
			return err
		}
		slog.Info("Created directory", "source", src, "target", "/"+strings.Join(tgtDirPath, "/"))
		// Read directory contents
		entries, err := os.ReadDir(filepath.Join(src, filepath.Join(relPath...)))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			entryInfo, err := entry.Info()
			if err != nil {
				slog.Warn("Failed to stat entry", "path", entry.Name(), "error", err)
				continue
			}
			nextRelPath := append(relPath, entry.Name())
			err = backupRecursive(m, src, entryInfo, targetPath, nextRelPath)
			if err != nil {
				return err
			}
		}
	} else if info.Mode().IsRegular() {
		// TODO: Handle regular files (add file to metadata, deduplication, etc.)
		slog.Info("Would backup file", "source", filepath.Join(src, filepath.Join(relPath...)), "target", "/"+strings.Join(tgtDirPath, "/"))
	} else {
		slog.Warn("Unsupported type - skipping", "path", filepath.Join(src, filepath.Join(relPath...)), "file mode", info.Mode())
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
	entry, err := m.Lookup(targetPath)
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
	//parentId := meta.RootID64
	//for _, filename := range targetPath {
	//	id, _, err := m.GetChild(parentId, filename)
	//	switch err {
	//	case nil:
	//		parentId = id
	//	case fserr.NotFound:
	//		id, err = m.Mkdir(append([]string{parentId}, filename))
	//		if err != nil {
	//			slog.Error("Failed to create target directory", "path", "/"+strings.Join(targetPath, "/"), "error", err)
	//			os.Exit(1)
	//		}
	//		parentId = id
	//	default:
	//		// other error
	//		slog.Error("Failed to lookup target directory", "path", "/"+strings.Join(targetPath, "/"), "error", err)
	//		os.Exit(1)
	//	}
	//}
	// FIXME check implementation, then remove below
	for i := 1; i <= len(targetPath); i++ {
		sub := targetPath[:i]
		if _, err := m.Lookup(sub); err != nil {
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
	if _, err := m.Lookup(targetPath); err == nil {
		return nil // already exists
	}
	_, err := m.Mkdir(targetPath)
	return err
}

func createTargetDirOnly(m *meta.Metadata, targetPath []string) {
	_, err := m.Mkdir(targetPath)
	if err != nil {
		slog.Error("Can't create target directory", "parent", "/"+strings.Join(targetPath, "/"), "error", err)
		os.Exit(1)
	}
}
