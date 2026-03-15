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
	"github.com/gdiet/backup/internal/util"
)

func Backup(repo string, args []string) error {
	tf, rest := ParseBackupFlags(args)
	if len(rest) < 2 {
		return util.Invalid("backup requires one or more sources and one target")
	}
	sources, target := rest[:len(rest)-1], rest[len(rest)-1]

	if !strings.HasPrefix(target, "/") {
		return util.Invalidf("target %s must start with '/'", target)
	}
	normalizedTarget := strings.TrimSuffix(target, "/")
	targetPath := strings.Split(normalizedTarget, "/")[1:]

	err := validateSources(sources)
	if err != nil {
		return err
	}

	// Open metadata
	metaRepo := filepath.Join(repo, "meta")
	m, err := meta.NewMetadata(metaRepo)
	if err != nil {
		return fmt.Errorf("failed to open database from %s: %w", metaRepo, err)
	}
	defer func() {
		err = m.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("failed to close database: %s", err))
		}
	}()

	// Target validation logic
	var parentID []byte
	err = m.Write(func(c *meta.Context) error {
		if tf.TargetExists {
			parentID, err = ensureTargetExistsAndIsDir(c, targetPath)
		} else if tf.CreateDirs {
			parentID, err = c.Mkdirs(targetPath)
		} else {
			parentID, err = c.Mkdir(targetPath)
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to validate backup target %s: %w", normalizedTarget, err)
	}
	slog.Info("validation OK, starting backup", "sources", sources, "target", normalizedTarget)

	backup(m, sources, parentID, normalizedTarget)
	return nil
}

func backup(m *meta.Metadata, sources []string, parentID []byte, normalizedTarget string) {
	warnings := 0
	for _, src := range sources {
		info, err := os.Stat(src)
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to access source %s: %s", src, err))
			warnings++
			continue
		}
		warnings += backupEntry(m, src, info, parentID, normalizedTarget+"/"+info.Name())
	}
	if warnings == 0 {
		slog.Info("backup completed successfully")
	} else {
		slog.Info(fmt.Sprintf("backup completed with %d warnings", warnings))
	}
}

func backupEntry(m *meta.Metadata, src string, info os.FileInfo, parentID []byte, normalizedTarget string) int {
	if info.IsDir() {
		return backupDirectory(m, src, info, parentID, normalizedTarget)
	} else if info.Mode().IsRegular() {
		// TODO: Handle regular files (add file to metadata, deduplication, etc.)
		slog.Debug(fmt.Sprintf("would backup file %s", src))
		return 0
	} else {
		slog.Warn(fmt.Sprintf("unsupported type - skipping %s (file type: %s)", src, info.Mode().Type()))
		return 1
	}
}

func backupDirectory(m *meta.Metadata, src string, info os.FileInfo, parentID []byte, normalizedTarget string) int {
	var err error
	var id []byte
	err = m.Write(func(c *meta.Context) error { id, err = c.MkdirUnchecked(parentID, info.Name()); return err })
	if err != nil && !errors.Is(err, fserr.Exists) {
		slog.Warn(fmt.Sprintf("failed to create target directory %s: %s", normalizedTarget, err))
		return 1
	}
	slog.Debug(fmt.Sprintf("created target directory %s", normalizedTarget))
	// Read source directory
	entries, err := os.ReadDir(src)
	if err != nil {
		slog.Warn(fmt.Sprintf("failed to read directory %s: %s", src, err))
		return 1
	}
	warnings := 0
	for _, entry := range entries {
		info, err = entry.Info()
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to access directory entry %s: %s", entry.Name(), err))
			warnings++
			continue
		}
		child := filepath.Join(src, entry.Name())
		warnings += backupEntry(m, child, info, id, normalizedTarget+"/"+info.Name())
	}
	return warnings
}

func validateSources(sources []string) error {
	for _, src := range sources {
		_, err := os.Stat(src)
		if err != nil {
			return fmt.Errorf("failed to access source %s: %w", src, err)
		}
	}
	return nil
}

func ensureTargetExistsAndIsDir(c *meta.Context, targetPath []string) ([]byte, error) {
	entry, err := c.Lookup(targetPath)
	if err != nil {
		return nil, err
	}
	if _, isDir := entry.(*meta.DirEntry); !isDir {
		return nil, fmt.Errorf("target is not a directory")
	}
	return entry.ID(), nil
}
