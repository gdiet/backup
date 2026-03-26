package core

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

func backup(m *meta.Metadata, sources []string, parentID []byte, target string) {
	warnings := &atomic.Uint64{}
	// worker pool for running func backupFile
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4) // TODO: eventually, make concurrency level configurable
	for _, src := range sources {
		info, err := os.Stat(src)
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to access source %s: %s", src, err))
			warnings.Add(1)
			continue
		}
		backupEntry(m, src, info, parentID, target+"/"+info.Name(), &techParams{warnings, &wg, sem})
	}
	wg.Wait()
	if warnings.Load() == 0 {
		slog.Info("backup completed successfully")
	} else {
		slog.Info(fmt.Sprintf("backup completed with %d warnings", warnings.Load()))
	}
}

type techParams struct {
	warnings *atomic.Uint64
	wg       *sync.WaitGroup
	sem      chan struct{}
}

func backupEntry(
	m *meta.Metadata, src string, info os.FileInfo, parentID []byte, target string, tp *techParams,
) {
	if info.IsDir() {
		backupDirectory(m, src, info, parentID, target, tp)
		return
	}
	if info.Mode().IsRegular() {
		backupFile(m, src, info, parentID, target, tp)
		return
	}
	slog.Warn(fmt.Sprintf("unsupported type - skipping %s (file type: %s)", src, info.Mode().Type()))
	tp.warnings.Add(1)
}

func backupFile(
	m *meta.Metadata, src string, info os.FileInfo, parentID []byte, target string, tp *techParams,
) {
	tp.sem <- struct{}{} // block if worker pool is full
	tp.wg.Add(1)
	go func() {
		defer tp.wg.Done()
		defer func() { <-tp.sem }()
		// jpg, jpeg, ...: FF D8 FF, and fourth byte C0–FE => custom chunking
		// avif, avis, ...: bytes[4..7] == 'avif' or bytes[4..7] == 'avis' => custom chunking
		// file <= 512*1024 => single chunk
		// otherwise => cdc
		time.Sleep(1 * time.Second)
		slog.Debug(fmt.Sprintf("backing up %s to %s", src, target))
		tp.warnings.Add(1) // TODO: implement file backup
	}()
}

func backupDirectory(
	m *meta.Metadata, src string, info os.FileInfo, parentID []byte, target string, tp *techParams,
) {
	var err error
	var id []byte
	err = m.Write(func(c *meta.Context) error { id, err = c.MkdirUnchecked(parentID, info.Name()); return err })
	if err != nil && !errors.Is(err, fserr.Exists) {
		slog.Warn(fmt.Sprintf("failed to create target directory %s: %s", target, err))
		tp.warnings.Add(1)
		return
	}
	slog.Debug(fmt.Sprintf("created target directory %s", target))
	// Read source directory
	entries, err := os.ReadDir(src)
	if err != nil {
		slog.Warn(fmt.Sprintf("failed to read directory %s: %s", src, err))
		tp.warnings.Add(1)
		return
	}
	for _, entry := range entries {
		info, err = entry.Info()
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to access directory entry %s: %s", entry.Name(), err))
			tp.warnings.Add(1)
			continue
		}
		child := filepath.Join(src, entry.Name())
		backupEntry(m, child, info, id, target+"/"+info.Name(), tp)
	}
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
