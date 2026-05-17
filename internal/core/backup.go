package core

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/fserr"
	"github.com/gdiet/backup/internal/meta"
	"github.com/gdiet/backup/internal/util"
	"lukechampine.com/blake3"
)

type BackupFlags struct {
	createDirs   bool // -p, --create-dirs     default false
	targetExists bool // -t, --target-exists   default false
	concurrency  uint // -c, --concurrency     default 4
}

func NewBackupFlags(createDirs bool, targetExists bool, concurrency uint) BackupFlags {
	return BackupFlags{createDirs, targetExists, concurrency}
}

type backupParams struct {
	BackupFlags
	RepositorySettings
	db        *meta.DB
	cdcConfig *cdc.Config
}

// Backup stores the sources in the repository target folder. Leading and trailing "/" in target are ignored.
func Backup(repo string, sources []string, target string, flags BackupFlags) error {
	targetPath := strings.Split(strings.Trim(target, "/"), "/")

	err := validateSources(sources)
	if err != nil {
		return err
	}

	db, settings, err := OpenDB(repo)
	if err != nil {
		return err
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("failed to close database: %s", err))
		}
	}()

	cdcConfig, err := cdc.NewConfig(settings.cdcTargetSizeBits)
	if err != nil {
		return err
	}

	b := &backupParams{flags, settings, db, cdcConfig}

	parentID, err := validateTarget(b, targetPath, target)
	if err != nil {
		return err
	}

	slog.Info("validation OK, starting backup", "sources", sources, "target", target)
	backup(b, sources, parentID, target)
	return nil
}

func backup(b *backupParams, sources []string, parentID []byte, target string) {
	warnings := &atomic.Uint64{}
	// worker pool for running func backupFile
	var wg sync.WaitGroup
	sem := make(chan struct{}, b.concurrency)
	for _, src := range sources {
		info, err := os.Stat(src)
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to access source %s: %s", src, err))
			warnings.Add(1)
			continue
		}
		backupEntry(b, src, info, parentID, target+"/"+info.Name(), &techParams{warnings, &wg, sem})
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
	b *backupParams, src string, info os.FileInfo, parentID []byte, target string, tp *techParams,
) {
	if info.IsDir() {
		backupDirectory(b, src, info, parentID, target, tp)
		return
	}
	if info.Mode().IsRegular() {
		tp.sem <- struct{}{} // block if worker pool is full
		tp.wg.Go(func() {
			defer func() { <-tp.sem }()
			backupFile(b, src, info, parentID, target, tp)
		})
		return
	}
	slog.Warn(fmt.Sprintf("unsupported type - skipping %s (file type: %s)", src, info.Mode().Type()))
	tp.warnings.Add(1)
}

func backupFile(
	b *backupParams, src string, info os.FileInfo, parentID []byte, target string, tp *techParams,
) {
	f, err := os.Open(src)
	if err != nil {
		slog.Warn(fmt.Sprintf("failed to open file %s: %s", src, err))
		tp.warnings.Add(1)
		return
	}
	defer func() {
		err := f.Close()
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to close file %s: %s", src, err))
			tp.warnings.Add(1)
		}
	}()
	var chunker cdc.Chunker
	switch {
	case b.chunking == chunkingNone || info.Size() <= 256*1024: // FIXME define the magic numbers as constants somewhere
		chunker = cdc.NewSingleChunk()
	case b.chunking == chunkingCdc:
		chunker = b.cdcConfig.NewCDC()
	case b.chunking == chunkingJpeg:
		chunker = b.cdcConfig.NewFileSpecificChunker()
	default:
		// TODO maybe better to instantiate the chunker during setup (instead of the cdcConfig)?
		util.AssertionFailedf("unsupported chunking method, default is CDC: %s", b.chunking)
		chunker = b.cdcConfig.NewCDC()
	}
	hasher := cdc.NewHashingChunker(blake3.New(20, nil), chunker) // FIXME define the magic numbers as constants somewhere
	buf := make([]byte, 64*1024)                                  // FIXME define the magic numbers as constants somewhere
	var result []cdc.LengthHash
	for {
		n, err := f.Read(buf)
		result = append(result, hasher.Next(buf[:n])...)
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to read from file %s: %s", src, err))
			tp.warnings.Add(1)
			return
		}
	}
	result = append(result, hasher.Flush()...)
	var resultStrs []string
	for _, lh := range result {
		resultStrs = append(resultStrs, lh.String())
	}
	// TODO instead of logging, deduplicate and store
	slog.Info(fmt.Sprintf("data of %s: [%s]", src, strings.Join(resultStrs, ", ")))
	slog.Debug(fmt.Sprintf("backing up %s to %s", src, target))
}

func backupDirectory(
	b *backupParams, src string, info os.FileInfo, parentID []byte, target string, tp *techParams,
) {
	var err error
	var id []byte
	err = b.db.Write(func(c *meta.Context) error { id, err = c.MkdirUnchecked(parentID, info.Name()); return err })
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
		backupEntry(b, child, info, id, target+"/"+info.Name(), tp)
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

func validateTarget(b *backupParams, targetPath []string, normalizedTarget string) ([]byte, error) {
	var parentID []byte
	err := b.db.Write(func(c *meta.Context) error {
		var err error
		switch {
		case b.targetExists:
			parentID, err = ensureTargetExistsAndIsDir(c, targetPath)
		case b.createDirs:
			parentID, err = c.Mkdirs(targetPath)
		default:
			parentID, err = c.Mkdir(targetPath)
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to validate backup target %s: %w", normalizedTarget, err)
	}
	return parentID, nil
}
