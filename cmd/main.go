package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/gdiet/backup/meta"
)

func main() {
	repo := flag.String("repo", "../backup-repository", "Repository directory")
	flag.Parse()

	args := flag.Args()

	if len(args) < 1 {
		printUsage()
		return
	}

	cmd := args[0]
	args = args[1:]

	switch cmd {
	case "init":
		initialize(*repo)
	case "stats":
		mockStats(*repo)
	case "backup":
		backup(*repo, args)
	case "restore":
		mockRestore(*repo, args)
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  [-repo=<target-dir>] init")
	fmt.Println("  [-repo=<target-dir>] stats")
	fmt.Println("  [-repo=<target-dir>] backup <source> [<source2> ...] <target>")
	fmt.Println("  [-repo=<target-dir>] restore <source> <target>")
}

func initialize(repo string) {
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

func mockStats(repo string) {
	fmt.Printf("[MOCK] Showing repository stats for '%s'\n", repo)
}

func backup(repo string, args []string) {
	tf, rest := ParseTargetFlags(args)
	if len(rest) < 2 {
		slog.Error("backup requires at least one source and one target")
		os.Exit(1)
	}
	sources := rest[:len(rest)-1]
	target := rest[len(rest)-1]

	// Validate sources. Later, we may also allow files as sources.
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

	// Open metadata
	metaRepo := filepath.Join(repo, "meta")
	m, err := meta.NewMetadata(metaRepo)
	if err != nil {
		slog.Error("Failed to open metadata", "repo", metaRepo, "error", err)
		os.Exit(1)
	}
	defer m.Close()

	// Split target path
	targetPath := strings.Split(strings.Trim(target, "/"), "/")

	// Target validation logic
	if tf.TargetExists {
		// Target must exist and be a directory
		_, entry, err := m.Lookup(targetPath)
		if err != nil {
			slog.Error("Target does not exist", "target", target, "error", err)
			os.Exit(1)
		}
		if _, isDir := entry.(*meta.DirEntry); !isDir {
			slog.Error("Target exists but is not a directory", "target", target)
			os.Exit(1)
		}
	} else if tf.CreateDirs {
		// Create missing directories in target path
		for i := 1; i <= len(targetPath); i++ {
			sub := targetPath[:i]
			_, _, err := m.Lookup(sub)
			if err != nil {
				// Only create if not found
				if err := createDirWithParents(m, sub); err != nil {
					slog.Error("Failed to create target directory", "path", strings.Join(sub, "/"), "error", err)
					os.Exit(1)
				}
			}
		}
	} else {
		// Default: error if target exists, error if parent does not exist
		if _, _, err := m.Lookup(targetPath); err == nil {
			slog.Error("Target already exists", "target", target)
			os.Exit(1)
		}
		parentPath := targetPath[:len(targetPath)-1]
		_, parentEntry, err := m.Lookup(parentPath)
		if err != nil {
			slog.Error("Parent directory of target does not exist", "parent", strings.Join(parentPath, "/"), "error", err)
			os.Exit(1)
		}
		if _, isDir := parentEntry.(*meta.DirEntry); !isDir {
			slog.Error("Parent of target is not a directory", "parent", strings.Join(parentPath, "/"))
			os.Exit(1)
		}
	}

	fmt.Println("Target path is valid for backup.")
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

func mockRestore(repo string, args []string) {
	if len(args) < 2 {
		fmt.Println("[MOCK] restore requires a source and a target")
		return
	}
	sources := args[:len(args)-1]
	target := args[len(args)-1]
	fmt.Printf("[MOCK] Restoring sources %v to target '%s' in repo '%s'\n", sources, target, repo)
}
