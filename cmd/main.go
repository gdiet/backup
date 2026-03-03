package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	repo := "../backup-repository"

	args := os.Args[1:]
	for i, arg := range args {
		if strings.HasPrefix(arg, "-repo=") {
			repo = arg[6:]
			args = append(args[:i], args[i+1:]...)
			break
		}
	}

	if len(args) < 1 {
		printUsage()
		return
	}

	cmd := args[0]
	args = args[1:]

	switch cmd {
	case "init":
		initialize(repo)
	case "stats":
		mockStats(repo)
	case "backup":
		backup(repo, args)
	case "restore":
		mockRestore(repo, args)
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  init [-repo=<target-dir>]")
	fmt.Println("  stats [-repo=<target-dir>]")
	fmt.Println("  backup <source> [<source2> ...] <target> [-repo=<target-dir>]")
	fmt.Println("  restore <source> <target> [-repo=<target-dir>]")
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
	if len(args) < 2 {
		slog.Error("backup requires at least one source and one target")
		os.Exit(1)
	}
	sources := args[:len(args)-1]
	// target := args[len(args)-1] // Not used in this step

	// Later, we may also allow files as sources.
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
	fmt.Println("All sources are valid directories.")
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
