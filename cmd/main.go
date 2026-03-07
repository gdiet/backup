package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/gdiet/backup/internal/core"
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
	case "backup":
		if tf, rest := core.ParseBackupFlags(args); len(rest) >= 2 {
			core.Backup(*repo, rest[:len(rest)-1], rest[len(rest)-1], tf)
		} else {
			slog.Error("backup requires at least one source and one target")
			os.Exit(2)
		}
	case "init":
		core.Initialize(*repo)
	case "restore":
		core.Restore(*repo, args[:len(args)-1], args[len(args)-1])
	case "stats":
		core.Stats(*repo)
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  [-repo=<target-dir>] init")
	fmt.Println("  [-repo=<target-dir>] stats")
	fmt.Println("  [-repo=<target-dir>] backup [flags] <source> [<source2> ...] <target>")
	fmt.Println("  [-repo=<target-dir>] restore [flags] <source> [<source2> ...] <target>")
}
