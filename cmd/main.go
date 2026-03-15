package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/gdiet/backup/internal/core"
	"github.com/gdiet/backup/internal/util"
)

func main() {
	err := runMain()
	if err != nil {
		slog.Error(err.Error())
		if errors.Is(err, util.InvalidError) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func runMain() error {
	repo := flag.String("repo", "../backup-repository", "Repository directory")
	logLevel := flag.String("logLevel", "info", "Log level")
	flag.Parse()

	configureLogging(logLevel)

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		return nil
	}

	cmd := args[0]
	args = args[1:]

	switch cmd {
	case "backup":
		return core.Backup(*repo, args)
	case "init":
		return core.Initialize(*repo)
	case "restore":
		return core.Restore(*repo, args[:len(args)-1], args[len(args)-1])
	case "stats":
		return core.Stats(*repo)
	}
	printUsage()
	return util.Invalidf("command %s not recognized", cmd)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  [flags] command")
	fmt.Println("Flags:")
	fmt.Println("  -repo=<target dir>")
	fmt.Println("  -logLevel=<log level>")
	fmt.Println("Commands:")
	fmt.Println("  init")
	fmt.Println("  backup [flags] <source> [<source2> ...] <target>")
	fmt.Println("  restore [flags] <source> [<source2> ...] <target>")
	fmt.Println("  stats")
}
