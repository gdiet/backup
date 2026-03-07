package core

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
)

type BackupFlags struct {
	CreateDirs   bool // -p, --create-dirs
	TargetExists bool // -t, --target-exists
}

func ParseBackupFlags(args []string) (BackupFlags, []string) {
	var flags BackupFlags
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.BoolVar(&flags.CreateDirs, "p", false, "create missing target directories")
	fs.BoolVar(&flags.CreateDirs, "create-dirs", false, "create missing target directories")
	fs.BoolVar(&flags.TargetExists, "t", false, "require target to be an existing directory")
	fs.BoolVar(&flags.TargetExists, "target-exists", false, "require target to be an existing directory")
	fs.Usage = func() {
		fmt.Println("Usage of backup:")
		fmt.Println("  backup [flags] <source> [<source2> ...] <target>")
		fmt.Println("Flags:")
		fmt.Println("  -p, --create-dirs    Create missing target directories")
		fmt.Println("  -t, --target-exists  Require target to be an existing directory")
	}
	// Parse only flags, leave positional args
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		slog.Error("Error parsing flags", "error", err)
		fs.Usage()
		os.Exit(2)
	}
	return flags, fs.Args()
}
