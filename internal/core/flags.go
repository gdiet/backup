package core

import (
	"flag"
	"fmt"

	"github.com/gdiet/backup/internal/util"
)

type BackupFlags struct {
	CreateDirs   bool // -p, --create-dirs     default false
	TargetExists bool // -t, --target-exists   default false
	Concurrency  uint // -c, --concurrency     default 4
}

func ParseBackupFlags(args []string) (BackupFlags, []string) {
	var flags BackupFlags
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	fs.BoolVar(&flags.CreateDirs, "p", false, "create missing target directories")
	fs.BoolVar(&flags.CreateDirs, "create-dirs", false, "create missing target directories")
	fs.BoolVar(&flags.TargetExists, "t", false, "require target to be an existing directory")
	fs.BoolVar(&flags.TargetExists, "target-exists", false, "require target to be an existing directory")
	fs.UintVar(&flags.Concurrency, "c", 4, "number of concurrent backup processes")
	fs.UintVar(&flags.Concurrency, "concurrency", 4, "number of concurrent backup processes")
	fs.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("  backup [flags] <source> [<source2> ...] <target>")
		fmt.Println("Flags:")
		fmt.Println("  -p, --create-dirs    Create missing target directories")
		fmt.Println("  -t, --target-exists  Require target to be an existing directory")
	}
	// Parse only flags, leave positional args
	err := fs.Parse(args)
	util.Assertf(err == nil, "Expected FlagSet to exit instead of failing: %s", err)
	return flags, fs.Args()
}
