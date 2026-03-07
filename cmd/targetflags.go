package main

import (
	"flag"
	"log/slog"
	"os"
)

type TargetFlags struct {
	CreateDirs   bool // -p, --create-dirs
	TargetExists bool // -t, --target-exists
}

func ParseTargetFlags(args []string) (TargetFlags, []string) {
	var tf TargetFlags
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.BoolVar(&tf.CreateDirs, "p", false, "create missing target directories")
	fs.BoolVar(&tf.CreateDirs, "create-dirs", false, "create missing target directories")
	fs.BoolVar(&tf.TargetExists, "t", false, "require target to be an existing directory")
	fs.BoolVar(&tf.TargetExists, "target-exists", false, "require target to be an existing directory")
	// Use main's printUsage for flag help output
	fs.Usage = printUsage
	// Parse only flags, leave positional args
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			fs.Usage()
			os.Exit(0)
		}
		slog.Error("Error parsing flags", "error", err)
		fs.Usage()
		os.Exit(2)
	}
	return tf, fs.Args()
}
