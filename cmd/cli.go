package main

import (
	"fmt"

	"github.com/gdiet/backup/internal/core"
)

type CLI struct {
	Repo     string `short:"r" default:"../backup-repository" help:"Repository directory."`
	LogLevel string `short:"l" default:"info" enum:"debug,info,warn,error" help:"Log level."`

	Init    InitCmd    `cmd:"" help:"Initialize a new repository."`
	Backup  BackupCmd  `cmd:"" help:"Back up one or more sources to a target path in the repository."`
	Restore RestoreCmd `cmd:"" help:"Restore one or more sources from the repository to a target directory."`
	Stats   StatsCmd   `cmd:"" help:"Show repository statistics."`
	Import  ImportCmd  `cmd:"" help:"Import data from a local HTTP server (experimental)."`
}

// AfterApply is a hook used by https://github.com/alecthomas/kong.
func (c *CLI) AfterApply() error {
	configureLogging(&c.LogLevel)
	return nil
}

// InitCmd initializes the repository.
type InitCmd struct{}

func (c *InitCmd) Run(cli *CLI) error {
	return core.Initialize(cli.Repo)
}

// BackupCmd backs up sources to a target path in the repository.
type BackupCmd struct {
	CreateDirs   bool     `short:"p" name:"create-dirs" help:"Create missing target directories."`
	TargetExists bool     `short:"t" name:"target-exists" help:"Require target to be an existing directory."`
	Concurrency  uint     `short:"c" default:"4" help:"Number of concurrent backup processes (1-32)."`
	Paths        []string `arg:"" name:"path" help:"One or more source paths followed by the target path in the repository."`
}

func (c *BackupCmd) Run(cli *CLI) error {
	if len(c.Paths) < 2 {
		return fmt.Errorf("backup requires one or more sources and one target")
	}
	sources, target := c.Paths[:len(c.Paths)-1], c.Paths[len(c.Paths)-1]
	flags := core.BackupFlags{
		CreateDirs:   c.CreateDirs,
		TargetExists: c.TargetExists,
		Concurrency:  c.Concurrency,
	}
	return core.Backup(cli.Repo, sources, target, flags)
}

// RestoreCmd restores sources from the repository.
type RestoreCmd struct {
	Paths []string `arg:"" name:"path" help:"One or more source paths in the repository followed by the target directory."`
}

func (c *RestoreCmd) Run(cli *CLI) error {
	if len(c.Paths) < 2 {
		return fmt.Errorf("restore requires one or more sources and one target")
	}
	sources, target := c.Paths[:len(c.Paths)-1], c.Paths[len(c.Paths)-1]
	return core.Restore(cli.Repo, sources, target)
}

// StatsCmd shows repository statistics.
type StatsCmd struct{}

func (c *StatsCmd) Run(cli *CLI) error {
	return core.Stats(cli.Repo)
}

// ImportCmd imports data from a local HTTP server.
type ImportCmd struct{}

func (c *ImportCmd) Run(cli *CLI) error {
	return core.Import(cli.Repo)
}
