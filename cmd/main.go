package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/alecthomas/kong"
	"github.com/gdiet/backup/internal/core"
	"github.com/gdiet/backup/internal/util"
)

func main() {
	var cli cli
	ctx := kong.Parse(&cli,
		kong.Name("backup"),
		kong.Description("Deduplicating backup application."),
		kong.UsageOnError(),
	)
	err := ctx.Run(&cli)
	if err != nil {
		slog.Error(err.Error())
		if util.IsInvalid(err) {
			println("invalid")
			os.Exit(2)
		}
		println("failed")
		os.Exit(1)
	}
}

type cli struct {
	Repo     string `short:"r" default:"../backup-repository" help:"Repository directory."`
	LogLevel string `short:"l" default:"info" enum:"debug,info,warn,error" help:"Log level."`

	Init    initCmd    `cmd:"" help:"Initialize a new repository."`
	Backup  backupCmd  `cmd:"" help:"Back up one or more sources to a target path in the repository."`
	Restore restoreCmd `cmd:"" help:"Restore one or more sources from the repository to a target directory."`
	Stats   statsCmd   `cmd:"" help:"Show repository statistics."`
	Import  importCmd  `cmd:"" help:"Import data from a local HTTP server (experimental)."`
}

func (c *cli) AfterApply() error { // kong hook
	return configureLogging(c.LogLevel)
}

// initCmd initializes a new repository.
type initCmd struct {
	CdcTargetSizeBits  int    `short:"s" name:"cdc-target-size-bits" default:"20" help:"CDC target size in bits (10-30)."`
	Chunking           string `short:"c" name:"chunking" default:"cdc" enum:"none,cdc,jpeg+cdc" help:"Chunking method."`
	repositorySettings core.RepositorySettings
}

func (c *initCmd) Validate() error { // kong hook
	var err error
	c.repositorySettings, err = core.NewRepositorySettings(c.CdcTargetSizeBits, c.Chunking)
	if err != nil {
		return err
	}
	if c.CdcTargetSizeBits < 10 || c.CdcTargetSizeBits > 30 {
		return fmt.Errorf("cdc-target-size-bits must be between 10 and 30")
	}
	return nil
}

func (c *initCmd) Run(cli *cli) error { // kong hook
	return core.Initialize(cli.Repo, c.repositorySettings)
}

// backupCmd backs up sources to a target path in the repository.
type backupCmd struct {
	CreateDirs   bool     `short:"p" name:"create-dirs" help:"Create missing target directories."`
	TargetExists bool     `short:"t" name:"target-exists" help:"Require target to be an existing directory."`
	Concurrency  uint     `short:"c" default:"4" help:"Number of concurrent backup processes (1-32)."`
	Paths        []string `arg:"" name:"path" help:"One or more source paths followed by the target path in the repository."`
}

func (c *backupCmd) Run(cli *cli) error { // kong hook
	if len(c.Paths) < 2 {
		return fmt.Errorf("backup requires one or more sources and one target")
	}
	sources, target := c.Paths[:len(c.Paths)-1], c.Paths[len(c.Paths)-1]
	flags := core.NewBackupFlags(c.CreateDirs, c.TargetExists, c.Concurrency)
	return core.Backup(cli.Repo, sources, target, flags)
}

// restoreCmd restores sources from the repository.
type restoreCmd struct {
	Paths []string `arg:"" name:"path" help:"One or more source paths in the repository followed by the target directory."`
}

func (c *restoreCmd) Run(cli *cli) error { // kong hook
	if len(c.Paths) < 2 {
		return fmt.Errorf("restore requires one or more sources and one target")
	}
	sources, target := c.Paths[:len(c.Paths)-1], c.Paths[len(c.Paths)-1]
	return core.Restore(cli.Repo, sources, target)
}

// statsCmd shows repository statistics.
type statsCmd struct{}

func (c *statsCmd) Run(cli *cli) error { // kong hook
	return core.Stats(cli.Repo)
}

// importCmd imports data from a local HTTP server.
type importCmd struct{}

func (c *importCmd) Run(cli *cli) error { // kong hook
	return core.Import(cli.Repo)
}
