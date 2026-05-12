package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/alecthomas/kong"
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
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("backup"),
		kong.Description("Deduplicating backup application."),
		kong.UsageOnError(),
	)
	return ctx.Run(&cli)
}
