package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/gdiet/backup/internal/util"
)

func main() {
	err := runCli()
	if err != nil {
		slog.Error(err.Error())
		if errors.Is(err, util.InvalidError) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}
