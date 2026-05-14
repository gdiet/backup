package core

import (
	"fmt"
	"strconv"

	"github.com/gdiet/backup/internal/util"
)

// repositorySettings define user defined per-repository constants. They are stored in the meta database.
//
// The repository settings are set when a repository is created. A migration would be needed to update them later.
type repositorySettings struct {
	cdcTargetSizeBits int    // Default 20, valid [10..30]
	chunking          string // Default "cdc", valid ["file", "cdc", "jpeg+cdc"]
}

func NewRepositorySettings(cdcTargetSizeBits int, chunking string) repositorySettings {
	util.Assertf(cdcTargetSizeBits >= 10 && cdcTargetSizeBits <= 30, "cdc target size %d not in range 10-30", cdcTargetSizeBits)
	util.Assertf(chunking == "file" || chunking == "cdc" || chunking == "jpeg+cdc", "invalid chunking method: %s", chunking)
	return repositorySettings{cdcTargetSizeBits, chunking}
}

func NewRepositorySettingsFrom(settings map[string]string) repositorySettings {
	cdcTargetSizeBits, err := strconv.Atoi(settings["cdcTargetSizeBits"])
	util.Assertf(err != nil, "cdcTargetSizeBits %s is not an integer", settings["cdcTargetSizeBits"])
	return NewRepositorySettings(cdcTargetSizeBits, settings["chunking"])
}

func (settings repositorySettings) SettingsMap() map[string]string {
	return map[string]string{
		"cdcTargetSizeBits": fmt.Sprintf("%d", settings.cdcTargetSizeBits),
		"chunking":          settings.chunking,
	}
}
