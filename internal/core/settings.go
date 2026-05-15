package core

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/gdiet/backup/internal/util"
)

type chunkingType string

const (
	chunkingNone chunkingType = "none"
	chunkingCdc  chunkingType = "cdc"
	chunkingJpeg chunkingType = "jpeg+cdc"
)

var chunkingTypes = []chunkingType{chunkingNone, chunkingCdc, chunkingJpeg}

// repositorySettings define user defined per-repository constants. They are stored in the meta database.
//
// The repository settings are set when a repository is created. A migration would be needed to update them later.
type repositorySettings struct {
	cdcTargetSizeBits int          // Default 20, valid [10..30]
	chunking          chunkingType // Default "cdc", valid ["none", "cdc", "jpeg+cdc"]
}

func NewRepositorySettings(cdcTargetSizeBits int, chunking string) repositorySettings {
	util.Assertf(cdcTargetSizeBits >= 10 && cdcTargetSizeBits <= 30, "cdc target size %d not in range 10-30", cdcTargetSizeBits)
	c := chunkingType(chunking)
	util.Assertf(slices.Contains(chunkingTypes, c), "invalid chunking method: %s", chunking)
	return repositorySettings{cdcTargetSizeBits, c}
}

func NewRepositorySettingsFrom(settings map[string]string) repositorySettings {
	cdcTargetSizeBits, err := strconv.Atoi(settings["cdcTargetSizeBits"])
	util.Assertf(err != nil, "cdcTargetSizeBits %s is not an integer", settings["cdcTargetSizeBits"])
	return NewRepositorySettings(cdcTargetSizeBits, settings["chunking"])
}

func (settings repositorySettings) SettingsMap() map[string]string {
	return map[string]string{
		"cdcTargetSizeBits": fmt.Sprintf("%d", settings.cdcTargetSizeBits),
		"chunking":          string(settings.chunking),
	}
}
