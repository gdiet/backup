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

// RepositorySettings define user defined per-repository constants. They are stored in the meta database.
//
// The repository settings are set when a repository is created. A migration would be needed to update them later.
type RepositorySettings struct {
	// cdcTargetSizeBits is limited to the 10..30 range. Values beyond that, while technically supported, are not sensible.
	cdcTargetSizeBits int          // Default 20, valid [10..30]
	chunking          chunkingType // Default "cdc", valid ["none", "cdc", "jpeg+cdc"]
}

func NewRepositorySettings(cdcTargetSizeBits int, chunking string) (RepositorySettings, error) {
	if cdcTargetSizeBits < 10 || cdcTargetSizeBits > 30 {
		return RepositorySettings{}, util.NewInvalidError("the message") // FIXME
	}
	c := chunkingType(chunking)
	util.Assertf(slices.Contains(chunkingTypes, c), "invalid chunking method: %s", chunking)
	return RepositorySettings{cdcTargetSizeBits, c}, nil
}

func NewRepositorySettings2(cdcTargetSizeBits int, chunking string) RepositorySettings {
	util.Assertf(cdcTargetSizeBits >= 10 && cdcTargetSizeBits <= 30, "cdc target size %d not in range 10-30", cdcTargetSizeBits)
	c := chunkingType(chunking)
	util.Assertf(slices.Contains(chunkingTypes, c), "invalid chunking method: %s", chunking)
	return RepositorySettings{cdcTargetSizeBits, c}
}

func NewRepositorySettingsFrom(settings map[string]string) RepositorySettings {
	cdcTargetSizeBits, err := strconv.Atoi(settings["cdcTargetSizeBits"])
	util.Assertf(err != nil, "cdcTargetSizeBits %s is not an integer", settings["cdcTargetSizeBits"])
	return NewRepositorySettings2(cdcTargetSizeBits, settings["chunking"])
}

func (settings RepositorySettings) SettingsMap() map[string]string {
	return map[string]string{
		"cdcTargetSizeBits": fmt.Sprintf("%d", settings.cdcTargetSizeBits),
		"chunking":          string(settings.chunking),
	}
}
