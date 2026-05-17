package core

import (
	"fmt"
	"slices"
	"strconv"
)

const (
	chunkingNone string = "none"
	chunkingCdc  string = "cdc"
	chunkingJpeg string = "jpeg+cdc"
)

var chunkingMethods = []string{chunkingNone, chunkingCdc, chunkingJpeg}

// RepositorySettings are user defined per-repository constants. They are stored in the meta database.
//
// The repository settings are set when a repository is created. A migration would be needed to update them later.
type RepositorySettings struct {
	// cdcTargetSizeBits is limited to the 10..30 range. Values beyond that, while technically supported, are not sensible.
	cdcTargetSizeBits int    // Default 20, valid [10..30]
	chunking          string // Default "cdc", valid ["none", "cdc", "jpeg+cdc"]
}

func NewRepositorySettings(cdcTargetSizeBits int, chunkingMethod string) (RepositorySettings, error) {
	if cdcTargetSizeBits < 10 || cdcTargetSizeBits > 30 {
		return RepositorySettings{}, fmt.Errorf("CDC target size (bits) '%d' is not in range 10-30", cdcTargetSizeBits)
	}
	if !slices.Contains(chunkingMethods, chunkingMethod) {
		return RepositorySettings{}, fmt.Errorf("invalid chunking method: %s", chunkingMethod)
	}
	return RepositorySettings{cdcTargetSizeBits, chunkingMethod}, nil
}

func NewRepositorySettingsFrom(settings map[string]string) (RepositorySettings, error) {
	cdcTargetSizeBits, err := strconv.Atoi(settings["cdcTargetSizeBits"])
	if err != nil {
		return RepositorySettings{}, fmt.Errorf("CDC target size (bits) '%s' is not an integer", settings["cdcTargetSizeBits"])
	}
	result, err := NewRepositorySettings(cdcTargetSizeBits, settings["chunking"])
	return result, err
}

func (settings RepositorySettings) SettingsMap() map[string]string {
	return map[string]string{
		"cdcTargetSizeBits": fmt.Sprintf("%d", settings.cdcTargetSizeBits),
		"chunking":          string(settings.chunking),
	}
}
