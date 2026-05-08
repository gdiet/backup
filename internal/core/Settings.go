package core

type Settings struct {
	cdcTargetSizeBits uint   // Default 20, valid [10..30]
	chunking          string // Default "cdc", valid ["file", "cdc", "jpeg+cdc"]
}
