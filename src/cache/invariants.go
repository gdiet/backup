//go:build !prod

package cache

import "fmt"

// validateAreasInvariants checks invariants:
// The areas are non-empty, sorted by offset, non-overlapping, and not adjacent to each other.
func validateAreasInvariants(areas areas) {
	for i := range areas {
		curr := areas[i]
		// Check non-empty
		if curr.len == 0 {
			panic(fmt.Sprintf("areas invariant violated: area %d is empty", i))
		}
		// Check relations to previous area
		if i == 0 {
			continue
		}
		prev := areas[i-1]
		// Check sorted by offset
		if prev.off > curr.off {
			panic(fmt.Sprintf("areas invariant violated: area %d not sorted by offset", i))
		}
		// Check non-overlapping and not adjacent
		if prev.off+prev.len >= curr.off {
			panic(fmt.Sprintf("areas invariant violated: area %d overlaps or is adjacent to area %d", i-1, i))
		}
	}
}

// validateDataAreasInvariants checks Invariants:
// The areas are non-empty, sorted by offset, non-overlapping.
func validateDataAreasInvariants(areas dataAreas) {
	for i := range areas {
		curr := areas[i]
		// Check non-empty
		if len(curr.data) == 0 {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d is empty", i))
		}
		// Check compactness
		if cap(curr.data) != len(curr.data) {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d data not compact", i))
		}
		// Check relations to previous area
		if i == 0 {
			continue
		}
		prev := areas[i-1]
		// Check sorted by offset
		if prev.off > curr.off {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d not sorted by offset", i))
		}
		// Check non-overlapping
		if prev.off+int64(len(prev.data)) > curr.off {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d overlaps with area %d", i-1, i))
		}
	}
}
