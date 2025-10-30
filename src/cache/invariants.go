//go:build !prod

package cache

import "fmt"

// validateAreasInvariants checks invariants:
// The areas are non-empty, sorted by position, non-overlapping, and not adjacent to each other.
func validateAreasInvariants(areas areas) {
	for i := 0; i < len(areas); i++ {
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
		// Check sorted by position
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
// The areas are non-empty, sorted by position, non-overlapping.
func validateDataAreasInvariants(areas dataAreas) {
	for i := 0; i < len(areas); i++ {
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
		// Check sorted by position
		if prev.position > curr.position {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d not sorted by position", i))
		}
		// Check non-overlapping
		if prev.position+len(prev.data) > curr.position {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d overlaps with area %d", i-1, i))
		}
	}
}
