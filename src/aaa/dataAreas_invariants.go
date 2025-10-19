//go:build !prod

package aaa

import "fmt"

// validateDataAreasInvariants checks Invariants: The areas are non-empty, sorted by position, non-overlapping.
func validateDataAreasInvariants(areas dataAreas) {
	for i := 0; i < len(areas); i++ {
		curr := areas[i]
		// Check non-empty
		if len(curr.data) == 0 {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d is empty", i))
		}
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
		// Check compactness
		if cap(curr.data) != len(curr.data) {
			panic(fmt.Sprintf("dataAreas invariant violated: area %d data not compact", i))
		}
	}
}
