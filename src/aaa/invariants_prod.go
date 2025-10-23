//go:build prod

package aaa

func validateAreasInvariants(areas areas) {
	// In prod builds, we skip the actual validation for performance reasons.
}

func validateDataAreasInvariants(areas dataAreas) {
	// In prod builds, we skip the actual validation for performance reasons.
}
