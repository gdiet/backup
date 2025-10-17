//go:build prod

package aaa

func validateDataAreasInvariants(areas dataAreas) {
	// In prod builds, we skip the actual validation for performance reasons.
}
