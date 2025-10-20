//go:build !prod

package aaa

func assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}
