//go:build prod

package aaa

import "log"

func assert(condition bool, message string) {
	if !condition {
		log.Println("WARNING:", message)
	}
}
