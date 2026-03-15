package core

import (
	"fmt"
)

func Restore(repo string, sources []string, target string) error {
	fmt.Printf("[MOCK] Restoring sources %v to target '%s' in repo '%s'\n", sources, target, repo)
	return nil
}
