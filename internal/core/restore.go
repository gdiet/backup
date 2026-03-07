package core

import "fmt"

func Restore(repo string, sources []string, target string) {
	fmt.Printf("[MOCK] Restoring sources %v to target '%s' in repo '%s'\n", sources, target, repo)
}
