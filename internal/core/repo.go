package core

import (
	"fmt"
	"path/filepath"

	"github.com/gdiet/backup/internal/meta"
)

func NewMetadata(repo string) (*meta.Metadata, error) {
	metaRepo := filepath.Join(repo, "meta")
	m, err := meta.NewMetadata(metaRepo)
	if err != nil {
		return nil, fmt.Errorf("failed to open database from %s: %w", metaRepo, err)
	}
	return m, nil
}
