package meta

import (
	"backup/src/fserr"
	"errors"
	"testing"
)

func TestLookupSuccess(t *testing.T) {
	repo := testRepo(t)
	defer repo.Close()

	// Root-Pfad (nil)
	id, entry, err := repo.Lookup(nil)
	if err != nil {
		t.Fatalf("Lookup(nil) failed: %v", err)
	}
	if id != 0 {
		t.Errorf("Expected root ID 0, got %d", id)
	}
	if entry.Name() != "" {
		t.Errorf("Expected root entry name '', got '%s'", entry.Name())
	}

	// Verzeichnis anlegen und Lookup
	dirID, err := repo.Mkdir([]string{"test"})
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	id, entry, err = repo.Lookup([]string{"test"})
	if err != nil {
		t.Fatalf("Lookup([test]) failed: %v", err)
	}
	if id != dirID {
		t.Errorf("Expected ID %d, got %d", dirID, id)
	}
	if entry.Name() != "test" {
		t.Errorf("Expected entry name 'test', got '%s'", entry.Name())
	}
}

func TestLookupNotFound(t *testing.T) {
	repo := testRepo(t)
	defer repo.Close()

	_, _, err := repo.Lookup([]string{"does not exist"})
	if !errors.Is(err, fserr.NotFound) {
		t.Error("Expected NotFound for non-existent path, got nil")
	}
}
