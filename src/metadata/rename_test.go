package metadata

import (
	"testing"
)

func TestRenameDirectory_SimpleRename(t *testing.T) {
	repo := testRepo(t)
	defer repo.Close()

	// Verzeichnis anlegen
	_, err := repo.Mkdir([]string{"old"})
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Umbenennen
	err = repo.Rename([]string{"old"}, []string{"new"})
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Prüfen, ob das neue Verzeichnis existiert
	_, entry, err := repo.Lookup([]string{"new"})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if entry.Name() != "new" {
		t.Errorf("Expected entry name 'new', got '%s'", entry.Name())
	}
}

func TestRenameDirectory_ReplaceEmptyTarget(t *testing.T) {
	repo := testRepo(t)
	defer repo.Close()

	// Zwei Verzeichnisse anlegen
	_, err := repo.Mkdir([]string{"src"})
	if err != nil {
		t.Fatalf("Mkdir src failed: %v", err)
	}
	_, err = repo.Mkdir([]string{"dst"})
	if err != nil {
		t.Fatalf("Mkdir dst failed: %v", err)
	}

	// src nach dst umbenennen
	err = repo.Rename([]string{"src"}, []string{"dst"})
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Prüfen, ob dst existiert und src nicht mehr
	_, _, err = repo.Lookup([]string{"src"})
	if err == nil {
		t.Error("Expected error for src after rename, got nil")
	}
	_, entry, err := repo.Lookup([]string{"dst"})
	if err != nil {
		t.Fatalf("Lookup dst failed: %v", err)
	}
	if entry.Name() != "dst" {
		t.Errorf("Expected entry name 'dst', got '%s'", entry.Name())
	}
}

func TestRenameDirectory_TargetNotEmpty(t *testing.T) {
	repo := testRepo(t)
	defer repo.Close()

	// Zielverzeichnis mit Inhalt anlegen
	_, err := repo.Mkdir([]string{"target"})
	if err != nil {
		t.Fatalf("Mkdir target failed: %v", err)
	}
	_, err = repo.Mkdir([]string{"target", "child"})
	if err != nil {
		t.Fatalf("Mkdir child failed: %v", err)
	}
	// Quellverzeichnis anlegen
	_, err = repo.Mkdir([]string{"source"})
	if err != nil {
		t.Fatalf("Mkdir source failed: %v", err)
	}

	// source nach target umbenennen (target ist nicht leer)
	err = repo.Rename([]string{"source"}, []string{"target"})
	if err == nil {
		t.Error("Expected error for not empty target, got nil")
	}
}
