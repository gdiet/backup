package meta

import (
	"testing"
)

func TestRenameBasics(t *testing.T) {
	repo := testRepo(t)
	defer repo.Close()

	// Create directories to work with
	_, err := repo.Mkdir([]string{"directory"})
	if err != nil {
		t.Fatalf("Mkdir directory failed: %v", err)
	}

	_, err = repo.Mkdir([]string{"directory", "child"})
	if err != nil {
		t.Fatalf("Mkdir directory/child failed: %v", err)
	}

	// The following tests should not modify the repository state

	t.Run("Rename root to root should succeed", func(t *testing.T) {
		err = repo.Rename(nil, nil)
		if err != nil {
			t.Fatalf("Expected renaming root to root to succeed, but got: %v", err)
		}
	})

	t.Run("source = target rename should succeed", func(t *testing.T) {
		err = repo.Rename([]string{"directory"}, []string{"directory"})
		if err != nil {
			t.Fatalf("Expected renaming root to root to succeed, but got: %v", err)
		}
	})

	t.Run("Rename root to non-root should fail: ErrIsRoot", func(t *testing.T) {
		err = repo.Rename([]string{}, []string{"directory"})
		if err != ErrIsRoot {
			t.Fatalf("Expected ErrIsRoot when renaming root to non-root, but got: %v", err)
		}
	})

	t.Run("Fail if source does not exist: ErrNotFound", func(t *testing.T) {
		err = repo.Rename([]string{"nonexistent"}, []string{"new name 1"})
		if err != ErrNotFound {
			t.Fatalf("Expected ErrNotFound when renaming nonexistent source, but got: %v", err)
		}
	})

	t.Run("Fail if parent of source does not exist: ErrNotFound", func(t *testing.T) {
		err = repo.Rename([]string{"nonexistent", "child"}, []string{"new name 2"})
		if err != ErrNotFound {
			t.Fatalf("Expected ErrNotFound when renaming source with nonexistent parent, but got: %v", err)
		}
	})

	t.Run("Fail if parent of target does not exist: ErrNotFound", func(t *testing.T) {
		err = repo.Rename([]string{"directory", "child"}, []string{"new name 3", "subdir"})
		if err != ErrNotFound {
			t.Fatalf("Expected ErrNotFound when renaming nonexistent source, but got: %v", err)
		}
	})

	t.Run("Fail for loop renames: ErrInvalid", func(t *testing.T) {
		err = repo.Rename([]string{"directory"}, []string{"directory", "child"})
		if err != ErrInvalid {
			t.Fatalf("Expected ErrInvalid when renaming directory to its own subdirectory, but got: %v", err)
		}
	})

	// A single modifying test can be added here

	t.Run("a standard rename should succeed", func(t *testing.T) {
		err = repo.Rename([]string{"directory", "child"}, []string{"child"})
		if err != nil {
			t.Fatalf("Expected renaming root to root to succeed, but got: %v", err)
		}
	})

}

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
