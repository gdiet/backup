package cache

import (
	"testing"
)

func TestEmptyBaseFileRead(t *testing.T) {
	t.Run("read should panic with assertion", func(t *testing.T) {
		emptyBase := &EmptyBaseFile{}
		data := make([]byte, 10)

		// Dieser Test sollte eine Panic durch den assert auslösen
		// Wir fangen die Panic ab, um zu verifizieren, dass sie auftritt
		defer func() {
			if r := recover(); r != nil {
				// Erwarte eine Panic von assert
				t.Logf("Expected panic occurred: %v", r)
			} else {
				t.Fatal("Expected panic from assert, but none occurred")
			}
		}()

		// Dies sollte eine Panic auslösen wegen assert(false, ...)
		err := emptyBase.Read(0, data)

		// Diese Zeile sollte nie erreicht werden
		t.Fatalf("Expected panic, but got error: %v", err)
	})
}

func TestEmptyBaseFileLength(t *testing.T) {
	t.Run("length should return 0", func(t *testing.T) {
		emptyBase := &EmptyBaseFile{}
		length := emptyBase.Length()

		if length != 0 {
			t.Fatalf("expected length 0, got %d", length)
		}
	})
}
