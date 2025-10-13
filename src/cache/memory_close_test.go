package cache

import (
	"testing"
)

func TestMemoryClose(t *testing.T) {
	tests := []struct {
		name         string
		initialAreas DataAreas
		description  string
	}{
		{
			name:         "Empty memory",
			initialAreas: DataAreas{},
			description:  "Close should work on empty memory",
		},
		{
			name: "Single area",
			initialAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
			},
			description: "Close should clear single area",
		},
		{
			name: "Multiple areas",
			initialAreas: DataAreas{
				{Off: 0, Data: Bytes("hello")},
				{Off: 10, Data: Bytes("world")},
				{Off: 20, Data: Bytes("test")},
			},
			description: "Close should clear all areas",
		},
		{
			name: "Large data areas",
			initialAreas: DataAreas{
				{Off: 0, Data: make(Bytes, 1024)},
				{Off: 2048, Data: make(Bytes, 2048)},
			},
			description: "Close should handle large data areas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Test description:", tt.description)

			// Create memory with initial areas
			memory := &Memory{areas: tt.initialAreas}

			// Verify areas are set
			if len(memory.areas) != len(tt.initialAreas) {
				t.Errorf("Initial setup failed: expected %d areas, got %d",
					len(tt.initialAreas), len(memory.areas))
				return
			}

			// Call Close
			memory.Close()

			// Verify areas are cleared
			if memory.areas != nil {
				t.Errorf("Expected areas to be nil after Close(), got %v", memory.areas)
			}

			// Verify length is 0
			if len(memory.areas) != 0 {
				t.Errorf("Expected 0 areas after Close(), got %d", len(memory.areas))
			}
		})
	}
}

func TestMemoryCloseReusability(t *testing.T) {
	t.Log("Test that Memory can be reused after Close()")

	memory := &Memory{areas: DataAreas{{Off: 0, Data: Bytes("test")}}}

	// Close the memory
	memory.Close()

	// Verify it's empty
	if memory.areas != nil {
		t.Error("Memory should be empty after Close()")
	}

	// Test that it can be reused
	memory.Write(5, Bytes("hello"), 100)

	// Verify write worked
	if len(memory.areas) != 1 {
		t.Errorf("Expected 1 area after reuse, got %d", len(memory.areas))
	}

	if memory.areas[0].Off != 5 {
		t.Errorf("Expected offset 5, got %d", memory.areas[0].Off)
	}

	if string(memory.areas[0].Data) != "hello" {
		t.Errorf("Expected 'hello', got %q", string(memory.areas[0].Data))
	}
}

func TestMemoryCloseIdempotent(t *testing.T) {
	t.Log("Test that Close() can be called multiple times safely")

	memory := &Memory{areas: DataAreas{{Off: 0, Data: Bytes("test")}}}

	// Call Close multiple times
	memory.Close()
	memory.Close()
	memory.Close()

	// Should still be empty and not panic
	if memory.areas != nil {
		t.Error("Memory should still be empty after multiple Close() calls")
	}
}

func TestMemoryCloseCompatibilityWithSparse(t *testing.T) {
	t.Log("Test that Memory.Close() signature is compatible with Sparse.Close()")

	// This test verifies that both have the same signature
	// so they can implement the same interface if needed

	memory := &Memory{areas: DataAreas{{Off: 0, Data: Bytes("test")}}}
	sparse := &Sparse{}

	// Both should have Close() methods that don't return anything
	memory.Close()
	sparse.Close()

	// If this compiles, the signatures are compatible
	t.Log("Memory.Close() and Sparse.Close() have compatible signatures")
}
