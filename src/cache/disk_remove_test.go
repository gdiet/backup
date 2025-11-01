package cache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskRemove(t *testing.T) {
	t.Run("remove from empty disk", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{filePath: filepath.Join(tempDir, "test.cache")}

		// Act
		d.remove(0, 10)

		// Assert - areas should remain empty
		if len(d.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(d.areas))
		}
	})

	t.Run("remove single area", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{
			filePath: filepath.Join(tempDir, "test.cache"),
			areas:    areas{{off: 5, len: 10}},
		}

		// Act
		d.remove(0, 20) // Remove entire area

		// Assert
		if len(d.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(d.areas))
		}
	})

	t.Run("remove within single area", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{
			filePath: filepath.Join(tempDir, "test.cache"),
			areas:    areas{{off: 0, len: 10}},
		}

		// Act
		d.remove(2, 5) // Remove part of the area

		// Assert - should split into two areas
		if len(d.areas) != 2 {
			t.Errorf("expected 2 areas, got %d", len(d.areas))
		}
		if d.areas[0] != (area{off: 0, len: 2}) {
			t.Errorf("expected first area to be {off: 0, len: 2}, got %+v", d.areas[0])
		}
		if d.areas[1] != (area{off: 7, len: 3}) {
			t.Errorf("expected second area to be {off: 7, len: 3}, got %+v", d.areas[1])
		}
	})

	t.Run("remove overlapping multiple areas", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{
			filePath: filepath.Join(tempDir, "test.cache"),
			areas: areas{
				{off: 0, len: 10},
				{off: 20, len: 10},
				{off: 40, len: 10},
			},
		}

		// Act
		d.remove(5, 30) // Remove from 5 to 35, overlaps first two areas

		// Assert - should keep part of first area and third area unchanged
		if len(d.areas) != 2 {
			t.Errorf("expected 2 areas, got %d", len(d.areas))
		}
		if d.areas[0] != (area{off: 0, len: 5}) {
			t.Errorf("expected first area to be {off: 0, len: 5}, got %+v", d.areas[0])
		}
		if d.areas[1] != (area{off: 40, len: 10}) {
			t.Errorf("expected second area to be {off: 40, len: 10}, got %+v", d.areas[1])
		}
	})

	t.Run("remove outside existing areas", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{
			filePath: filepath.Join(tempDir, "test.cache"),
			areas: areas{
				{off: 0, len: 10},
				{off: 20, len: 10},
			},
		}

		// Act
		d.remove(50, 10) // Remove outside existing areas

		// Assert - areas should remain unchanged
		if len(d.areas) != 2 {
			t.Errorf("expected 2 areas, got %d", len(d.areas))
		}
		if d.areas[0] != (area{off: 0, len: 10}) {
			t.Errorf("expected first area to be {off: 0, len: 10}, got %+v", d.areas[0])
		}
		if d.areas[1] != (area{off: 20, len: 10}) {
			t.Errorf("expected second area to be {off: 20, len: 10}, got %+v", d.areas[1])
		}
	})

	t.Run("remove with zero length does nothing", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{
			filePath: filepath.Join(tempDir, "test.cache"),
			areas:    areas{{off: 5, len: 10}},
		}

		// Act
		d.remove(5, 0)

		// Assert - areas should remain unchanged
		if len(d.areas) != 1 {
			t.Errorf("expected 1 area, got %d", len(d.areas))
		}
		if d.areas[0] != (area{off: 5, len: 10}) {
			t.Errorf("expected area to be {off: 5, len: 10}, got %+v", d.areas[0])
		}
	})

	t.Run("remove with negative length does nothing", func(t *testing.T) {
		tempDir := t.TempDir()
		d := &disk{
			filePath: filepath.Join(tempDir, "test.cache"),
			areas:    areas{{off: 5, len: 10}},
		}

		// Act
		d.remove(5, -10)

		// Assert - areas should remain unchanged
		if len(d.areas) != 1 {
			t.Errorf("expected 1 area, got %d", len(d.areas))
		}
		if d.areas[0] != (area{off: 5, len: 10}) {
			t.Errorf("expected area to be {off: 5, len: 10}, got %+v", d.areas[0])
		}
	})

	t.Run("remove with actual file operations", func(t *testing.T) {
		tempDir := t.TempDir()
		filePath := filepath.Join(tempDir, "test.cache")

		// Create a disk cache and write some data
		d := &disk{filePath: filePath}
		err := d.write(0, bytes{1, 2, 3, 4, 5})
		if err != nil {
			t.Fatalf("failed to write initial data: %v", err)
		}
		err = d.write(10, bytes{6, 7, 8})
		if err != nil {
			t.Fatalf("failed to write second data: %v", err)
		}

		// Verify initial state
		if len(d.areas) != 2 {
			t.Errorf("expected 2 initial areas, got %d", len(d.areas))
		}

		// Act - remove part of first area
		d.remove(2, 2)

		// Assert - should split first area
		if len(d.areas) != 3 {
			t.Errorf("expected 3 areas after remove, got %d", len(d.areas))
		}
		if d.areas[0] != (area{off: 0, len: 2}) {
			t.Errorf("expected first area to be {off: 0, len: 2}, got %+v", d.areas[0])
		}
		if d.areas[1] != (area{off: 4, len: 1}) {
			t.Errorf("expected second area to be {off: 4, len: 1}, got %+v", d.areas[1])
		}
		if d.areas[2] != (area{off: 10, len: 3}) {
			t.Errorf("expected third area to be {off: 10, len: 3}, got %+v", d.areas[2])
		}

		// Cleanup
		if d.file != nil {
			d.file.Close()
			os.Remove(filePath)
		}
	})
}
