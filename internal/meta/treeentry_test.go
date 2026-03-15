package meta

import "fmt"

// String is a convenience converter for the IDEA debugger.
func (d *DirEntry) String() string {
	id := b64i(d.id)
	if id == 0 {
		return fmt.Sprintf("Dir: %d - <ROOT>", id)
	}
	return fmt.Sprintf("Dir: %d - %s", id, d.name)
}
