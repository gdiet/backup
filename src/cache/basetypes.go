package cache

type bytes []byte

// dataArea represents a contiguous area of bytes in a file.
type dataArea struct {
	position int
	data     bytes
}

// dataAreas represents a collection of data areas.
// Recommended general invariants (not enforced by the type system):
// - Sorted by Offset
// - Non-overlapping
// - Mostly merged (adjacent areas combined)
type dataAreas []dataArea
